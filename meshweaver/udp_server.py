

import asyncio
import json
import time
import uuid

from meshweaver.protocol import create_message, parse_message
from meshweaver.peer_protocol import decode_message as decode_peer_message
from meshweaver.serialization import deserialize_task
from peer_registry import PeerRegistry
from peer import Peer
from kademlia import RoutingTable


def execute_task_sync(task_data):
    """Deserialize and execute a synchronous task."""
    task = deserialize_task(task_data)

    return task["function"](
        *task["args"],
        **task["kwargs"]
    )


async def start_server(host="127.0.0.1", port=9999):
    loop = asyncio.get_running_loop()

    transport, _ = await loop.create_datagram_endpoint(
        lambda: UDPServerProtocol(),
        local_addr=(host, port),
    )

    print(f"UDP server started on {host}:{port}")

    try:
        await asyncio.Future()
    finally:
        transport.close()


class UDPServerProtocol(asyncio.DatagramProtocol):

    def __init__(
        self,
        peer_registry=None,
        task_cache_ttl=300,
        max_completed_tasks=1000,
        max_concurrent_tasks=10,
        task_timeout=30,
        peer_timeout=45,
    ):
        if task_cache_ttl <= 0:
            raise ValueError("task_cache_ttl must be greater than 0")

        if max_completed_tasks <= 0:
            raise ValueError("max_completed_tasks must be greater than 0")

        if (
            not isinstance(max_concurrent_tasks, int)
            or isinstance(max_concurrent_tasks, bool)
            or max_concurrent_tasks <= 0
        ):
            raise ValueError(
                "max_concurrent_tasks must be a positive integer"
            )

        if (
            not isinstance(task_timeout, (int, float))
            or isinstance(task_timeout, bool)
            or task_timeout <= 0
        ):
            raise ValueError("task_timeout must be a positive number")

        self.task_timeout = task_timeout

        if (
            not isinstance(peer_timeout, (int, float))
            or isinstance(peer_timeout, bool)
            or peer_timeout <= 0
        ):
            raise ValueError("peer_timeout must be a positive number")

        self.peer_timeout = peer_timeout
        self._peer_cleanup_task = None

        self.transport = None
        self.peer_registry = peer_registry or PeerRegistry()
        self.peer_addresses = {}
        self.distributed_tasks = {}
        self.local_node_id = None
        self.routing_table = None

        self.task_cache_ttl = task_cache_ttl
        self.max_completed_tasks = max_completed_tasks
        self.max_concurrent_tasks = max_concurrent_tasks

        # Completed task responses and timestamps
        self.completed_tasks = {}
        self.task_cache_timestamps = {}

        # Track tasks currently executing by Task ID
        self.in_flight_tasks = {}

        # Track all active tasks
        self.active_tasks = set()

        # Track task lifecycle and execution history
        self.task_history = {}

    def _set_task_status(self, task_id, status, **details):
        """Create or update a task lifecycle record."""
        if task_id is None:
            return

        if task_id not in self.task_history:
            self.task_history[task_id] = {
                "task_id": task_id,
                "status": "PENDING",
                "created_at": time.time(),
                "started_at": None,
                "completed_at": None,
                "result": None,
                "error": None,
            }

        record = self.task_history[task_id]
        record["status"] = status

        if status == "RUNNING":
            record["started_at"] = time.time()

        if status in ("COMPLETED", "FAILED", "CANCELLED", "TIMED_OUT"):
            record["completed_at"] = time.time()

        record.update(details)

    def cleanup_task_cache(self):
        """Remove expired cache entries and return the number removed."""
        now = time.monotonic()

        expired_keys = [
            key
            for key, timestamp in self.task_cache_timestamps.items()
            if now - timestamp >= self.task_cache_ttl
        ]

        for key in expired_keys:
            self.completed_tasks.pop(key, None)
            self.task_cache_timestamps.pop(key, None)

        return len(expired_keys)

    def cache_task_response(self, cache_key, response):
        """Store a response while respecting the maximum cache size."""
        self.cleanup_task_cache()

        while len(self.completed_tasks) >= self.max_completed_tasks:
            oldest_key = min(
                self.task_cache_timestamps,
                key=self.task_cache_timestamps.get,
            )

            self.completed_tasks.pop(oldest_key, None)
            self.task_cache_timestamps.pop(oldest_key, None)

        self.completed_tasks[cache_key] = response
        self.task_cache_timestamps[cache_key] = time.monotonic()

    def connection_made(self, transport):
        self.transport = transport

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop is not None and (
            self._peer_cleanup_task is None
            or self._peer_cleanup_task.done()
        ):
            self._peer_cleanup_task = loop.create_task(
                self._peer_cleanup_loop()
            )

        get_extra_info = getattr(transport, "get_extra_info", None)
        local_address = (
            get_extra_info("sockname")
            if callable(get_extra_info)
            else None
        )

        if local_address:
            host, port = local_address[:2]
            self.local_peer = Peer(host=host, port=port)
            self.local_node_id = self.local_peer.node_id
            self.routing_table = RoutingTable(self.local_node_id)

    def _send_response(self, response, addr):
        """Send a response if the UDP transport is available."""
        if self.transport is not None:
            self.transport.sendto(
                response.encode("utf-8"),
                addr,
            )

    def cancel_task(self, task_id):
        """Cancel a running task by its Task ID."""
        if not isinstance(task_id, str) or not task_id.strip():
            return False

        for cache_key, task in list(self.in_flight_tasks.items()):
            if cache_key[1] != task_id:
                continue

            if task.done():
                return False

            self._set_task_status(
                task_id,
                "CANCELLED",
                error="Task cancelled by request",
            )

            task.cancel()
            print(f"Task cancellation requested: {task_id}")
            return True

        return False

    async def _execute_task(self, message_data, addr, cache_key):
        """Execute locally or forward to the lowest-CPU available peer."""
        task_id = message_data.get("task_id")
        dispatch_id = None

        self._set_task_status(task_id, "RUNNING")

        try:
            try:
                requester_peer_id = next(
                    (
                        peer_id
                        for peer_id, peer_addr in self.peer_addresses.items()
                        if peer_addr == addr
                    ),
                    None,
                )

                candidates = [
                    peer_id
                    for peer_id in self.peer_registry.cpu_loads
                    if (
                        peer_id in self.peer_registry.peers
                        and peer_id in self.peer_addresses
                        and peer_id != requester_peer_id
                    )
                ]

                if candidates:
                    selected_peer_id = min(
                        candidates,
                        key=lambda peer_id:
                            self.peer_registry.cpu_loads[peer_id],
                    )
                    worker_addr = self.peer_addresses[selected_peer_id]
                    dispatch_id = str(uuid.uuid4())
                    future = asyncio.get_running_loop().create_future()

                    self.distributed_tasks[dispatch_id] = {
                        "task_id": task_id,
                        "worker_id": selected_peer_id,
                        "future": future,
                    }

                    forward_message = create_message(
                        "TASK_FORWARD",
                        {
                            "dispatch_id": dispatch_id,
                            "task_id": task_id,
                            "task": message_data["task"],
                        },
                    )

                    self._send_response(forward_message, worker_addr)

                    print(
                        f"Task {task_id} forwarded to "
                        f"{selected_peer_id} "
                        f"(CPU: "
                        f"{self.peer_registry.cpu_loads[selected_peer_id]}%)"
                    )

                    success, worker_result = await asyncio.wait_for(
                        future,
                        timeout=self.task_timeout,
                    )

                    if not success:
                        raise RuntimeError(worker_result)

                    result = worker_result

                else:
                    print(
                        f"No eligible worker for task {task_id}; "
                        "executing locally."
                    )

                    result = await asyncio.wait_for(
                        asyncio.to_thread(
                            execute_task_sync,
                            message_data["task"],
                        ),
                        timeout=self.task_timeout,
                    )

                self._set_task_status(
                    task_id,
                    "COMPLETED",
                    result=result,
                )

                response = create_message(
                    "TASK_RESULT",
                    {
                        "task_id": task_id,
                        "result": result,
                    },
                )

            except asyncio.TimeoutError:
                error = (
                    f"Task execution timed out after "
                    f"{self.task_timeout} seconds"
                )
                self._set_task_status(
                    task_id,
                    "TIMED_OUT",
                    error=error,
                )
                response = create_message(
                    "TASK_ERROR",
                    {"task_id": task_id, "error": error},
                )

            except Exception as exc:
                self._set_task_status(
                    task_id,
                    "FAILED",
                    error=str(exc),
                )
                response = create_message(
                    "TASK_ERROR",
                    {"task_id": task_id, "error": str(exc)},
                )

            if cache_key is not None:
                self.cache_task_response(cache_key, response)

            self._send_response(response, addr)
            print(f"Task completed: {task_id}")

        finally:
            if dispatch_id is not None:
                self.distributed_tasks.pop(dispatch_id, None)

            current_task = asyncio.current_task()
            if current_task is not None:
                self.active_tasks.discard(current_task)

            if cache_key is not None:
                self.in_flight_tasks.pop(cache_key, None)

    def _cleanup_stale_peers(self):
        """Remove stale peers and notify the remaining network."""
        known_peers = {
            peer.peer_id: peer
            for peer in self.peer_registry.get_all_peers()
        }

        stale_peer_ids = self.peer_registry.remove_stale_peers(
            self.peer_timeout
        )

        for peer_id in stale_peer_ids:
            stale_peer = known_peers.get(peer_id)
            self.peer_addresses.pop(peer_id, None)

            if stale_peer is not None and self.routing_table is not None:
                self.routing_table.remove_node(stale_peer.node_id)

            notification = create_message(
                "PEER_LEFT",
                {
                    "peer_id": peer_id,
                    "node_id": (
                        stale_peer.node_id if stale_peer is not None else None
                    ),
                },
            )

            for address in list(self.peer_addresses.values()):
                self._send_response(notification, address)

            print(f"Removed stale peer: {peer_id}")

        return stale_peer_ids

    async def _peer_cleanup_loop(self):
        """Periodically remove peers that have stopped sending heartbeats."""
        interval = min(5, self.peer_timeout / 2)

        try:
            while True:
                await asyncio.sleep(interval)
                self._cleanup_stale_peers()
        except asyncio.CancelledError:
            raise


    def datagram_received(self, data, addr):
        try:
            incoming = json.loads(data.decode("utf-8"))

            # ---------------- HELLO ----------------

            if (
                isinstance(incoming, dict)
                and incoming.get("message_type") == "HELLO"
            ):
                hello_message = decode_peer_message(data)

                existing_peers = self.peer_registry.get_all_peers()

                registered = self.peer_registry.register_hello_message(
                    hello_message
                )

                peer_id = hello_message["payload"]["peer_id"]
                self.peer_addresses[peer_id] = addr
                registered_peer = self.peer_registry.get_peer(peer_id)

                if (
                    registered_peer is not None
                    and self.routing_table is not None
                ):
                    self.routing_table.add_node(registered_peer.node_id)

                known_peers = [
                    peer.to_dict()
                    for peer in existing_peers
                    if peer.peer_id != peer_id
                ]

                response = create_message(
                    "HELLO_ACK",
                    {
                        "peer_id": peer_id,
                        "registered": registered,
                        "peers": known_peers,
                    },
                )

                if registered and registered_peer is not None:
                    notification = create_message(
                        "PEER_JOINED",
                        {"peer": registered_peer.to_dict()},
                    )

                    for existing_peer in existing_peers:
                        existing_address = self.peer_addresses.get(
                            existing_peer.peer_id
                        )
                        if existing_address is not None:
                            self._send_response(
                                notification,
                                existing_address,
                            )

            else:
                message = parse_message(data.decode("utf-8"))

                message_type = message["type"]
                message_data = message["data"]

                # ---------------- PING ----------------

                if message_type == "PING":
                    response = create_message(
                        "PONG",
                        {},
                    )

                # ---------------- HEARTBEAT ----------------

                elif message_type == "HEARTBEAT":
                    peer_id = message_data.get("peer_id")
                    cpu_load = message_data.get("cpu_load")

                    # Validate peer identity
                    if not isinstance(peer_id, str) or not peer_id.strip():
                        response = create_message(
                            "TASK_ERROR",
                            {"error": "Invalid heartbeat peer_id"},
                        )

                    # Verify sender address belongs to this peer
                    elif self.peer_addresses.get(peer_id) != addr:
                        print(f"Rejected HEARTBEAT from unverified peer: {peer_id}")
                        return

                    # Validate CPU load
                    elif (
                        not isinstance(cpu_load, (int, float))
                        or isinstance(cpu_load, bool)
                        or not 0 <= cpu_load <= 100
                    ):
                        print(f"Rejected invalid CPU load from {peer_id}")
                        return

                    else:
                        # Refresh this peer before checking other stale peers.
                        self.peer_registry.update_last_seen(peer_id)

                        # Remove peers whose heartbeat has expired.
                        self._cleanup_stale_peers()

                        updated = self.peer_registry.update_cpu_load(
                            peer_id,
                            cpu_load,
                        )

                        if updated:
                            selected_peer = (
                                self.peer_registry.select_lowest_cpu_peer()
                            )

                            print(
                                f"CPU load updated: {peer_id} = {cpu_load}%"
                            )

                            if selected_peer is not None:
                                selected_load = self.peer_registry.cpu_loads[
                                    selected_peer.peer_id
                                ]
                                print(
                                    f"Lowest CPU peer: {selected_peer.peer_id} "
                                    f"({selected_load}%)"
                                )

                        response = create_message(
                            "HEARTBEAT_ACK",
                            {"peer_id": peer_id},
                        )

                # -------- FORWARDED WORKER RESULT --------

                elif message_type in (
                    "TASK_FORWARD_RESULT",
                    "TASK_FORWARD_ERROR",
                ):
                    dispatch_id = message_data.get("dispatch_id")
                    worker_id = message_data.get("worker_id")
                    record = self.distributed_tasks.get(dispatch_id)

                    if (
                        record is None
                        or record["worker_id"] != worker_id
                        or self.peer_addresses.get(worker_id) != addr
                    ):
                        print("Rejected unknown or unauthorised worker result")
                        return

                    future = record["future"]

                    if not future.done():
                        if message_type == "TASK_FORWARD_RESULT":
                            future.set_result(
                                (True, message_data.get("result"))
                            )
                        else:
                            future.set_result(
                                (
                                    False,
                                    message_data.get(
                                        "error",
                                        "Worker execution failed",
                                    ),
                                )
                            )
                    return

                # ---------------- TASK ----------------

                elif message_type == "TASK":
                    task_id = message_data.get("task_id")

                    if task_id is not None and (
                        not isinstance(task_id, str)
                        or not task_id.strip()
                    ):
                        response = create_message(
                            "TASK_ERROR",
                            {"error": "Invalid task_id"},
                        )

                    else:
                        cache_key = (
                            (addr, task_id)
                            if task_id is not None
                            else None
                        )

                        self.cleanup_task_cache()

                        # Check completed task cache
                        if (
                            cache_key is not None
                            and cache_key in self.completed_tasks
                        ):
                            print(f"Cached task response: {task_id}")

                            response = self.completed_tasks[cache_key]

                        # Check whether the task is already running
                        elif (
                            cache_key is not None
                            and cache_key in self.in_flight_tasks
                        ):
                            print(f"Task already running: {task_id}")
                            return

                        # Enforce maximum concurrent task limit
                        elif (
                            len(self.active_tasks)
                            >= self.max_concurrent_tasks
                        ):
                            print(
                                "Task rejected: maximum concurrent "
                                "task limit reached."
                            )

                            response = create_message(
                                "TASK_ERROR",
                                {
                                    "task_id": task_id,
                                    "error": (
                                        "Server is busy: maximum "
                                        "concurrent task limit reached "
                                        f"({self.max_concurrent_tasks})."
                                    ),
                                },
                            )

                        else:
                            # Record the task before execution starts
                            self._set_task_status(task_id, "PENDING")

                            # Schedule task execution
                            task = asyncio.get_running_loop().create_task(
                                self._execute_task(
                                    message_data,
                                    addr,
                                    cache_key,
                                )
                            )

                            # Track every active task
                            self.active_tasks.add(task)

                            # Track Task ID for duplicate prevention
                            if cache_key is not None:
                                self.in_flight_tasks[cache_key] = task

                            print(
                                f"Task scheduled: {task_id} "
                                f"({len(self.active_tasks)}/"
                                f"{self.max_concurrent_tasks} active)"
                            )

                            return

                # ---------------- UNKNOWN MESSAGE ----------------

                else:
                    response = create_message(
                        "TASK_ERROR",
                        {
                            "error": (
                                f"Unsupported message type: "
                                f"{message_type}"
                            ),
                        },
                    )

        except Exception as exc:
            print(
                f"Message processing failed from {addr}: {exc}"
            )

            response = create_message(
                "TASK_ERROR",
                {"error": str(exc)},
            )

        self._send_response(response, addr)

    def connection_lost(self, exc):
        """Cancel unfinished task coroutines when transport closes."""
        self.transport = None

        if (
            self._peer_cleanup_task is not None
            and not self._peer_cleanup_task.done()
        ):
            self._peer_cleanup_task.cancel()

        self._peer_cleanup_task = None

        # Mark in-flight tasks as cancelled before cancelling them.
        for cache_key, task in list(self.in_flight_tasks.items()):
            if not task.done():
                task_id = cache_key[1]
                self._set_task_status(
                    task_id,
                    "CANCELLED",
                    error="Server shutting down"
                )

        # Cancel all active task coroutines.
        for task in list(self.active_tasks):
            if not task.done():
                task.cancel()

        self.active_tasks.clear()
        self.in_flight_tasks.clear()

        # Cancel pending distributed task futures.
        for record in self.distributed_tasks.values():
            future = record["future"]
            if not future.done():
                future.cancel()

        self.distributed_tasks.clear()
        self.peer_addresses.clear()


if __name__ == "__main__":
    try:
        asyncio.run(start_server())
    except KeyboardInterrupt:
        print("\nUDP server stopped.")
