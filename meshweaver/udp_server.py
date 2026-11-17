import asyncio
import json
import time

from meshweaver.protocol import create_message, parse_message
from meshweaver.peer_protocol import decode_message as decode_peer_message
from meshweaver.serialization import deserialize_task
from peer_registry import PeerRegistry


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
    ):
        if task_cache_ttl <= 0:
            raise ValueError("task_cache_ttl must be greater than 0")

        if max_completed_tasks <= 0:
            raise ValueError("max_completed_tasks must be greater than 0")

        self.peer_registry = peer_registry or PeerRegistry()

        self.task_cache_ttl = task_cache_ttl
        self.max_completed_tasks = max_completed_tasks

        self.completed_tasks = {}
        self.task_cache_timestamps = {}

        # Track tasks currently executing
        self.in_flight_tasks = {}

    def cleanup_task_cache(self):
        """Remove expired cache entries."""
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

    def _send_response(self, response, addr):
        if self.transport is not None:
            self.transport.sendto(
                response.encode("utf-8"),
                addr,
            )

    async def _execute_task(self, message_data, addr, cache_key):
        """Execute a task independently without blocking the event loop."""
        task_id = message_data.get("task_id")

        try:
            result = await asyncio.to_thread(
                execute_task_sync,
                message_data["task"],
            )

            response = create_message(
                "TASK_RESULT",
                {
                    "task_id": task_id,
                    "result": result,
                },
            )

        except Exception as exc:
            response = create_message(
                "TASK_ERROR",
                {
                    "task_id": task_id,
                    "error": str(exc),
                },
            )

        if cache_key is not None:
            self.cache_task_response(cache_key, response)

        self._send_response(response, addr)

        if cache_key is not None:
            self.in_flight_tasks.pop(cache_key, None)

        print(f"Task completed: {task_id}")

    def datagram_received(self, data, addr):
        try:
            incoming = json.loads(data.decode("utf-8"))

            # ---------------- HELLO ----------------

            if (
                isinstance(incoming, dict)
                and incoming.get("message_type") == "HELLO"
            ):
                hello_message = decode_peer_message(data)

                registered = self.peer_registry.register_hello_message(
                    hello_message
                )

                response = create_message(
                    "HELLO_ACK",
                    {
                        "peer_id": hello_message["payload"]["peer_id"],
                        "registered": registered,
                    },
                )

            else:
                message = parse_message(data.decode("utf-8"))

                message_type = message["type"]
                message_data = message["data"]

                # ---------------- PING ----------------

                if message_type == "PING":
                    response = create_message("PONG", {})

                # ---------------- HEARTBEAT ----------------

                elif message_type == "HEARTBEAT":
                    peer_id = message_data.get("peer_id")

                    if (
                        not isinstance(peer_id, str)
                        or not peer_id.strip()
                    ):
                        response = create_message(
                            "TASK_ERROR",
                            {"error": "Invalid heartbeat peer_id"},
                        )
                    else:
                        response = create_message(
                            "HEARTBEAT_ACK",
                            {"peer_id": peer_id},
                        )

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

                        else:
                            # Schedule execution and immediately return
                            # control to the event loop.
                            task = asyncio.get_running_loop().create_task(
                                self._execute_task(
                                    message_data,
                                    addr,
                                    cache_key,
                                )
                            )

                            if cache_key is not None:
                                self.in_flight_tasks[cache_key] = task

                            print(f"Task scheduled: {task_id}")

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

        for task in list(self.in_flight_tasks.values()):
            if not task.done():
                task.cancel()

        self.in_flight_tasks.clear()


if __name__ == "__main__":
    try:
        asyncio.run(start_server())
    except KeyboardInterrupt:
        print("\nUDP server stopped.")
