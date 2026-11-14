
import asyncio
import uuid

from meshweaver.protocol import create_message, parse_message
from meshweaver.peer_protocol import create_hello_message, encode_message
from meshweaver.serialization import serialize_task
from peer import Peer


def add(a, b):
    return a + b


def multiply(a, b):
    return a * b


def subtract(a, b):
    return a - b


class UDPClientProtocol(asyncio.DatagramProtocol):

    def __init__(self, heartbeat_interval=5, auto_submit_demo=False):
        self.reply_received = asyncio.Event()
        self.heartbeat_ack_received = asyncio.Event()

        self.transport = None
        self.task_sent = False
        self.hello_received = False
        self.peer = None

        # Demo configuration
        self.auto_submit_demo = auto_submit_demo

        # Heartbeat configuration
        self.heartbeat_interval = heartbeat_interval
        self.ack_timeout = 2
        self.heartbeat_timeout = self.ack_timeout
        self.max_missed_heartbeats = 3
        self.missed_heartbeats = 0
        self.peer_unresponsive = False
        self.heartbeat_task = None

        # Legacy task retry configuration
        self.task_timeout = 2
        self.max_task_retries = 3
        self.task_retry_count = 0
        self.task_id = None
        self.task_message = None
        self.task_retry_task = None

        # Multiple task configuration
        self.pending_tasks = {}
        self.task_results = {}
        self.task_errors = {}
        self.task_retry_tasks = {}

    # ---------------- CONNECTION ----------------

    def connection_made(self, transport):
        self.transport = transport

        local_addr = transport.get_extra_info("sockname")

        self.peer = Peer(
            host="127.0.0.1",
            port=local_addr[1],
        )

        hello_message = create_hello_message(self.peer)

        self.transport.sendto(encode_message(hello_message))

        print("HELLO sent:", self.peer)

    # ---------------- HEARTBEAT ----------------

    def send_heartbeat(self):
        if self.transport is None or self.peer is None:
            return

        heartbeat_message = create_message(
            "HEARTBEAT",
            {"peer_id": self.peer.peer_id},
        )

        self.transport.sendto(
            heartbeat_message.encode("utf-8")
        )

        print("Sent HEARTBEAT message")

    async def heartbeat_loop(self):
        try:
            while (
                self.transport is not None
                and not self.peer_unresponsive
            ):
                self.heartbeat_ack_received.clear()

                sent_at = asyncio.get_running_loop().time()

                self.send_heartbeat()

                try:
                    await asyncio.wait_for(
                        self.heartbeat_ack_received.wait(),
                        timeout=self.ack_timeout,
                    )

                    self.missed_heartbeats = 0
                    self.peer_unresponsive = False

                    print("Heartbeat ACK received successfully!")

                except asyncio.TimeoutError:
                    self.missed_heartbeats += 1

                    print(
                        f"Heartbeat ACK missed: "
                        f"{self.missed_heartbeats}/"
                        f"{self.max_missed_heartbeats}"
                    )

                    if (
                        self.missed_heartbeats
                        >= self.max_missed_heartbeats
                    ):
                        self.peer_unresponsive = True
                        print("Peer is unresponsive!")
                        break

                remaining = self.heartbeat_interval - (
                    asyncio.get_running_loop().time() - sent_at
                )

                if remaining > 0:
                    await asyncio.sleep(remaining)

        except asyncio.CancelledError:
            pass

    def start_heartbeat(self):
        if (
            self.heartbeat_task is None
            or self.heartbeat_task.done()
        ):
            self.heartbeat_task = asyncio.create_task(
                self.heartbeat_loop()
            )

    def stop_heartbeat(self):
        if self.heartbeat_task is not None:
            self.heartbeat_task.cancel()
            self.heartbeat_task = None

    # ---------------- LEGACY DEMO TASK ----------------

    def send_tasks(self):
        if self.transport is None:
            return

        ping_message = create_message(
            "PING",
            {"node_id": self.peer.peer_id},
        )

        self.transport.sendto(
            ping_message.encode("utf-8")
        )

        print("Sent PING message")

        self.task_id = str(uuid.uuid4())

        task_data = serialize_task(
            add,
            args=(10, 20),
        )

        self.task_message = create_message(
            "TASK",
            {
                "task_id": self.task_id,
                "task": task_data,
            },
        )

        self.task_retry_count = 0
        self.reply_received.clear()
        self.task_sent = True

        self.task_retry_task = asyncio.create_task(
            self.send_task_with_retry()
        )

    async def send_task_with_retry(self):
        try:
            total_attempts = self.max_task_retries + 1

            for attempt in range(total_attempts):

                if self.reply_received.is_set():
                    return

                if self.transport is None:
                    return

                self.transport.sendto(
                    self.task_message.encode("utf-8")
                )

                print(
                    f"Sent TASK "
                    f"(Attempt {attempt + 1}/{total_attempts})"
                )

                try:
                    await asyncio.wait_for(
                        self.reply_received.wait(),
                        timeout=self.task_timeout,
                    )

                    print("TASK response received successfully!")
                    return

                except asyncio.TimeoutError:

                    if attempt < self.max_task_retries:
                        self.task_retry_count += 1

                        print(
                            f"TASK response timeout. "
                            f"Retrying {self.task_retry_count}/"
                            f"{self.max_task_retries}..."
                        )

                    else:
                        print(
                            "TASK failed: Maximum retries exceeded."
                        )

        except asyncio.CancelledError:
            pass

    def stop_task_retry(self):
        if (
            self.task_retry_task is not None
            and not self.task_retry_task.done()
        ):
            self.task_retry_task.cancel()

        self.task_retry_task = None

    # ---------------- MULTIPLE TASK SUBMISSION ----------------

    def submit_task(self, function, args=(), kwargs=None):
        if self.transport is None:
            raise RuntimeError("Client is not connected.")

        kwargs = kwargs or {}
        task_id = str(uuid.uuid4())

        task_data = serialize_task(
            function,
            args=args,
            kwargs=kwargs,
        )

        task_message = create_message(
            "TASK",
            {
                "task_id": task_id,
                "task": task_data,
            },
        )

        self.pending_tasks[task_id] = {
            "message": task_message,
            "event": asyncio.Event(),
            "retry_count": 0,
        }

        retry_task = asyncio.create_task(
            self.send_individual_task_with_retry(task_id)
        )

        self.task_retry_tasks[task_id] = retry_task

        print(f"Task submitted: {task_id}")

        return task_id

    def submit_tasks(self, tasks):
        task_ids = []

        for task in tasks:
            task_id = self.submit_task(
                function=task["function"],
                args=task.get("args", ()),
                kwargs=task.get("kwargs", {}),
            )

            task_ids.append(task_id)

        return task_ids

    async def send_individual_task_with_retry(self, task_id):
        task_info = self.pending_tasks.get(task_id)

        if task_info is None:
            return

        try:
            total_attempts = self.max_task_retries + 1

            for attempt in range(total_attempts):

                if task_info["event"].is_set():
                    break

                if self.transport is None:
                    break

                task_info["retry_count"] = attempt

                print(
                    f"Task {task_id}: "
                    f"Attempt {attempt + 1}/{total_attempts}"
                )

                self.transport.sendto(
                    task_info["message"].encode("utf-8")
                )

                try:
                    await asyncio.wait_for(
                        task_info["event"].wait(),
                        timeout=self.task_timeout,
                    )
                    break

                except asyncio.TimeoutError:

                    if attempt == self.max_task_retries:
                        self.task_errors[task_id] = (
                            "Maximum task retries reached"
                        )

                        print(
                            f"Task {task_id}: "
                            "Maximum retries reached."
                        )

                    else:
                        print(
                            f"Task {task_id} timed out. Retrying..."
                        )

        except asyncio.CancelledError:
            pass

        finally:
            self.task_retry_tasks.pop(task_id, None)
            self.pending_tasks.pop(task_id, None)

    # ---------------- RECEIVE RESPONSES ----------------

    def datagram_received(self, data, addr):
        try:
            message = parse_message(
                data.decode("utf-8")
            )

            message_type = message["type"]
            message_data = message["data"]

            if message_type == "HELLO_ACK":

                if (
                    self.peer is None
                    or message_data.get("peer_id") != self.peer.peer_id
                ):
                    print("HELLO_ACK peer ID mismatch")
                    return

                if message_data.get("registered") is True:
                    self.hello_received = True

                    print("Peer registration successful!")

                    self.start_heartbeat()

                    if self.auto_submit_demo:
                        self.send_tasks()
                    else:
                        print("Client ready for task submission.")

                else:
                    print("Peer registration failed")

            elif message_type == "HEARTBEAT_ACK":

                if (
                    self.peer is None
                    or message_data.get("peer_id") != self.peer.peer_id
                ):
                    print("HEARTBEAT_ACK peer ID mismatch")
                    return

                print("Heartbeat acknowledged!")
                self.heartbeat_ack_received.set()

            elif message_type == "PONG":
                print("Health check passed!")

            elif message_type == "TASK_RESULT":

                task_id = message_data.get("task_id")
                result = message_data.get("result")

                if task_id in self.pending_tasks:
                    self.task_results[task_id] = result
                    self.pending_tasks[task_id]["event"].set()

                    print(
                        f"Task {task_id} result received: {result}"
                    )
                    return

                if task_id == self.task_id:
                    print("Task result received:", result)
                    self.reply_received.set()

            elif message_type == "TASK_ERROR":

                task_id = message_data.get("task_id")
                error = message_data.get(
                    "error",
                    "Unknown task error",
                )

                if task_id in self.pending_tasks:
                    self.task_errors[task_id] = error
                    self.pending_tasks[task_id]["event"].set()

                    print(
                        f"Task {task_id} failed: {error}"
                    )
                    return

                if task_id == self.task_id:
                    print("Task failed:", error)
                    self.reply_received.set()

            else:
                print("Unexpected response:", message)

        except (
            UnicodeDecodeError,
            ValueError,
            KeyError,
            TypeError,
        ) as exc:
            print(f"Invalid server response: {exc}")

    # ---------------- CONNECTION CLEANUP ----------------

    def connection_lost(self, exc):
        self.transport = None

        print("UDP connection closed.")

        self.stop_heartbeat()
        self.stop_task_retry()

        for retry_task in list(self.task_retry_tasks.values()):
            if not retry_task.done():
                retry_task.cancel()

        self.task_retry_tasks.clear()
        self.pending_tasks.clear()

    def error_received(self, exc):
        print(f"UDP error: {exc}")


# ---------------- CLIENT MAIN ----------------

async def main(
    host="127.0.0.1",
    port=9999,
    continuous=False,
    auto_submit_demo=False,
):

    loop = asyncio.get_running_loop()

    transport, protocol = await loop.create_datagram_endpoint(
        lambda: UDPClientProtocol(
            auto_submit_demo=auto_submit_demo
        ),
        remote_addr=(host, port),
    )

    try:
        if continuous:
            print("Client running in continuous mode.")
            await asyncio.Future()

        else:
            task_timeout = getattr(protocol, "task_timeout", 2)
            max_retries = getattr(protocol, "max_task_retries", 3)

            total_timeout = task_timeout * (max_retries + 1) + 1

            try:
                await asyncio.wait_for(
                    protocol.reply_received.wait(),
                    timeout=total_timeout,
                )

            except asyncio.TimeoutError:
                print("Server result timed out")

    finally:
        if hasattr(protocol, "stop_heartbeat"):
            protocol.stop_heartbeat()

        if hasattr(protocol, "stop_task_retry"):
            protocol.stop_task_retry()

        transport.close()


if __name__ == "__main__":
    try:
        asyncio.run(
            main(
                continuous=True,
                auto_submit_demo=False,
            )
        )

    except KeyboardInterrupt:
        print("\nClient stopped.")
