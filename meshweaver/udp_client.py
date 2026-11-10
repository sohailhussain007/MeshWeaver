import asyncio

from meshweaver import protocol
from meshweaver.protocol import create_message, parse_message
from meshweaver.peer_protocol import create_hello_message, encode_message
from meshweaver.serialization import serialize_task
from peer import Peer


def add(a, b):
    return a + b


class UDPClientProtocol(asyncio.DatagramProtocol):

    def __init__(self, heartbeat_interval=5):
        self.reply_received = asyncio.Event()
        self.heartbeat_ack_received = asyncio.Event()
        self.transport = None
        self.task_sent = False
        self.hello_received = False
        self.peer = None

        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_task = None

        self.ack_timeout = 2
        self.missed_heartbeats = 0
        self.max_missed_heartbeats = 3
        self.peer_unresponsive = False

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

    def send_heartbeat(self):
        heartbeat_message = create_message(
            "HEARTBEAT",
            {"peer_id": self.peer.peer_id},
        )

        self.transport.sendto(heartbeat_message.encode("utf-8"))
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

    def send_tasks(self):
        ping_message = create_message(
            "PING",
            {"node_id": self.peer.peer_id},
        )

        self.transport.sendto(ping_message.encode("utf-8"))
        print("Sent PING message")

        task_data = serialize_task(add, args=(10, 20))
        task_message = create_message("TASK", {"task": task_data})

        self.transport.sendto(task_message.encode("utf-8"))
        self.task_sent = True

        print("Sent serialized TASK message")

    def datagram_received(self, data, addr):
        try:
            message = parse_message(data.decode("utf-8"))

            message_type = message["type"]
            message_data = message["data"]

            if message_type == "HELLO_ACK":
                if message_data.get("peer_id") != self.peer.peer_id:
                    print("HELLO_ACK peer ID mismatch")
                    return

                if message_data.get("registered") is True:
                    self.hello_received = True
                    print("Peer registration successful!")
                    self.start_heartbeat()
                    self.send_tasks()
                else:
                    print("Peer registration failed")

            elif message_type == "HEARTBEAT_ACK":
                if message_data.get("peer_id") != self.peer.peer_id:
                    print("HEARTBEAT_ACK peer ID mismatch")
                    return

                print("Heartbeat acknowledged!")
                self.heartbeat_ack_received.set()

            elif message_type == "PONG":
                print("Health check passed!")

            elif message_type == "TASK_RESULT":
                print("Task result received:", message_data["result"])
                self.reply_received.set()

            elif message_type == "TASK_ERROR":
                print(
                    "Task failed:",
                    message_data.get("error", "Unknown error"),
                )
                self.reply_received.set()

            else:
                print("Unexpected response:", message)

        except (UnicodeDecodeError, ValueError, KeyError, TypeError) as exc:
            print(f"Invalid server response: {exc}")

    def connection_lost(self, exc):
        self.transport = None
        self.stop_heartbeat()

    def error_received(self, exc):
        print(f"UDP error: {exc}")


async def main(host="127.0.0.1", port=9999):
    loop = asyncio.get_running_loop()

    transport, protocol = await loop.create_datagram_endpoint(
        lambda: UDPClientProtocol(),
        remote_addr=(host, port),
    )

    try:
        await asyncio.wait_for(
            protocol.reply_received.wait(),
            timeout=5,
        )

    except asyncio.TimeoutError:
        print("Server result timed out")

    finally:
        if hasattr(protocol, "stop_heartbeat"):
           protocol.stop_heartbeat()

        transport.close()


if __name__ == "__main__":
    asyncio.run(main())
