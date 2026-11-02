import asyncio

from meshweaver.protocol import create_message, parse_message
from meshweaver.serialization import serialize_task


def add(a, b):
    return a + b


class UDPClientProtocol(asyncio.DatagramProtocol):
    def __init__(self):
        self.reply_received = asyncio.Event()
        self.transport = None
        self.task_sent = False

    def connection_made(self, transport):
        self.transport = transport

        ping_message = create_message("PING", {"node_id": "node_1"})
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

            if message_type == "PONG":
                print("Health check passed!")

            elif message_type == "TASK_RESULT":
                print("Task result received:", message_data["result"])
                self.reply_received.set()

            elif message_type == "TASK_ERROR":
                print("Task failed:", message_data.get("error", "Unknown error"))
                self.reply_received.set()

            else:
                print("Unexpected response:", message)

        except (UnicodeDecodeError, ValueError, KeyError, TypeError) as exc:
            print(f"Invalid server response: {exc}")

    def error_received(self, exc):
        print(f"UDP error: {exc}")


async def main():
    loop = asyncio.get_running_loop()

    transport, protocol = await loop.create_datagram_endpoint(
        lambda: UDPClientProtocol(),
        remote_addr=("127.0.0.1", 9999),
    )

    try:
        await asyncio.wait_for(protocol.reply_received.wait(), timeout=5)
    except asyncio.TimeoutError:
        print("Server result timed out")
    finally:
        transport.close()


if __name__ == "__main__":
    asyncio.run(main())
