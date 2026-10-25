
import asyncio

from meshweaver.protocol import create_message
from meshweaver.serialization import serialize_task


def add(a, b):
    return a + b


class UDPClientProtocol(asyncio.DatagramProtocol):
    def __init__(self):
        self.reply_received = asyncio.Event()
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport

        # Send health-check message
        ping_message = create_message(
            "PING",
            {"node_id": "node_1"},
        )
        self.transport.sendto(ping_message.encode("utf-8"))
        print("Sent PING message")

        # Serialize a Python task
        task_data = serialize_task(add, args=(10, 20))

        # Create and send the TASK message
        task_message = create_message(
            "TASK",
            {"task": task_data},
        )
        self.transport.sendto(task_message.encode("utf-8"))
        print("Sent serialized TASK message")

    def datagram_received(self, data, addr):
        reply = data.decode("utf-8")
        print(f"Received reply from server: {reply}")

        if reply == "PONG":
            print("Health check passed!")

        elif reply.startswith("RESULT:"):
            print("Task result received:", reply)
            self.reply_received.set()

        else:
            print("Server response:", reply)

    def error_received(self, exc):
        print(f"UDP error: {exc}")


async def main():
    loop = asyncio.get_running_loop()

    transport, protocol = await loop.create_datagram_endpoint(
        lambda: UDPClientProtocol(),
        remote_addr=("127.0.0.1", 9999),
    )

    try:
        await asyncio.wait_for(
            protocol.reply_received.wait(),
            timeout=5,
        )
    except asyncio.TimeoutError:
        print("Server result timed out")
    finally:
        transport.close()


if __name__ == "__main__":
    asyncio.run(main())
