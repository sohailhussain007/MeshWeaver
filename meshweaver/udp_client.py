
import asyncio

from meshweaver.protocol import create_message



class UDPClientProtocol(asyncio.DatagramProtocol):
    def __init__(self):
        self.reply_received = asyncio.Event()
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport

        message = create_message("PING", {"node_id": "node_1"})
        self.transport.sendto(message.encode("utf-8"))

        print("Sent message:", message)

    def datagram_received(self, data, addr):
        reply = data.decode("utf-8")
        print(f"Received reply from server: {reply}")

        self.reply_received.set()

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
        print("Server reply timed out")
    finally:
        transport.close()



async def protocol_received(protocol):
    while not protocol.transport.is_closing():
        await asyncio.sleep(0.1)


if __name__ == "__main__":
    asyncio.run(main())
