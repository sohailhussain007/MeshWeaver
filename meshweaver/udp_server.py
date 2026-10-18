
import asyncio


async def start_server():
    loop = asyncio.get_running_loop()

    transport, protocol = await loop.create_datagram_endpoint(
        lambda: UDPServerProtocol(),
        local_addr=("127.0.0.1", 9999),
    )

    print("UDP server started on 127.0.0.1:9999")

    try:
        await asyncio.Future()
    finally:
        transport.close()



class UDPServerProtocol(asyncio.DatagramProtocol):
    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        message = data.decode("utf-8")
        print(f"Received from {addr}: {message}")

        reply = "ACK: Message received"
        self.transport.sendto(reply.encode("utf-8"), addr)

        print(f"Reply sent to {addr}: {reply}")



if __name__ == "__main__":
    asyncio.run(start_server())
