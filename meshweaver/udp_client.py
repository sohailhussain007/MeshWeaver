
import asyncio

from meshweaver.protocol import create_message


async def main():
    loop = asyncio.get_running_loop()

    transport, protocol = await loop.create_datagram_endpoint(
        asyncio.DatagramProtocol,
        remote_addr=("127.0.0.1", 9999),
    )

    message = create_message("PING", {"node_id": "node_1"})
    transport.sendto(message.encode("utf-8"))

    print("Sent message:", message)

    transport.close()


if __name__ == "__main__":
    asyncio.run(main())
