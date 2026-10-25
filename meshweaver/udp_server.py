
import asyncio

from meshweaver.protocol import parse_message
from meshweaver.serialization import deserialize_task

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
        try:
            raw_message = data.decode("utf-8")
            message = parse_message(raw_message)

            message_type = message["type"]
            message_data = message["data"]

            print(f"Received {message_type} message from {addr}")

            if message_type == "PING":
                reply = "PONG"

            elif message_type == "TASK":
                encoded_task = message_data["task"]
                task = deserialize_task(encoded_task)

                function = task["function"]
                args = task["args"]
                kwargs = task["kwargs"]

                result = function(*args, **kwargs)
                reply = f"RESULT: {result}"

                print(f"Task executed successfully. Result: {result}")

            else:
                reply = f"ERROR: Unsupported message type: {message_type}"

            self.transport.sendto(reply.encode("utf-8"), addr)
            print(f"Reply sent to {addr}: {reply}")

        except (UnicodeDecodeError, ValueError) as error:
            print(f"Invalid message from {addr}: {error}")


if __name__ == "__main__":
    asyncio.run(start_server())

