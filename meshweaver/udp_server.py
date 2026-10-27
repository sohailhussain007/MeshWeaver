import asyncio

from meshweaver.protocol import create_message, parse_message
from meshweaver.serialization import deserialize_task


async def start_server():
    loop = asyncio.get_running_loop()

    transport, _ = await loop.create_datagram_endpoint(
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
            message = parse_message(data.decode("utf-8"))
            message_type = message["type"]
            message_data = message["data"]

            print(f"Received {message_type} message from {addr}")

            if message_type == "PING":
                response = create_message("PONG", {})

            elif message_type == "TASK":
                encoded_task = message_data["task"]
                task = deserialize_task(encoded_task)

                function = task["function"]
                args = task["args"]
                kwargs = task["kwargs"]

                try:
                    result = function(*args, **kwargs)
                    response = create_message(
                        "TASK_RESULT",
                        {"result": result},
                    )
                    print(f"Task executed successfully. Result: {result}")

                except Exception as exc:
                    response = create_message(
                        "TASK_ERROR",
                        {"error": str(exc)},
                    )
                    print(f"Task execution failed: {exc}")

            else:
                response = create_message(
                    "TASK_ERROR",
                    {"error": f"Unsupported message type: {message_type}"},
                )

        except Exception as exc:
            print(f"Message processing failed from {addr}: {exc}")
            response = create_message(
                "TASK_ERROR",
                {"error": str(exc)},
            )

        self.transport.sendto(response.encode("utf-8"), addr)
        print(f"Reply sent to {addr}: {response}")


if __name__ == "__main__":
    asyncio.run(start_server())
