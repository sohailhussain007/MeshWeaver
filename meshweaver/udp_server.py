import asyncio
import json

from meshweaver.protocol import create_message, parse_message
from meshweaver.peer_protocol import decode_message as decode_peer_message
from meshweaver.serialization import deserialize_task
from peer_registry import PeerRegistry


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

    def __init__(self, peer_registry=None):
        self.peer_registry = peer_registry or PeerRegistry()

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        try:
            incoming = json.loads(data.decode("utf-8"))

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

                if message_type == "PING":
                    response = create_message("PONG", {})

                elif message_type == "HEARTBEAT":
                    peer_id = message_data.get("peer_id")

                    if not isinstance(peer_id, str) or not peer_id.strip():
                        response = create_message(
                            "TASK_ERROR",
                            {"error": "Invalid heartbeat peer_id"},
                        )
                    else:
                        response = create_message(
                            "HEARTBEAT_ACK",
                            {"peer_id": peer_id},
                        )

                elif message_type == "TASK":
                    task = deserialize_task(message_data["task"])

                    try:
                        result = task["function"](
                            *task["args"],
                            **task["kwargs"]
                        )

                        response = create_message(
                            "TASK_RESULT",
                            {"result": result},
                        )

                    except Exception as exc:
                        response = create_message(
                            "TASK_ERROR",
                            {"error": str(exc)},
                        )

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


if __name__ == "__main__":
    asyncio.run(start_server())
