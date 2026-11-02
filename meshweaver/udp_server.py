import asyncio
import json
from meshweaver.protocol import (
    create_message as create_network_message,
    parse_message,
)
from meshweaver.serialization import deserialize_task
from peer_registry import PeerRegistry
from protocol import decode_message as decode_peer_message
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
    def __init__(self, peer_registry=None):
        self.peer_registry = (
            peer_registry if peer_registry is not None
            else PeerRegistry()
        )
    def connection_made(self, transport):
        self.transport = transport
    def datagram_received(self, data, addr):
        try:
            raw_message = data.decode("utf-8")
            message_data = json.loads(raw_message)
            # Handle HELLO messages from Peer Protocol
            if (
                isinstance(message_data, dict)
                and message_data.get("message_type") == "HELLO"
            ):
                hello_message = decode_peer_message(data)

                registered = self.peer_registry.register_hello_message(
                    hello_message
                )
                peer_id = hello_message["payload"]["peer_id"]
                response = create_network_message(
                    "HELLO_ACK",
                    {
                        "peer_id": peer_id,
                        "registered": registered,
                    },
                )
            else:
                # Handle existing Sohail network messages
                message = parse_message(raw_message)
                message_type = message["type"]
                message_data = message["data"]
                print(
                    f"Received {message_type} message from {addr}"
                )
                if message_type == "PING":
                    response = create_network_message("PONG", {})
                elif message_type == "TASK":
                    encoded_task = message_data["task"]
                    task = deserialize_task(encoded_task)
                    function = task["function"]
                    args = task["args"]
                    kwargs = task["kwargs"]
                    try:
                        result = function(*args, **kwargs)
                        response = create_network_message(
                            "TASK_RESULT",
                            {"result": result},
                        )
                        print(
                            f"Task executed successfully. Result: {result}"
                        )
                    except Exception as exc:
                        response = create_network_message(
                            "TASK_ERROR",
                            {"error": str(exc)},
                        )
                        print(f"Task execution failed: {exc}")
                else:
                    response = create_network_message(
                        "TASK_ERROR",
                        {
                            "error": (
                                f"Unsupported message type: {message_type}"
                            )
                        },
                    )
        except Exception as exc:
            print(
                f"Message processing failed from {addr}: {exc}"
            )
            response = create_network_message(
                "TASK_ERROR",
                {"error": str(exc)},
            )
        self.transport.sendto(
            response.encode("utf-8"),
            addr,
        )
        print(f"Reply sent to {addr}: {response}")
if __name__ == "__main__":
    asyncio.run(start_server())
