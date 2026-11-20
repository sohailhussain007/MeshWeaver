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
        self.peer_addresses = {}
        self.last_seen = {}
        self.peer_registry = (
            peer_registry if peer_registry is not None
            else PeerRegistry()
        )
        self.monitor_task = None
    def connection_made(self, transport):
        self.transport = transport
        self.monitor_task = asyncio.create_task(
            self.monitor_peers()
        )

    async def monitor_peers(self):
        while True:
            await asyncio.sleep(5)
            current_time = asyncio.get_running_loop().time()
            for peer_id, last_time in list(self.last_seen.items()):
                if current_time - last_time > 15:
                    print(f"Peer {peer_id} is offline")
                    offline_peer = self.peer_registry.get_peer(peer_id)
                    node_id = offline_peer.node_id if offline_peer else None
                    self.peer_registry.unregister_peer(peer_id)
                    self.last_seen.pop(peer_id, None)
                    self.peer_addresses.pop(peer_id, None)
                    notification = create_network_message(
                        "PEER_LEFT",
                        {"peer_id": peer_id,
                         "node_id": node_id,
                         },
                    )
                    for address in self.peer_addresses.values():
                        self.transport.sendto(
                            notification.encode("utf-8"),
                            address,
                        )
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
                # Save peers that were already registered
                existing_peers = self.peer_registry.get_all_peers()
                registered = self.peer_registry.register_hello_message(
                    hello_message
                )
                peer_id = hello_message["payload"]["peer_id"]
                if registered:
                    # Store the new peer's actual UDP address
                    self.peer_addresses[peer_id] = addr
                    self.last_seen[peer_id] = asyncio.get_running_loop().time()
                    known_peers = [
                        peer.to_dict()
                        for peer in self.peer_registry.get_all_peers()
                        if peer.peer_id != peer_id
                    ]
                    # Notify previously registered peers
                    new_peer = self.peer_registry.get_peer(peer_id)
                    notification = create_network_message(
                        "PEER_JOINED",
                        {"peer": new_peer.to_dict()},
                    )
                    for existing_peer in existing_peers:
                        existing_addr = self.peer_addresses.get(
                            existing_peer.peer_id
                        )
                        if existing_addr:
                            self.transport.sendto(
                                notification.encode("utf-8"),
                                existing_addr,
                            )
                            print(
                                f"PEER_JOINED notification sent to "
                                f"{existing_peer.peer_id}"
                            )
                else:
                    known_peers = []
                response = create_network_message(
                    "HELLO_ACK",
                    {
                        "peer_id": peer_id,
                        "registered": registered,
                        "peers": known_peers,
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
                    node_id = message_data.get("node_id")
                    for peer_id, peer_addr in self.peer_addresses.items():
                        if peer_addr == addr:
                            self.last_seen[peer_id] = (
                                asyncio.get_running_loop().time()
                            )
                            break
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
