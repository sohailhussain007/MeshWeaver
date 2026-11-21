import asyncio
import psutil
from meshweaver.protocol import create_message, parse_message
from meshweaver.serialization import serialize_task
from peer import Peer
from kademlia import RoutingTable
from protocol import create_hello_message, encode_message

def add(a, b):
    return a + b

class UDPClientProtocol(asyncio.DatagramProtocol):
    def __init__(self):
        self.reply_received = asyncio.Event()
        self.transport = None
        self.hello_received = False
    def connection_made(self, transport):
        self.transport = transport
        self.heartbeat_task = None
        # Get the actual local UDP port
        local_addr = transport.get_extra_info("sockname")
        # Create this client's peer identity
        self.peer = Peer(
            host="127.0.0.1",
            port=local_addr[1],
        )
        self.routing_table = RoutingTable(self.peer.node_id)
        # Send HELLO first
        hello_message = create_hello_message(self.peer)
        self.transport.sendto(encode_message(hello_message))
        print("HELLO sent:", self.peer)
    def send_heartbeat(self):
        cpu_load = psutil.cpu_percent(interval=None)

        ping_message = create_message(
            "PING",
            {"node_id": self.peer.node_id,
             "cpu_load": cpu_load,
             },
        )
        self.transport.sendto(ping_message.encode("utf-8"))
        print("Heartbeat PING sent")
    async def heartbeat_loop(self):
        while True:
            self.send_heartbeat()
            await asyncio.sleep(5)
    def send_tasks(self):
        # Send TASK
        task_data = serialize_task(add, args=(10, 20))
        task_message = create_message("TASK", {"task": task_data})
        self.transport.sendto(task_message.encode("utf-8"))
        print("Sent serialized TASK message")
    def datagram_received(self, data, addr):
        try:
            message = parse_message(data.decode("utf-8"))
            message_type = message["type"]
            message_data = message["data"]
            if message_type == "HELLO_ACK":
                print("HELLO_ACK received:", message_data)
                if message_data.get("registered"):
                    self.hello_received = True
                    print("Peer registration successful!")
                    known_peers = message_data.get("peers", [])
                    print("Discovered peers:")
                    for peer in known_peers:
                        print(
                            f"Peer ID: {peer['peer_id']}, "
                            f"Host: {peer['host']}, "
                            f"Port: {peer['port']}"
                        )
                        node_id = peer.get("node_id")
                        if node_id:
                            added = self.routing_table.add_node(node_id)
                            if added:
                                print("Peer added to Kademlia routing table!")
                            else:
                                print("Peer already exists or could not be added.")
                    # Start heartbeat after peer discovery
                    if self.heartbeat_task is None:
                        self.heartbeat_task = asyncio.create_task(
                            self.heartbeat_loop()
                        )
                    self.send_tasks()
                else:
                    print("Peer registration failed")
            elif message_type == "PEER_JOINED":
                new_peer = message_data.get("peer", {})
                print("New peer joined:", new_peer)
                node_id = new_peer.get("node_id")
                if node_id:
                    added = self.routing_table.add_node(node_id)
                    if added:
                        print("New peer added to Kademlia routing table!")
                    else:
                        print("Peer already exists or could not be added.")
            elif message_type == "PEER_LEFT":
                peer_id = message_data.get("peer_id")
                node_id = message_data.get("node_id")
                print(f"Peer went offline: {peer_id}")
                if node_id:
                    self.routing_table.remove_node(node_id)
                    print("Offline peer removed from routing table!")
            elif message_type == "PONG":
                print("Health check passed!")
            elif message_type == "TASK_RESULT":
                print("Task result received:", message_data["result"])
                self.reply_received.set()
            elif message_type == "TASK_ERROR":
                print("Task failed:", message_data.get("error"))
                self.reply_received.set()
            else:
                print("Unexpected response:", message)
        except (UnicodeDecodeError, ValueError, KeyError, TypeError) as exc:
            print(f"Invalid server response: {exc}")
    def stop_heartbeat(self):
        if self.heartbeat_task is not None:
            self.heartbeat_task.cancel()
            self.heartbeat_task = None
            print("Heartbeat stopped")
    def error_received(self, exc):
        print(f"UDP error: {exc}")
async def main():
    loop = asyncio.get_running_loop()
    transport, protocol = await loop.create_datagram_endpoint(
        lambda: UDPClientProtocol(),
        remote_addr=("127.0.0.1", 9999),
    )
    try:
        print("Client listening for peer notifications for 60 seconds...")
        await asyncio.sleep(60)
    except asyncio.CancelledError:
        pass
    finally:
        protocol.stop_heartbeat()
        transport.close()
        print("Client connection closed.")
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nClient stopped safely.")
