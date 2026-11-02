import asyncio
from meshweaver.protocol import create_message, parse_message
from meshweaver.serialization import serialize_task
from peer import Peer
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
        # Get the actual local UDP port
        local_addr = transport.get_extra_info("sockname")
        # Create this client's peer identity
        self.peer = Peer(
            host="127.0.0.1",
            port=local_addr[1],
        )
        # Send HELLO first
        hello_message = create_hello_message(self.peer)
        self.transport.sendto(encode_message(hello_message))
        print("HELLO sent:", self.peer)
    def send_tasks(self):
        # Send PING
        ping_message = create_message(
            "PING",
            {"node_id": self.peer.peer_id},
        )
        self.transport.sendto(ping_message.encode("utf-8"))
        print("Sent PING message")
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
                    self.send_tasks()
                else:
                    print("Peer registration failed")
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
