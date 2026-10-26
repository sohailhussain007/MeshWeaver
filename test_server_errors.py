
import asyncio

from meshweaver.udp_server import UDPServerProtocol


class FakeTransport:
    def __init__(self):
        self.sent_messages = []

    def sendto(self, data, addr):
        self.sent_messages.append((data.decode("utf-8"), addr))


def test_task_execution_error():
    protocol = UDPServerProtocol()
    transport = FakeTransport()
    protocol.connection_made(transport)

    # A valid TASK message with a missing task field
    message = b'{"type": "TASK", "data": {}}'
    addr = ("127.0.0.1", 12345)

    protocol.datagram_received(message, addr)

    assert len(transport.sent_messages) == 1
    assert transport.sent_messages[0][0].startswith("ERROR: Invalid message:")
    print("Error response verified!")
    print("Malformed task was rejected without crashing the server!")

def divide(a, b):
    return a / b


def test_function_execution_error():
    from meshweaver.serialization import serialize_task
    from meshweaver.protocol import create_message

    protocol = UDPServerProtocol()
    transport = FakeTransport()
    protocol.connection_made(transport)

    encoded_task = serialize_task(divide, args=(10, 0))
    message = create_message("TASK", {"task": encoded_task})
    addr = ("127.0.0.1", 12345)

    protocol.datagram_received(message.encode("utf-8"), addr)

    assert len(transport.sent_messages) == 1
    assert transport.sent_messages[0][0].startswith("ERROR:")
    print("Function execution error response verified!")

if __name__ == "__main__":
    test_task_execution_error()
    test_function_execution_error()