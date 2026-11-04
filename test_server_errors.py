import json

from meshweaver.udp_server import UDPServerProtocol
from meshweaver.serialization import serialize_task
from meshweaver.protocol import create_message


class FakeTransport:
    def __init__(self):
        self.sent_messages = []

    def sendto(self, data, addr):
        self.sent_messages.append((data.decode("utf-8"), addr))


def test_task_execution_error():
    protocol = UDPServerProtocol()
    transport = FakeTransport()
    protocol.connection_made(transport)

    message = b'{"type": "TASK", "data": {}}'
    addr = ("127.0.0.1", 12345)

    protocol.datagram_received(message, addr)

    assert len(transport.sent_messages) == 1

    response = json.loads(transport.sent_messages[0][0])
    assert response["type"] == "TASK_ERROR"
    assert "error" in response["data"]


def divide(a, b):
    return a / b


def test_function_execution_error():
    protocol = UDPServerProtocol()
    transport = FakeTransport()
    protocol.connection_made(transport)

    encoded_task = serialize_task(divide, args=(10, 0))
    message = create_message("TASK", {"task": encoded_task})
    addr = ("127.0.0.1", 12345)

    protocol.datagram_received(message.encode("utf-8"), addr)

    assert len(transport.sent_messages) == 1

    response = json.loads(transport.sent_messages[0][0])
    assert response["type"] == "TASK_ERROR"
    assert response["data"]["error"] == "division by zero"


if __name__ == "__main__":
    test_task_execution_error()
    test_function_execution_error()
    print("Server error tests passed!")

def test_successful_task_execution():
    protocol = UDPServerProtocol()
    transport = FakeTransport()
    protocol.connection_made(transport)

    encoded_task = serialize_task(lambda a, b: a + b, args=(10, 20))
    message = create_message("TASK", {"task": encoded_task})
    addr = ("127.0.0.1", 12345)

    protocol.datagram_received(message.encode("utf-8"), addr)

    assert len(transport.sent_messages) == 1

    response = json.loads(transport.sent_messages[0][0])
    assert response["type"] == "TASK_RESULT"
    assert response["data"]["result"] == 30
    assert transport.sent_messages[0][1] == addr

def test_malformed_json_packet():
    protocol = UDPServerProtocol()
    transport = FakeTransport()
    protocol.connection_made(transport)

    addr = ("127.0.0.1", 12345)
    protocol.datagram_received(b"{invalid json}", addr)

    assert len(transport.sent_messages) == 1
    response = json.loads(transport.sent_messages[0][0])
    assert response["type"] == "TASK_ERROR"
    assert "error" in response["data"]
    assert transport.sent_messages[0][1] == addr


def test_unsupported_message_type():
    protocol = UDPServerProtocol()
    transport = FakeTransport()
    protocol.connection_made(transport)

    message = create_message("UNKNOWN", {})
    addr = ("127.0.0.1", 12345)
    protocol.datagram_received(message.encode("utf-8"), addr)

    assert len(transport.sent_messages) == 1
    response = json.loads(transport.sent_messages[0][0])
    assert response["type"] == "TASK_ERROR"
    assert "Unsupported message type" in response["data"]["error"]
    assert transport.sent_messages[0][1] == addr
