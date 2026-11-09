import json

from meshweaver.udp_server import UDPServerProtocol
from meshweaver.protocol import create_message


class FakeTransport:
    def __init__(self):
        self.sent_data = None
        self.sent_addr = None

    def sendto(self, data, addr):
        self.sent_data = data
        self.sent_addr = addr


def test_udp_server_heartbeat_ack():
    server = UDPServerProtocol()
    transport = FakeTransport()
    server.connection_made(transport)

    message = create_message(
        "HEARTBEAT",
        {"peer_id": "node_1"},
    )

    address = ("127.0.0.1", 5001)

    server.datagram_received(
        message.encode("utf-8"),
        address,
    )

    response = json.loads(transport.sent_data.decode("utf-8"))

    assert response["type"] == "HEARTBEAT_ACK"
    assert response["data"]["peer_id"] == "node_1"
    assert transport.sent_addr == address


def test_udp_server_rejects_invalid_heartbeat():
    server = UDPServerProtocol()
    transport = FakeTransport()
    server.connection_made(transport)

    message = create_message("HEARTBEAT", {})

    server.datagram_received(
        message.encode("utf-8"),
        ("127.0.0.1", 5001),
    )

    response = json.loads(transport.sent_data.decode("utf-8"))

    assert response["type"] == "TASK_ERROR"
    assert response["data"]["error"] == "Invalid heartbeat peer_id"
