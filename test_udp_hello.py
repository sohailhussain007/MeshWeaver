import json

from meshweaver.udp_server import UDPServerProtocol
from meshweaver.peer_protocol import create_hello_message, encode_message
from peer import Peer


class FakeTransport:
    def __init__(self):
        self.sent_data = None
        self.sent_addr = None

    def sendto(self, data, addr):
        self.sent_data = data
        self.sent_addr = addr


def test_udp_server_registers_hello_peer():
    server = UDPServerProtocol()
    transport = FakeTransport()
    server.connection_made(transport)

    peer = Peer(
        peer_id="node_test",
        host="127.0.0.1",
        port=5001,
    )

    message = create_hello_message(peer)
    data = encode_message(message)
    address = ("127.0.0.1", 5001)

    server.datagram_received(data, address)

    registered_peer = server.peer_registry.get_peer("node_test")

    assert registered_peer is not None
    assert registered_peer.host == "127.0.0.1"
    assert registered_peer.port == 5001

    response = json.loads(transport.sent_data.decode("utf-8"))

    assert response["type"] == "HELLO_ACK"
    assert response["data"]["peer_id"] == "node_test"
    assert response["data"]["registered"] is True
    assert transport.sent_addr == address

def test_udp_server_registers_multiple_peers():
    server = UDPServerProtocol()
    transport = FakeTransport()
    server.connection_made(transport)

    peers = [
        Peer(
            peer_id="node_1",
            host="127.0.0.1",
            port=5001,
        ),
        Peer(
            peer_id="node_2",
            host="127.0.0.1",
            port=5002,
        ),
        Peer(
            peer_id="node_3",
            host="127.0.0.1",
            port=5003,
        ),
    ]

    for peer in peers:
        message = create_hello_message(peer)
        data = encode_message(message)

        server.datagram_received(
            data,
            (peer.host, peer.port),
        )

    registered_peers = server.peer_registry.get_all_peers()

    assert len(registered_peers) == 3

    registered_ids = {
        peer.peer_id for peer in registered_peers
    }

    assert registered_ids == {
        "node_1",
        "node_2",
        "node_3",
    }