import json

from meshweaver.udp_server import UDPServerProtocol
from meshweaver.udp_client import UDPClientProtocol
from meshweaver.peer_protocol import create_hello_message, encode_message
from meshweaver.protocol import create_message
from peer import Peer


class RecordingTransport:
    def __init__(self):
        self.sent_messages = []

    def get_extra_info(self, name):
        if name == "sockname":
            return ("127.0.0.1", 9999)
        return None

    def sendto(self, data, addr):
        self.sent_messages.append(
            (json.loads(data.decode("utf-8")), addr)
        )


def test_hello_ack_contains_existing_peers():
    server = UDPServerProtocol()
    transport = RecordingTransport()
    server.connection_made(transport)

    first = Peer(peer_id="peer_1", host="127.0.0.1", port=5001)
    second = Peer(peer_id="peer_2", host="127.0.0.1", port=5002)

    server.datagram_received(
        encode_message(create_hello_message(first)),
        (first.host, first.port),
    )

    transport.sent_messages.clear()

    server.datagram_received(
        encode_message(create_hello_message(second)),
        (second.host, second.port),
    )

    ack = next(
        message
        for message, address in transport.sent_messages
        if message["type"] == "HELLO_ACK"
    )

    known_peers = ack["data"]["peers"]

    assert any(peer["peer_id"] == "peer_1" for peer in known_peers)
    assert not any(peer["peer_id"] == "peer_2" for peer in known_peers)


def test_server_notifies_existing_peer_when_new_peer_joins():
    server = UDPServerProtocol()
    transport = RecordingTransport()
    server.connection_made(transport)

    first = Peer(peer_id="peer_1", host="127.0.0.1", port=5001)
    second = Peer(peer_id="peer_2", host="127.0.0.1", port=5002)

    server.datagram_received(
        encode_message(create_hello_message(first)),
        (first.host, first.port),
    )

    transport.sent_messages.clear()

    server.datagram_received(
        encode_message(create_hello_message(second)),
        (second.host, second.port),
    )

    joined = [
        message
        for message, address in transport.sent_messages
        if message["type"] == "PEER_JOINED"
    ]

    assert len(joined) == 1
    assert joined[0]["data"]["peer"]["peer_id"] == "peer_2"

    notification_address = next(
        address
        for message, address in transport.sent_messages
        if message["type"] == "PEER_JOINED"
    )
    assert notification_address == (first.host, first.port)


def test_client_adds_and_removes_discovered_peers():
    client = UDPClientProtocol()

    local_peer = Peer(peer_id="local_peer", host="127.0.0.1", port=6000)
    client.peer = local_peer

    from kademlia import RoutingTable
    client.routing_table = RoutingTable(local_peer.node_id)

    remote_peer = Peer(peer_id="remote_peer", host="127.0.0.1", port=6001)

    client.datagram_received(
        create_message(
            "PEER_JOINED",
            {"peer": remote_peer.to_dict()},
        ).encode("utf-8"),
        ("127.0.0.1", 9999),
    )

    assert "remote_peer" in client.discovered_peers

    client.datagram_received(
        create_message(
            "PEER_LEFT",
            {
                "peer_id": "remote_peer",
                "node_id": remote_peer.node_id,
            },
        ).encode("utf-8"),
        ("127.0.0.1", 9999),
    )

    assert "remote_peer" not in client.discovered_peers

    routing_nodes = [
        node_id
        for bucket in client.routing_table.buckets
        for node_id in bucket
    ]
    assert remote_peer.node_id not in routing_nodes


def test_server_notifies_remaining_peers_when_peer_goes_stale():
    server = UDPServerProtocol(peer_timeout=30)
    transport = RecordingTransport()
    server.connection_made(transport)

    first = Peer(peer_id="peer_1", host="127.0.0.1", port=5001)
    second = Peer(peer_id="peer_2", host="127.0.0.1", port=5002)

    for peer in (first, second):
        server.datagram_received(
            encode_message(create_hello_message(peer)),
            (peer.host, peer.port),
        )

    transport.sent_messages.clear()

    # Make peer_2 stale without waiting for the timeout.
    server.peer_registry.last_seen["peer_2"] -= 60

    removed = server._cleanup_stale_peers()

    assert "peer_2" in removed
    assert server.peer_registry.get_peer("peer_2") is None

    left_notifications = [
        (message, address)
        for message, address in transport.sent_messages
        if message["type"] == "PEER_LEFT"
    ]

    assert len(left_notifications) == 1
    message, address = left_notifications[0]

    assert message["data"]["peer_id"] == "peer_2"
    assert message["data"]["node_id"] == second.node_id
    assert address == (first.host, first.port)
