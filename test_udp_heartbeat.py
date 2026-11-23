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

    address = ("127.0.0.1", 5001)
    server.peer_addresses["node_1"] = address

    message = create_message(
        "HEARTBEAT",
        {"peer_id": "node_1", "cpu_load": 20.0},
    )

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

def test_client_sends_periodic_heartbeats():
    import asyncio
    import json

    from meshweaver.udp_client import UDPClientProtocol
    from peer import Peer

    class FakeHeartbeatTransport:
        def __init__(self):
            self.sent_messages = []
            self.three_heartbeats_sent = asyncio.Event()

        def sendto(self, data):
            self.sent_messages.append(json.loads(data.decode("utf-8")))

            if len(self.sent_messages) >= 3:
                self.three_heartbeats_sent.set()

    async def run_test():
        protocol = UDPClientProtocol(heartbeat_interval=0.01)
        protocol.ack_timeout = 0.01
        transport = FakeHeartbeatTransport()

        protocol.transport = transport
        protocol.peer = Peer(host="127.0.0.1", port=5001)

        protocol.start_heartbeat()

        try:
            await asyncio.wait_for(
                transport.three_heartbeats_sent.wait(),
                timeout=2,
            )
        finally:
            heartbeat_task = protocol.heartbeat_task
            protocol.stop_heartbeat()

            if heartbeat_task is not None:
                await heartbeat_task

        assert len(transport.sent_messages) >= 3

        for message in transport.sent_messages:
            assert message["type"] == "HEARTBEAT"
            assert message["data"]["peer_id"] == protocol.peer.peer_id
            assert "cpu_load" in message["data"]
            assert 0 <= message["data"]["cpu_load"] <= 100

    asyncio.run(run_test())


def test_udp_server_updates_cpu_load_and_selects_lowest_peer():
    from peer import Peer
    from peer_registry import PeerRegistry

    registry = PeerRegistry()

    peer_one = Peer(peer_id="peer_one", host="127.0.0.1", port=5001)
    peer_two = Peer(peer_id="peer_two", host="127.0.0.1", port=5002)

    registry.register_peer(peer_one)
    registry.register_peer(peer_two)

    server = UDPServerProtocol(peer_registry=registry)
    transport = FakeTransport()
    server.connection_made(transport)

    server.peer_addresses["peer_one"] = ("127.0.0.1", 5001)
    server.peer_addresses["peer_two"] = ("127.0.0.1", 5002)

    for peer_id, cpu_load, port in (
        ("peer_one", 75.0, 5001),
        ("peer_two", 25.0, 5002),
    ):
        message = create_message(
            "HEARTBEAT",
            {"peer_id": peer_id, "cpu_load": cpu_load},
        )
        server.datagram_received(
            message.encode("utf-8"),
            ("127.0.0.1", port),
        )

    assert registry.cpu_loads["peer_one"] == 75.0
    assert registry.cpu_loads["peer_two"] == 25.0
    assert registry.select_lowest_cpu_peer().peer_id == "peer_two"

    response = json.loads(transport.sent_data.decode("utf-8"))
    assert response["type"] == "HEARTBEAT_ACK"

def test_peer_registry_removes_stale_peers():
    import time

    from peer import Peer
    from peer_registry import PeerRegistry

    registry = PeerRegistry()

    stale_peer = Peer(peer_id="stale_node", host="127.0.0.1", port=5001)
    active_peer = Peer(peer_id="active_node", host="127.0.0.1", port=5002)

    registry.register_peer(stale_peer)
    registry.register_peer(active_peer)

    # Make stale_node older than the timeout.
    registry.last_seen["stale_node"] = time.monotonic() - 60

    removed = registry.remove_stale_peers(timeout=45)

    assert "stale_node" in removed
    assert "stale_node" not in registry.peers
    assert "stale_node" not in registry.cpu_loads
    assert "active_node" in registry.peers


def test_udp_server_cleanup_removes_stale_peer_address():
    import time

    from peer import Peer
    from peer_registry import PeerRegistry

    registry = PeerRegistry()

    peer = Peer(peer_id="stale_node", host="127.0.0.1", port=5001)
    registry.register_peer(peer)

    server = UDPServerProtocol(
        peer_registry=registry,
        peer_timeout=45,
    )

    server.peer_addresses["stale_node"] = ("127.0.0.1", 5001)

    # Simulate a peer that has not sent a heartbeat for 60 seconds.
    registry.last_seen["stale_node"] = time.monotonic() - 60

    removed = server._cleanup_stale_peers()

    assert "stale_node" in removed
    assert "stale_node" not in registry.peers
    assert "stale_node" not in registry.cpu_loads
    assert "stale_node" not in server.peer_addresses

