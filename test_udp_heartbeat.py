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

    asyncio.run(run_test())