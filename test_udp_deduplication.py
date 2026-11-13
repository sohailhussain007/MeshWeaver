import json

from meshweaver.protocol import create_message, parse_message
from meshweaver import udp_server


def test_duplicate_task_executes_only_once(monkeypatch):

    execution_count = {"count": 0}

    def fake_task_function(*args, **kwargs):
        execution_count["count"] += 1
        return 30

    monkeypatch.setattr(
        udp_server,
        "deserialize_task",
        lambda task_data: {
            "function": fake_task_function,
            "args": (),
            "kwargs": {},
        },
    )

    class FakeTransport:

        def __init__(self):
            self.sent_messages = []

        def sendto(self, data, addr):
            self.sent_messages.append(
                parse_message(data.decode("utf-8"))
            )

    protocol = udp_server.UDPServerProtocol()
    transport = FakeTransport()

    protocol.connection_made(transport)

    addr = ("127.0.0.1", 5001)

    message = create_message(
        "TASK",
        {
            "task_id": "task-101",
            "task": "dummy-task-data",
        },
    )

    # Send the same task twice
    protocol.datagram_received(
        message.encode("utf-8"),
        addr,
    )

    protocol.datagram_received(
        message.encode("utf-8"),
        addr,
    )

    assert execution_count["count"] == 1
    assert len(transport.sent_messages) == 2

    first_response = transport.sent_messages[0]
    second_response = transport.sent_messages[1]

    assert first_response["type"] == "TASK_RESULT"
    assert second_response["type"] == "TASK_RESULT"

    assert first_response == second_response
    assert first_response["data"]["task_id"] == "task-101"
    assert first_response["data"]["result"] == 30
