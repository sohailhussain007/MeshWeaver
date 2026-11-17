import asyncio
import json
import time

from meshweaver.protocol import create_message, parse_message
from meshweaver import udp_server


def test_duplicate_task_executes_only_once(monkeypatch):

    async def run_test():
        execution_count = {"count": 0}

        def fake_task_function(*args, **kwargs):
            execution_count["count"] += 1
            time.sleep(0.2)
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

        encoded_message = message.encode("utf-8")

        # First request schedules execution.
        protocol.datagram_received(encoded_message, addr)

        # Duplicate arrives while task is running.
        protocol.datagram_received(encoded_message, addr)

        assert execution_count["count"] == 0
        assert len(protocol.in_flight_tasks) == 1

        async def wait_for_response():
            while not transport.sent_messages:
                await asyncio.sleep(0.01)

        await asyncio.wait_for(wait_for_response(), timeout=3)

        assert execution_count["count"] == 1
        assert len(transport.sent_messages) == 1

        first_response = transport.sent_messages[0]

        assert first_response["type"] == "TASK_RESULT"
        assert first_response["data"]["task_id"] == "task-101"
        assert first_response["data"]["result"] == 30

        # Duplicate arrives after completion.
        protocol.datagram_received(encoded_message, addr)

        assert len(transport.sent_messages) == 2
        assert transport.sent_messages[1] == first_response

        assert execution_count["count"] == 1
        assert len(protocol.in_flight_tasks) == 0

    asyncio.run(run_test())
