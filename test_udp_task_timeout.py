import asyncio
import json
import threading

import pytest
import meshweaver.udp_server as udp_server
from meshweaver.protocol import create_message


class FakeTransport:
    def __init__(self):
        self.sent_messages = []

    def sendto(self, data, addr):
        self.sent_messages.append(json.loads(data.decode("utf-8")))


def test_task_timeout_marks_task_as_timed_out(monkeypatch):
    async def run_test():
        started = threading.Event()
        release = threading.Event()

        def slow_execute(task_data):
            started.set()
            release.wait(timeout=5)
            return 100

        monkeypatch.setattr(
            udp_server,
            "execute_task_sync",
            slow_execute,
        )

        protocol = udp_server.UDPServerProtocol(task_timeout=0.1)
        transport = FakeTransport()
        protocol.connection_made(transport)

        task_id = "timeout-test-001"
        addr = ("127.0.0.1", 5001)

        message = create_message(
            "TASK",
            {"task_id": task_id, "task": "dummy"},
        )

        try:
            protocol.datagram_received(
                message.encode("utf-8"),
                addr,
            )

            await asyncio.wait_for(
                asyncio.to_thread(started.wait),
                timeout=3,
            )

            async def wait_for_cleanup():
                while protocol.active_tasks:
                    await asyncio.sleep(0.01)

            await asyncio.wait_for(wait_for_cleanup(), timeout=3)

            history = protocol.task_history[task_id]

            assert history["status"] == "TIMED_OUT"
            assert history["completed_at"] is not None
            assert history["error"] is not None
            assert (addr, task_id) not in protocol.in_flight_tasks

            response = transport.sent_messages[-1]
            assert response["type"] == "TASK_ERROR"
            assert response["data"]["task_id"] == task_id

        finally:
            release.set()
            protocol.connection_lost(None)

    asyncio.run(run_test())


def test_task_timeout_configuration_must_be_positive():
    with pytest.raises(ValueError):
        udp_server.UDPServerProtocol(task_timeout=0)

    with pytest.raises(ValueError):
        udp_server.UDPServerProtocol(task_timeout=-1)


def test_task_completes_before_timeout(monkeypatch):
    async def run_test():
        monkeypatch.setattr(
            udp_server,
            "execute_task_sync",
            lambda task_data: 100,
        )

        protocol = udp_server.UDPServerProtocol(task_timeout=2)
        transport = FakeTransport()
        protocol.connection_made(transport)

        task_id = "timeout-success-001"
        addr = ("127.0.0.1", 5001)

        message = create_message(
            "TASK",
            {"task_id": task_id, "task": "dummy"},
        )

        protocol.datagram_received(
            message.encode("utf-8"),
            addr,
        )

        async def wait_for_completion():
            while (
                protocol.task_history[task_id]["status"]
                != "COMPLETED"
            ):
                await asyncio.sleep(0.01)

        await asyncio.wait_for(wait_for_completion(), timeout=3)

        assert protocol.task_history[task_id]["result"] == 100
        assert transport.sent_messages[-1]["type"] == "TASK_RESULT"

        protocol.connection_lost(None)

    asyncio.run(run_test())
