import asyncio
import threading

import meshweaver.udp_server as udp_server
from meshweaver.protocol import create_message


class FakeTransport:
    def __init__(self):
        self.sent_messages = []

    def sendto(self, data, addr):
        self.sent_messages.append(data.decode("utf-8"))


async def wait_for_status(protocol, task_id, expected_status):
    async def check():
        while (
            protocol.task_history[task_id]["status"]
            != expected_status
        ):
            await asyncio.sleep(0.01)

    await asyncio.wait_for(check(), timeout=3)


def test_task_lifecycle_pending_running_completed(monkeypatch):
    async def run_test():
        started = threading.Event()
        release = threading.Event()

        def fake_execute(task_data):
            started.set()
            release.wait(timeout=5)
            return 30

        monkeypatch.setattr(
            udp_server,
            "execute_task_sync",
            fake_execute,
        )

        protocol = udp_server.UDPServerProtocol()
        transport = FakeTransport()
        protocol.connection_made(transport)

        task_id = "lifecycle-success-test"

        message = create_message(
            "TASK",
            {"task_id": task_id, "task": "dummy"},
        )

        try:
            protocol.datagram_received(
                message.encode("utf-8"),
                ("127.0.0.1", 5001),
            )

            assert protocol.task_history[task_id]["status"] == "PENDING"

            await asyncio.wait_for(
                asyncio.to_thread(started.wait),
                timeout=3,
            )

            await wait_for_status(protocol, task_id, "RUNNING")

            assert protocol.task_history[task_id]["status"] == "RUNNING"

            release.set()

            await wait_for_status(protocol, task_id, "COMPLETED")

            history = protocol.task_history[task_id]

            assert history["status"] == "COMPLETED"
            assert history["result"] == 30
            assert history["created_at"] is not None
            assert history["started_at"] is not None
            assert history["completed_at"] is not None

        finally:
            release.set()
            protocol.connection_lost(None)

    asyncio.run(run_test())


def test_task_lifecycle_failed(monkeypatch):
    async def run_test():
        def fake_execute(task_data):
            raise ValueError("Task execution failed")

        monkeypatch.setattr(
            udp_server,
            "execute_task_sync",
            fake_execute,
        )

        protocol = udp_server.UDPServerProtocol()
        transport = FakeTransport()
        protocol.connection_made(transport)

        task_id = "lifecycle-failure-test"

        message = create_message(
            "TASK",
            {"task_id": task_id, "task": "dummy"},
        )

        protocol.datagram_received(
            message.encode("utf-8"),
            ("127.0.0.1", 5001),
        )

        await wait_for_status(protocol, task_id, "FAILED")

        history = protocol.task_history[task_id]

        assert history["status"] == "FAILED"
        assert history["error"] == "Task execution failed"
        assert history["created_at"] is not None
        assert history["started_at"] is not None
        assert history["completed_at"] is not None

        protocol.connection_lost(None)

    asyncio.run(run_test())
