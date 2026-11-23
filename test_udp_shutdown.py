import asyncio
import threading

import meshweaver.udp_server as udp_server
from meshweaver.protocol import create_message


class FakeTransport:
    def __init__(self):
        self.sent_messages = []

    def sendto(self, data, addr):
        self.sent_messages.append(data.decode("utf-8"))


def test_server_shutdown_marks_running_task_as_cancelled(monkeypatch):
    async def run_test():
        started = threading.Event()
        release = threading.Event()

        def slow_execute(task_data):
            started.set()
            release.wait(timeout=5)
            return 100

        monkeypatch.setattr(udp_server, "execute_task_sync", slow_execute)

        protocol = udp_server.UDPServerProtocol()
        transport = FakeTransport()
        protocol.connection_made(transport)

        task_id = "shutdown-cancel-001"
        addr = ("127.0.0.1", 5001)
        message = create_message(
            "TASK",
            {"task_id": task_id, "task": "dummy"}
        )

        try:
            protocol.datagram_received(message.encode("utf-8"), addr)

            await asyncio.wait_for(
                asyncio.to_thread(started.wait),
                timeout=3
            )

            protocol.connection_lost(None)
            await asyncio.sleep(0.05)

            assert protocol.task_history[task_id]["status"] == "CANCELLED"
            assert len(protocol.active_tasks) == 0
            assert (addr, task_id) not in protocol.in_flight_tasks

        finally:
            release.set()

    asyncio.run(run_test())
