import asyncio
import json
import threading

import meshweaver.udp_server as udp_server


class FakeTransport:
    def __init__(self):
        self.sent_messages = []

    def sendto(self, data, addr):
        self.sent_messages.append(json.loads(data.decode("utf-8")))


async def wait_until(condition, timeout=3):
    async def check():
        while not condition():
            await asyncio.sleep(0.01)

    await asyncio.wait_for(check(), timeout=timeout)


def send_task(protocol, task_id):
    message = {
        "type": "TASK",
        "data": {
            "task_id": task_id,
            "task": {},
        },
    }

    protocol.datagram_received(
        json.dumps(message).encode("utf-8"),
        ("127.0.0.1", 5000),
    )


def get_response_type(transport, task_id):
    for message in reversed(transport.sent_messages):
        data = message.get("data", {})
        if data.get("task_id") == task_id:
            return message.get("type")

    return None


def cleanup_protocol(protocol, release):
    release.set()

    for task in list(protocol.active_tasks):
        if not task.done():
            task.cancel()


def test_server_accepts_maximum_concurrent_tasks(monkeypatch):
    async def run_test():
        release = threading.Event()
        started = 0
        lock = threading.Lock()

        def fake_execute(task_data):
            nonlocal started
            with lock:
                started += 1
            release.wait(timeout=5)
            return "completed"

        monkeypatch.setattr(
            udp_server, "execute_task_sync", fake_execute
        )

        protocol = udp_server.UDPServerProtocol(
            max_concurrent_tasks=10
        )
        transport = FakeTransport()
        protocol.connection_made(transport)

        try:
            for number in range(10):
                send_task(protocol, f"task-{number}")

            await wait_until(lambda: started == 10)

            assert len(protocol.active_tasks) == 10
            assert len(protocol.in_flight_tasks) == 10

            assert not any(
                message.get("type") == "TASK_ERROR"
                for message in transport.sent_messages
            )
        finally:
            cleanup_protocol(protocol, release)
            await asyncio.sleep(0.1)

    asyncio.run(run_test())


def test_server_rejects_task_when_limit_is_reached(monkeypatch):
    async def run_test():
        release = threading.Event()
        started = 0
        lock = threading.Lock()

        def fake_execute(task_data):
            nonlocal started
            with lock:
                started += 1
            release.wait(timeout=5)
            return "completed"

        monkeypatch.setattr(
            udp_server, "execute_task_sync", fake_execute
        )

        protocol = udp_server.UDPServerProtocol(
            max_concurrent_tasks=10
        )
        transport = FakeTransport()
        protocol.connection_made(transport)

        try:
            for number in range(10):
                send_task(protocol, f"task-{number}")

            await wait_until(lambda: started == 10)

            send_task(protocol, "task-11")

            assert len(protocol.active_tasks) == 10
            assert get_response_type(
                transport, "task-11"
            ) == "TASK_ERROR"

            error_message = next(
                message["data"]["error"]
                for message in transport.sent_messages
                if message.get("data", {}).get("task_id") == "task-11"
            )

            assert "maximum concurrent task limit reached" in error_message

        finally:
            cleanup_protocol(protocol, release)
            await asyncio.sleep(0.1)

    asyncio.run(run_test())


def test_server_accepts_new_task_after_slot_is_freed(monkeypatch):
    async def run_test():
        release = threading.Event()

        def fake_execute(task_data):
            release.wait(timeout=5)
            return "completed"

        monkeypatch.setattr(
            udp_server, "execute_task_sync", fake_execute
        )

        protocol = udp_server.UDPServerProtocol(
            max_concurrent_tasks=1
        )
        transport = FakeTransport()
        protocol.connection_made(transport)

        try:
            send_task(protocol, "first-task")

            await wait_until(
                lambda: len(protocol.active_tasks) == 1
            )

            release.set()

            await wait_until(
                lambda: len(protocol.active_tasks) == 0
            )

            send_task(protocol, "second-task")

            await wait_until(
                lambda: get_response_type(
                    transport, "second-task"
                ) is not None
            )

            assert get_response_type(
                transport, "first-task"
            ) == "TASK_RESULT"

            assert get_response_type(
                transport, "second-task"
            ) == "TASK_RESULT"

        finally:
            cleanup_protocol(protocol, release)
            await asyncio.sleep(0.1)

    asyncio.run(run_test())
