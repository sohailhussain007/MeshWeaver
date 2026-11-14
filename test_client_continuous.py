import asyncio
import pytest

from meshweaver.udp_client import main


def test_client_stays_active_after_task_response(monkeypatch):

    async def run_test():
        loop = asyncio.get_running_loop()
        protocol_ready = asyncio.Event()
        captured = {}

        class FakeProtocol:
            def __init__(self):
                self.reply_received = asyncio.Event()
                self.heartbeat_stopped = False
                self.retry_stopped = False

            def stop_heartbeat(self):
                self.heartbeat_stopped = True

            def stop_task_retry(self):
                self.retry_stopped = True

        class FakeTransport:
            def __init__(self):
                self.closed = False

            def close(self):
                self.closed = True

        async def fake_create_datagram_endpoint(*args, **kwargs):
            protocol = FakeProtocol()
            transport = FakeTransport()

            captured["protocol"] = protocol
            captured["transport"] = transport

            protocol_ready.set()

            return transport, protocol

        monkeypatch.setattr(
            loop,
            "create_datagram_endpoint",
            fake_create_datagram_endpoint,
        )

        client_task = asyncio.create_task(
            main(continuous=True)
        )

        await protocol_ready.wait()

        protocol = captured["protocol"]

        # Simulate receiving a task response
        protocol.reply_received.set()

        await asyncio.sleep(0.01)

        # Client must remain active after task response
        assert not client_task.done()
        assert not captured["transport"].closed

        # Stop client and verify cleanup
        client_task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await client_task

        assert captured["transport"].closed
        assert protocol.heartbeat_stopped
        assert protocol.retry_stopped

    asyncio.run(run_test())
