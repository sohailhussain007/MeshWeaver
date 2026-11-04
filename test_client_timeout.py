import asyncio
import pytest

from meshweaver.udp_client import main


def test_client_timeout(capsys, monkeypatch):
    async def run_test():
        loop = asyncio.get_running_loop()

        async def fake_create_datagram_endpoint(*args, **kwargs):
            class FakeProtocol:
                reply_received = asyncio.Event()

            class FakeTransport:
                def close(self):
                    pass

            return FakeTransport(), FakeProtocol()

        monkeypatch.setattr(
            loop,
            "create_datagram_endpoint",
            fake_create_datagram_endpoint,
        )

        await main()

    asyncio.run(run_test())

    captured = capsys.readouterr()
    assert "Server result timed out" in captured.out
