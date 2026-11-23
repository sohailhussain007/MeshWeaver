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

def test_client_uses_configured_server_address(monkeypatch):
    async def run_test():
        loop = asyncio.get_running_loop()
        captured = {}

        async def fake_create_datagram_endpoint(*args, **kwargs):
            captured.update(kwargs)

            class FakeProtocol:
                def __init__(self):
                    self.reply_received = asyncio.Event()
                    self.reply_received.set()

            class FakeTransport:
                def close(self):
                    pass

            return FakeTransport(), FakeProtocol()

        monkeypatch.setattr(
            loop,
            "create_datagram_endpoint",
            fake_create_datagram_endpoint,
        )

        await main(host="192.168.1.10", port=8888)

        assert captured["remote_addr"] == ("192.168.1.10", 8888)

    asyncio.run(run_test())