import asyncio

from meshweaver.udp_client import UDPClientProtocol
from meshweaver.protocol import parse_message
from peer import Peer


class FakeTransport:

    def __init__(self):
        self.sent_messages = []

    def sendto(self, data):
        self.sent_messages.append(
            parse_message(data.decode("utf-8"))
        )


def test_task_retries_when_response_is_missing():

    async def run_test():
        protocol = UDPClientProtocol()

        protocol.task_timeout = 0.02
        protocol.max_task_retries = 3

        transport = FakeTransport()
        protocol.transport = transport
        protocol.peer = Peer(host="127.0.0.1", port=5001)

        protocol.task_message = (
            '{"type":"TASK","data":{"task":"test"}}'
        )

        await protocol.send_task_with_retry()

        assert len(transport.sent_messages) == 4
        assert protocol.task_retry_count == 3
        assert not protocol.reply_received.is_set()

    asyncio.run(run_test())


def test_task_retry_stops_after_response():

    async def run_test():
        protocol = UDPClientProtocol()

        protocol.task_timeout = 0.1
        protocol.max_task_retries = 3

        transport = FakeTransport()
        protocol.transport = transport
        protocol.peer = Peer(host="127.0.0.1", port=5001)

        protocol.task_message = (
            '{"type":"TASK","data":{"task":"test"}}'
        )

        async def send_response():
            await asyncio.sleep(0.01)
            protocol.reply_received.set()

        response_task = asyncio.create_task(send_response())

        await protocol.send_task_with_retry()
        await response_task

        assert len(transport.sent_messages) == 1
        assert protocol.reply_received.is_set()

    asyncio.run(run_test())


def test_task_stops_after_maximum_retries():

    async def run_test():
        protocol = UDPClientProtocol()

        protocol.task_timeout = 0.01
        protocol.max_task_retries = 3

        transport = FakeTransport()
        protocol.transport = transport
        protocol.peer = Peer(host="127.0.0.1", port=5001)

        protocol.task_message = (
            '{"type":"TASK","data":{"task":"test"}}'
        )

        await protocol.send_task_with_retry()

        assert len(transport.sent_messages) == 4
        assert protocol.task_retry_count == 3
        assert not protocol.reply_received.is_set()

    asyncio.run(run_test())
