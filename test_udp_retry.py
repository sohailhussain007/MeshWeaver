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


def test_individual_task_cleanup_after_max_retries():
    async def run_test():
        protocol = UDPClientProtocol()

        protocol.task_timeout = 0.01
        protocol.max_task_retries = 3

        transport = FakeTransport()
        protocol.transport = transport

        task_id = protocol.submit_task(
            function=lambda a: a + 1,
            args=(10,),
        )

        retry_task = protocol.task_retry_tasks[task_id]

        await retry_task

        assert len(transport.sent_messages) == 4

        assert task_id not in protocol.pending_tasks
        assert task_id not in protocol.task_retry_tasks

        assert task_id in protocol.task_errors
        assert protocol.task_errors[task_id] == (
            "Maximum task retries reached"
        )

    asyncio.run(run_test())


def test_individual_task_cleanup_after_successful_response():
    async def run_test():
        protocol = UDPClientProtocol()

        protocol.task_timeout = 1
        protocol.max_task_retries = 3

        transport = FakeTransport()
        protocol.transport = transport

        task_id = protocol.submit_task(
            function=lambda a, b: a + b,
            args=(10, 20),
        )

        retry_task = protocol.task_retry_tasks[task_id]

        await asyncio.sleep(0.01)

        protocol.datagram_received(
            (
                '{"type":"TASK_RESULT","data":'
                '{"task_id":"' + task_id + '","result":30}}'
            ).encode("utf-8"),
            ("127.0.0.1", 9999),
        )

        await retry_task

        assert protocol.task_results[task_id] == 30
        assert task_id not in protocol.pending_tasks
        assert task_id not in protocol.task_retry_tasks
        assert task_id not in protocol.task_errors

        assert len(transport.sent_messages) == 1

    asyncio.run(run_test())


def test_connection_lost_cleans_up_pending_tasks():
    async def run_test():
        protocol = UDPClientProtocol()

        protocol.task_timeout = 10
        protocol.max_task_retries = 3

        transport = FakeTransport()
        protocol.transport = transport

        first_id = protocol.submit_task(
            function=lambda: 10,
        )

        second_id = protocol.submit_task(
            function=lambda: 20,
        )

        await asyncio.sleep(0.01)

        assert len(protocol.pending_tasks) == 2
        assert len(protocol.task_retry_tasks) == 2

        protocol.connection_lost(None)

        await asyncio.sleep(0)

        assert protocol.transport is None
        assert protocol.pending_tasks == {}
        assert protocol.task_retry_tasks == {}

        assert first_id not in protocol.pending_tasks
        assert second_id not in protocol.pending_tasks

    asyncio.run(run_test())


def test_connection_lost_records_pending_task_errors():
    async def run_test():
        protocol = UDPClientProtocol()
        protocol.task_timeout = 10

        transport = FakeTransport()
        protocol.transport = transport

        first_id = protocol.submit_task(lambda: 10)
        second_id = protocol.submit_task(lambda: 20)

        await asyncio.sleep(0.01)

        protocol.connection_lost(None)
        await asyncio.sleep(0)

        assert first_id in protocol.task_errors
        assert second_id in protocol.task_errors

        assert protocol.task_errors[first_id] == "Client connection lost"
        assert protocol.task_errors[second_id] == "Client connection lost"

        assert protocol.pending_tasks == {}
        assert protocol.task_retry_tasks == {}

    asyncio.run(run_test())
