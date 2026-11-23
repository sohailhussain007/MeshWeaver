import asyncio
import json

from meshweaver.udp_server import UDPServerProtocol
from meshweaver.serialization import serialize_task
from meshweaver.protocol import create_message


class FakeTransport:
    def __init__(self):
        self.sent_messages = []

    def sendto(self, data, addr):
        self.sent_messages.append((data.decode("utf-8"), addr))


def divide(a, b):
    return a / b


async def send_and_wait(protocol, transport, message, addr):
    previous_count = len(transport.sent_messages)

    protocol.datagram_received(
        message.encode("utf-8"),
        addr,
    )

    async def wait_for_response():
        while len(transport.sent_messages) <= previous_count:
            await asyncio.sleep(0.01)

    await asyncio.wait_for(wait_for_response(), timeout=3)

    return json.loads(
        transport.sent_messages[-1][0]
    )


def test_task_execution_error():
    async def run_test():
        protocol = UDPServerProtocol()
        transport = FakeTransport()
        protocol.connection_made(transport)

        message = '{"type": "TASK", "data": {}}'
        addr = ("127.0.0.1", 12345)

        response = await send_and_wait(
            protocol,
            transport,
            message,
            addr,
        )

        assert response["type"] == "TASK_ERROR"
        assert "error" in response["data"]

    asyncio.run(run_test())


def test_function_execution_error():
    async def run_test():
        protocol = UDPServerProtocol()
        transport = FakeTransport()
        protocol.connection_made(transport)

        encoded_task = serialize_task(divide, args=(10, 0))
        message = create_message("TASK", {"task": encoded_task})
        addr = ("127.0.0.1", 12345)

        response = await send_and_wait(
            protocol,
            transport,
            message,
            addr,
        )

        assert response["type"] == "TASK_ERROR"
        assert response["data"]["error"] == "division by zero"

    asyncio.run(run_test())


def test_successful_task_execution():
    async def run_test():
        protocol = UDPServerProtocol()
        transport = FakeTransport()
        protocol.connection_made(transport)

        encoded_task = serialize_task(
            lambda a, b: a + b,
            args=(10, 20),
        )
        message = create_message("TASK", {"task": encoded_task})
        addr = ("127.0.0.1", 12345)

        response = await send_and_wait(
            protocol,
            transport,
            message,
            addr,
        )

        assert response["type"] == "TASK_RESULT"
        assert response["data"]["result"] == 30
        assert transport.sent_messages[-1][1] == addr

    asyncio.run(run_test())


def test_malformed_json_packet():
    protocol = UDPServerProtocol()
    transport = FakeTransport()
    protocol.connection_made(transport)

    addr = ("127.0.0.1", 12345)
    protocol.datagram_received(b"{invalid json}", addr)

    assert len(transport.sent_messages) == 1

    response = json.loads(transport.sent_messages[0][0])

    assert response["type"] == "TASK_ERROR"
    assert "error" in response["data"]
    assert transport.sent_messages[0][1] == addr


def test_unsupported_message_type():
    protocol = UDPServerProtocol()
    transport = FakeTransport()
    protocol.connection_made(transport)

    message = create_message("UNKNOWN", {})
    addr = ("127.0.0.1", 12345)

    protocol.datagram_received(
        message.encode("utf-8"),
        addr,
    )

    assert len(transport.sent_messages) == 1

    response = json.loads(transport.sent_messages[0][0])

    assert response["type"] == "TASK_ERROR"
    assert "Unsupported message type" in response["data"]["error"]
    assert transport.sent_messages[0][1] == addr


def test_missing_task_payload():
    async def run_test():
        protocol = UDPServerProtocol()
        transport = FakeTransport()
        protocol.connection_made(transport)

        message = create_message("TASK", {})
        addr = ("127.0.0.1", 12345)

        response = await send_and_wait(
            protocol, transport, message, addr
        )

        assert response["type"] == "TASK_ERROR"
        assert "error" in response["data"]

    asyncio.run(run_test())


def test_invalid_serialized_task():
    async def run_test():
        protocol = UDPServerProtocol()
        transport = FakeTransport()
        protocol.connection_made(transport)

        message = create_message(
            "TASK",
            {"task": "this-is-not-a-valid-serialized-task"},
        )
        addr = ("127.0.0.1", 12345)

        response = await send_and_wait(
            protocol, transport, message, addr
        )

        assert response["type"] == "TASK_ERROR"
        assert "error" in response["data"]

    asyncio.run(run_test())


def test_invalid_task_id():
    protocol = UDPServerProtocol()
    transport = FakeTransport()
    protocol.connection_made(transport)

    message = create_message(
        "TASK",
        {
            "task_id": "",
            "task": "dummy-task",
        },
    )
    addr = ("127.0.0.1", 12345)

    protocol.datagram_received(
        message.encode("utf-8"),
        addr,
    )

    assert len(transport.sent_messages) == 1

    response = json.loads(transport.sent_messages[0][0])

    assert response["type"] == "TASK_ERROR"
    assert response["data"]["error"] == "Invalid task_id"
