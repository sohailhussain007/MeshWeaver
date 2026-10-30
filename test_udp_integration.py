import asyncio

from meshweaver.udp_server import UDPServerProtocol
from meshweaver.protocol import create_message, parse_message
from meshweaver.serialization import serialize_task


async def run_ping_test():
    loop = asyncio.get_running_loop()
    response_future = loop.create_future()

    class TestClient(asyncio.DatagramProtocol):
        def datagram_received(self, data, addr):
            if not response_future.done():
                response_future.set_result(
                    parse_message(data.decode("utf-8"))
                )

    server_transport, _ = await loop.create_datagram_endpoint(
        UDPServerProtocol,
        local_addr=("127.0.0.1", 0),
    )

    port = server_transport.get_extra_info("sockname")[1]

    client_transport, _ = await loop.create_datagram_endpoint(
        TestClient,
        remote_addr=("127.0.0.1", port),
    )

    try:
        message = create_message("PING", {})
        client_transport.sendto(message.encode("utf-8"))

        response = await asyncio.wait_for(response_future, timeout=2)

        assert response["type"] == "PONG"

    finally:
        client_transport.close()
        server_transport.close()


def test_udp_ping_pong():
    asyncio.run(run_ping_test())

def multiply(a, b):
    return a * b


async def run_task_test():
    loop = asyncio.get_running_loop()
    response_future = loop.create_future()

    class TestClient(asyncio.DatagramProtocol):
        def datagram_received(self, data, addr):
            if not response_future.done():
                response_future.set_result(
                    parse_message(data.decode("utf-8"))
                )

    server_transport, _ = await loop.create_datagram_endpoint(
        UDPServerProtocol,
        local_addr=("127.0.0.1", 0),
    )

    port = server_transport.get_extra_info("sockname")[1]

    client_transport, _ = await loop.create_datagram_endpoint(
        TestClient,
        remote_addr=("127.0.0.1", port),
    )

    try:
        task_data = serialize_task(multiply, args=(10, 3))
        message = create_message("TASK", {"task": task_data})

        client_transport.sendto(message.encode("utf-8"))

        response = await asyncio.wait_for(response_future, timeout=2)

        assert response["type"] == "TASK_RESULT"
        assert response["data"]["result"] == 30

    finally:
        client_transport.close()
        server_transport.close()


def test_udp_task_execution():
    asyncio.run(run_task_test())


def divide(a, b):
    return a / b


async def run_task_error_test():
    loop = asyncio.get_running_loop()
    response_future = loop.create_future()

    class TestClient(asyncio.DatagramProtocol):
        def datagram_received(self, data, addr):
            if not response_future.done():
                response_future.set_result(
                    parse_message(data.decode("utf-8"))
                )

    server_transport, _ = await loop.create_datagram_endpoint(
        UDPServerProtocol,
        local_addr=("127.0.0.1", 0),
    )

    port = server_transport.get_extra_info("sockname")[1]

    client_transport, _ = await loop.create_datagram_endpoint(
        TestClient,
        remote_addr=("127.0.0.1", port),
    )

    try:
        task_data = serialize_task(divide, args=(10, 0))
        message = create_message("TASK", {"task": task_data})

        client_transport.sendto(message.encode("utf-8"))

        response = await asyncio.wait_for(response_future, timeout=2)

        assert response["type"] == "TASK_ERROR"
        assert response["data"]["error"] == "division by zero"

    finally:
        client_transport.close()
        server_transport.close()


def test_udp_task_error():
    asyncio.run(run_task_error_test())
