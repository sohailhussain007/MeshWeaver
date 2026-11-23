import asyncio

from meshweaver.udp_server import UDPServerProtocol
from meshweaver.udp_client import UDPClientProtocol
from meshweaver.protocol import create_message, parse_message
from meshweaver.serialization import serialize_task


def multiply(a, b):
    return a * b


class WorkerProtocol(UDPClientProtocol):
    def __init__(self):
        super().__init__(heartbeat_interval=0.1)
        self.forwarded_task_received = asyncio.Event()

    async def _execute_forwarded_task(self, message_data):
        self.forwarded_task_received.set()
        await super()._execute_forwarded_task(message_data)


class RequesterProtocol(asyncio.DatagramProtocol):
    def __init__(self):
        self.response_future = asyncio.get_running_loop().create_future()

    def datagram_received(self, data, addr):
        message = parse_message(data.decode("utf-8"))

        if message["type"] in ("TASK_RESULT", "TASK_ERROR"):
            if not self.response_future.done():
                self.response_future.set_result(message)


async def wait_for_worker_registration(server, worker, timeout=5):
    async def check():
        while True:
            peer_id = worker.peer.peer_id

            if (
                worker.hello_received
                and peer_id in server.peer_addresses
                and peer_id in server.peer_registry.cpu_loads
            ):
                return

            await asyncio.sleep(0.05)

    await asyncio.wait_for(check(), timeout=timeout)


async def run_forwarding_test():
    loop = asyncio.get_running_loop()

    server_transport, server = await loop.create_datagram_endpoint(
        lambda: UDPServerProtocol(task_timeout=5),
        local_addr=("127.0.0.1", 0),
    )

    server_port = server_transport.get_extra_info("sockname")[1]

    worker_transport, worker = await loop.create_datagram_endpoint(
        WorkerProtocol,
        remote_addr=("127.0.0.1", server_port),
    )

    requester_transport, requester = await loop.create_datagram_endpoint(
        RequesterProtocol,
        remote_addr=("127.0.0.1", server_port),
    )

    try:
        await wait_for_worker_registration(server, worker)

        task_id = "forwarding-test-001"
        task_data = serialize_task(multiply, args=(10, 3))

        task_message = create_message(
            "TASK",
            {
                "task_id": task_id,
                "task": task_data,
            },
        )

        requester_transport.sendto(
            task_message.encode("utf-8")
        )

        response = await asyncio.wait_for(
            requester.response_future,
            timeout=8,
        )

        await asyncio.wait_for(
            worker.forwarded_task_received.wait(),
            timeout=3,
        )

        assert response["type"] == "TASK_RESULT", response
        assert response["data"]["result"] == 30

        assert worker.peer.peer_id in server.peer_addresses
        assert server.task_history[task_id]["status"] == "COMPLETED"

        print("PASS: Worker received TASK_FORWARD")
        print("PASS: Worker executed multiply(10, 3)")
        print("PASS: Server returned result 30 to requester")

    finally:
        requester_transport.close()
        worker_transport.close()
        server_transport.close()
        await asyncio.sleep(0)


def test_task_is_forwarded_to_worker():
    asyncio.run(run_forwarding_test())

