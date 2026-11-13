import asyncio

import meshweaver.udp_server as udp_server_module

from meshweaver.udp_client import UDPClientProtocol
from meshweaver.udp_server import UDPServerProtocol
from meshweaver.protocol import create_message
from meshweaver.serialization import serialize_task


def add(a, b):
    return a + b


class DropFirstResponseTransport:

    def __init__(self, transport):
        self.transport = transport
        self.dropped = False
        self.send_count = 0

    def sendto(self, data, addr):
        self.send_count += 1

        if not self.dropped:
            self.dropped = True
            print("Integration test: First server response dropped.")
            return

        self.transport.sendto(data, addr)


def test_client_server_retry_uses_cached_response(monkeypatch):

    async def run_test():
        loop = asyncio.get_running_loop()

        execution_count = 0
        original_deserialize = udp_server_module.deserialize_task

        def tracked_deserialize(task_data):
            nonlocal execution_count
            execution_count += 1
            return original_deserialize(task_data)

        monkeypatch.setattr(
            udp_server_module,
            "deserialize_task",
            tracked_deserialize,
        )

        server_protocol = UDPServerProtocol()

        server_transport, _ = await loop.create_datagram_endpoint(
            lambda: server_protocol,
            local_addr=("127.0.0.1", 0),
        )

        server_protocol.transport = DropFirstResponseTransport(
            server_transport
        )

        port = server_transport.get_extra_info("sockname")[1]

        client_protocol = UDPClientProtocol()

        client_transport, _ = await loop.create_datagram_endpoint(
            lambda: client_protocol,
            remote_addr=("127.0.0.1", port),
        )

        try:
            client_protocol.task_timeout = 0.1
            client_protocol.max_task_retries = 1
            client_protocol.task_retry_count = 0

            client_protocol.task_id = "integration-task-001"

            task_data = serialize_task(
                add,
                args=(10, 20),
            )

            client_protocol.task_message = create_message(
                "TASK",
                {
                    "task_id": client_protocol.task_id,
                    "task": task_data,
                },
            )

            await asyncio.wait_for(
                client_protocol.send_task_with_retry(),
                timeout=3,
            )

            assert client_protocol.reply_received.is_set()
            assert execution_count == 1

            assert server_protocol.transport.dropped is True
            assert server_protocol.transport.send_count == 2

            assert len(server_protocol.completed_tasks) == 1

        finally:
            client_protocol.stop_task_retry()
            client_transport.close()
            server_transport.close()

    asyncio.run(run_test())
