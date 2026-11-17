import asyncio
import time

from meshweaver.protocol import create_message, parse_message
from meshweaver.udp_server import UDPServerProtocol


def test_server_executes_tasks_concurrently():

    async def run_test():
        loop = asyncio.get_running_loop()

        server_protocol = UDPServerProtocol()

        server_transport, _ = await loop.create_datagram_endpoint(
            lambda: server_protocol,
            local_addr=("127.0.0.1", 0),
        )

        server_port = server_transport.get_extra_info("sockname")[1]

        responses = {}

        class TestClient(asyncio.DatagramProtocol):
            def datagram_received(self, data, addr):
                response = parse_message(data.decode("utf-8"))
                task_id = response["data"].get("task_id")
                responses[task_id] = response

        client_transport, _ = await loop.create_datagram_endpoint(
            TestClient,
            remote_addr=("127.0.0.1", server_port),
        )

        def slow_task():
            time.sleep(2)
            return "slow"

        def fast_task():
            time.sleep(0.1)
            return "fast"

        try:
            slow_message = create_message(
                "TASK",
                {
                    "task_id": "slow-task",
                    "task": "slow",
                },
            )

            fast_message = create_message(
                "TASK",
                {
                    "task_id": "fast-task",
                    "task": "fast",
                },
            )

            import meshweaver.udp_server as server_module

            original_deserialize = server_module.deserialize_task

            def fake_deserialize(task_data):
                if task_data == "slow":
                    return {
                        "function": slow_task,
                        "args": (),
                        "kwargs": {},
                    }

                if task_data == "fast":
                    return {
                        "function": fast_task,
                        "args": (),
                        "kwargs": {},
                    }

                return original_deserialize(task_data)

            server_module.deserialize_task = fake_deserialize

            client_transport.sendto(
                slow_message.encode("utf-8")
            )

            client_transport.sendto(
                fast_message.encode("utf-8")
            )

            await asyncio.wait_for(
                wait_for_responses(responses),
                timeout=5,
            )

            assert responses["fast-task"]["data"]["result"] == "fast"
            assert responses["slow-task"]["data"]["result"] == "slow"

            assert list(responses.keys())[0] == "fast-task"

        finally:
            server_module.deserialize_task = original_deserialize
            client_transport.close()
            server_transport.close()
            await asyncio.sleep(0)

    async def wait_for_responses(responses):
        while len(responses) < 2:
            await asyncio.sleep(0.01)

    asyncio.run(run_test())
