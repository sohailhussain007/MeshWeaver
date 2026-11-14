import asyncio

from meshweaver.udp_client import (
    UDPClientProtocol,
    add,
    multiply,
    subtract,
)
from meshweaver.udp_server import UDPServerProtocol


def test_multiple_tasks_execute_and_return_separate_results():

    async def run_test():
        loop = asyncio.get_running_loop()

        # Start actual UDP server
        server_transport, server_protocol = (
            await loop.create_datagram_endpoint(
                lambda: UDPServerProtocol(),
                local_addr=("127.0.0.1", 0),
            )
        )

        server_port = server_transport.get_extra_info(
            "sockname"
        )[1]

        client_protocol = UDPClientProtocol()

        # Faster test retries
        client_protocol.task_timeout = 1
        client_protocol.max_task_retries = 2

        client_transport, _ = await loop.create_datagram_endpoint(
            lambda: client_protocol,
            remote_addr=("127.0.0.1", server_port),
        )

        try:
            # Wait for HELLO registration
            async def wait_for_registration():
                while not client_protocol.hello_received:
                    await asyncio.sleep(0.01)

            await asyncio.wait_for(
                wait_for_registration(),
                timeout=3,
            )

            # Submit three independent tasks
            task_ids = client_protocol.submit_tasks([
                {
                    "function": add,
                    "args": (10, 20),
                },
                {
                    "function": multiply,
                    "args": (5, 6),
                },
                {
                    "function": subtract,
                    "args": (50, 15),
                },
            ])

            # All task IDs must be unique
            assert len(task_ids) == 3
            assert len(set(task_ids)) == 3

            # Wait for all task results
            async def wait_for_results():
                while not all(
                    task_id in client_protocol.task_results
                    for task_id in task_ids
                ):
                    await asyncio.sleep(0.01)

            await asyncio.wait_for(
                wait_for_results(),
                timeout=5,
            )

            expected_results = [30, 30, 35]

            for task_id, expected in zip(
                task_ids,
                expected_results,
            ):
                assert (
                    client_protocol.task_results[task_id]
                    == expected
                )

            # Wait for retry coroutines to finish cleanup
            async def wait_for_cleanup():
                while any(
                    task_id in client_protocol.pending_tasks
                    or task_id in client_protocol.task_retry_tasks
                    for task_id in task_ids
                ):
                    await asyncio.sleep(0.01)

            await asyncio.wait_for(
                wait_for_cleanup(),
                timeout=3,
            )

            assert all(
                task_id not in client_protocol.pending_tasks
                for task_id in task_ids
            )

            print("All three tasks completed successfully!")

        finally:
            client_transport.close()
            server_transport.close()

            await asyncio.sleep(0)

    asyncio.run(run_test())
