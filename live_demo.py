import asyncio

from meshweaver.udp_client import UDPClientProtocol


def multiply(a, b):
    return a * b


async def main():
    loop = asyncio.get_running_loop()
    transport, protocol = await loop.create_datagram_endpoint(
        lambda: UDPClientProtocol(auto_submit_demo=False),
        remote_addr=("127.0.0.1", 9999),
    )

    try:
        print("Waiting for peer registration...")

        for _ in range(50):
            if protocol.hello_received:
                break
            await asyncio.sleep(0.1)

        if not protocol.hello_received:
            raise RuntimeError("Server registration timed out")

        print("Submitting multiply(10, 3)...")
        task_id = protocol.submit_task(multiply, args=(10, 3))
        task_info = protocol.pending_tasks[task_id]

        await asyncio.wait_for(task_info["event"].wait(), timeout=20)

        if task_id in protocol.task_errors:
            print("TASK FAILED:", protocol.task_errors[task_id])
        else:
            result = protocol.task_results[task_id]
            print("FINAL RESULT:", result)

            if result == 30:
                print("LIVE DEMO PASSED!")
            else:
                print("Unexpected result!")

    finally:
        protocol.stop_heartbeat()
        protocol.stop_task_retry()
        transport.close()


if __name__ == "__main__":
    asyncio.run(main())
