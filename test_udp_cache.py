import time

from meshweaver.udp_server import UDPServerProtocol


def test_task_cache_removes_expired_entries():
    server = UDPServerProtocol(task_cache_ttl=300)

    cache_key = (("127.0.0.1", 5000), "task-1")
    response = "cached-response"

    server.completed_tasks[cache_key] = response
    server.task_cache_timestamps[cache_key] = time.monotonic() - 301

    removed = server.cleanup_task_cache()

    assert removed == 1
    assert cache_key not in server.completed_tasks
    assert cache_key not in server.task_cache_timestamps


def test_task_cache_evicts_oldest_entry_when_full():
    server = UDPServerProtocol(max_completed_tasks=2)

    key1 = (("127.0.0.1", 5001), "task-1")
    key2 = (("127.0.0.1", 5002), "task-2")
    key3 = (("127.0.0.1", 5003), "task-3")

    server.cache_task_response(key1, "response-1")
    server.cache_task_response(key2, "response-2")
    server.cache_task_response(key3, "response-3")

    assert key1 not in server.completed_tasks
    assert key2 in server.completed_tasks
    assert key3 in server.completed_tasks
    assert len(server.completed_tasks) == 2
