import pytest

from meshweaver.serialization import serialize_task, deserialize_task


def add(a, b):
    return a + b


def test_serialize_rejects_non_callable():
    with pytest.raises(TypeError):
        serialize_task(123)


def test_serialize_rejects_invalid_args():
    with pytest.raises(TypeError):
        serialize_task(add, args="invalid")


def test_serialize_rejects_invalid_kwargs():
    with pytest.raises(TypeError):
        serialize_task(add, args=(10, 20), kwargs=[])


def test_deserialize_rejects_non_string():
    with pytest.raises(TypeError):
        deserialize_task(123)


def test_deserialize_rejects_invalid_base64():
    with pytest.raises(ValueError):
        deserialize_task("!!!")


def test_valid_task_serialization():
    encoded = serialize_task(add, args=(10, 20))
    task = deserialize_task(encoded)

    assert task["function"](*task["args"], **task["kwargs"]) == 30
