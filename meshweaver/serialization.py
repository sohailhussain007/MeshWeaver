
"""Serialize and deserialize MeshWeaver tasks using cloudpickle and Base64."""

import base64
import binascii

import cloudpickle


def serialize_task(function, args=(), kwargs=None):
    """Serialize a function and its arguments as a Base64 string."""
    if not callable(function):
        raise TypeError("function must be callable")

    if not isinstance(args, (tuple, list)):
        raise TypeError("args must be a tuple or list")

    if kwargs is None:
        kwargs = {}

    if not isinstance(kwargs, dict):
        raise TypeError("kwargs must be a dictionary")

    task = {
        "function": function,
        "args": tuple(args),
        "kwargs": kwargs,
    }

    serialized = cloudpickle.dumps(task)
    return base64.b64encode(serialized).decode("ascii")


def deserialize_task(encoded_task):
    """Restore a task from its Base64-encoded string."""
    if not isinstance(encoded_task, str):
        raise TypeError("encoded_task must be a Base64 string")

    try:
        serialized = base64.b64decode(encoded_task, validate=True)
        task = cloudpickle.loads(serialized)
    except (
        binascii.Error,
        ValueError,
        EOFError,
        TypeError,
        AttributeError,
        ImportError,
        IndexError,
    ) as error:
        raise ValueError("Invalid serialized task data") from error

    if not isinstance(task, dict):
        raise ValueError("Invalid task: expected a dictionary")

    if not callable(task.get("function")):
        raise ValueError("Invalid task: function is missing")

    if not isinstance(task.get("args"), tuple):
        raise ValueError("Invalid task: args must be a tuple")

    if not isinstance(task.get("kwargs"), dict):
        raise ValueError("Invalid task: kwargs must be a dictionary")

    return task