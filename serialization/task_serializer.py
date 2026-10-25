"""Serialize and deserialize Python tasks for MeshWeaver."""

import cloudpickle
import pickle

def serialize_task(func, args=(), kwargs=None):
    """Serialize a function and its arguments into bytes."""
    if not callable(func):
        raise TypeError("func must be callable")

    if not isinstance(args, (tuple, list)):
        raise TypeError("args must be a tuple or list")

    if kwargs is None:
        kwargs = {}

    if not isinstance(kwargs, dict):
        raise TypeError("kwargs must be a dictionary")

    task = {
        "function": func,
        "args": tuple(args),
        "kwargs": kwargs,
    }

    return cloudpickle.dumps(task)


def deserialize_task(data):
    """Deserialize task bytes into a task dictionary."""
    if not isinstance(data, bytes):
        raise TypeError("data must be bytes")
    try:
        task = cloudpickle.loads(data)
    except (
        pickle.UnpicklingError,
        EOFError,
        ValueError,
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


def execute_task(task):
    """Execute a deserialized task and return its result."""
    if not isinstance(task, dict):
        raise TypeError("task must be a dictionary")

    func = task.get("function")
    args = task.get("args")
    kwargs = task.get("kwargs")

    if not callable(func):
        raise ValueError("Invalid task: function is missing")

    if not isinstance(args, tuple) or not isinstance(kwargs, dict):
        raise ValueError("Invalid task arguments")

    return func(*args, **kwargs)
