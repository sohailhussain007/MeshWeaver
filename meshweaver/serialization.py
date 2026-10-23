import base64
import cloudpickle


def serialize_task(function, args=(), kwargs=None):
    if kwargs is None:
        kwargs = {}

    task = {
        "function": function,
        "args": args,
        "kwargs": kwargs,
    }

    serialized_task = cloudpickle.dumps(task)
    encoded_task = base64.b64encode(serialized_task).decode("ascii")

    return encoded_task


def deserialize_task(encoded_task):
    serialized_task = base64.b64decode(encoded_task)
    task = cloudpickle.loads(serialized_task)

    return task
