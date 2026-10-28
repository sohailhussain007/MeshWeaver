

import base64
import json
import unittest

from serialization.task_serializer import (
    serialize_task,
    deserialize_task,
    execute_task,
)


def add(a, b):
    return a + b


def calculate_total(price, quantity, discount=0):
    return (price * quantity) - discount


class TestSerializationCompatibility(unittest.TestCase):

    def test_task_can_be_encoded_for_json_and_executed(self):
        serialized = serialize_task(add, (10, 20))
        encoded = base64.b64encode(serialized).decode("ascii")

        message = json.dumps({
            "type": "TASK",
            "data": {"task": encoded},
        })

        received_message = json.loads(message)
        received_encoded = received_message["data"]["task"]
        received_bytes = base64.b64decode(received_encoded)
        task = deserialize_task(received_bytes)

        self.assertEqual(received_message["type"], "TASK")
        self.assertEqual(execute_task(task), 30)

    def test_mixed_arguments_survive_json_transport(self):
        serialized = serialize_task(
            calculate_total,
            args=(100, 3),
            kwargs={"discount": 50},
        )
        encoded = base64.b64encode(serialized).decode("ascii")

        message = json.dumps({
            "type": "TASK",
            "data": {"task": encoded},
        })

        received_message = json.loads(message)
        received_encoded = received_message["data"]["task"]
        received_bytes = base64.b64decode(received_encoded)
        task = deserialize_task(received_bytes)

        self.assertEqual(task["args"], (100, 3))
        self.assertEqual(task["kwargs"], {"discount": 50})
        self.assertEqual(execute_task(task), 250)


if __name__ == "__main__":
    unittest.main()
