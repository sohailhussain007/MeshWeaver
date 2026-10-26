
import base64
import json
import unittest

from serialization.task_serializer import (
    serialize_task,
    deserialize_task,
    execute_task,
)


def multiply(a, b):
    return a * b


class TestTaskTransport(unittest.TestCase):

    def test_task_survives_json_transport(self):
        # Serialize the task into bytes.
        serialized = serialize_task(multiply, (6, 7))

        # Encode the bytes as Base64 text.
        encoded = base64.b64encode(serialized).decode("ascii")

        # Create a JSON message like the team's protocol format.
        message = json.dumps({
            "type": "TASK",
            "data": {"task": encoded},
        })

        # Simulate receiving and parsing the JSON message.
        received_message = json.loads(message)
        received_encoded = received_message["data"]["task"]

        # Decode and deserialize the received task.
        received_bytes = base64.b64decode(received_encoded)
        task = deserialize_task(received_bytes)

        # Verify that the task executes correctly.
        self.assertEqual(execute_task(task), 42)


if __name__ == "__main__":
    unittest.main()
