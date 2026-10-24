
import base64
import unittest

from serialization.task_serializer import (
    serialize_task,
    deserialize_task,
    execute_task,
)


def add(a, b):
    return a + b


class TestSerializationCompatibility(unittest.TestCase):

    def test_task_can_be_encoded_for_json_and_executed(self):
        # Serialize the task using our module.
        serialized = serialize_task(add, (10, 20))

        # Convert bytes to Base64 text for JSON transport.
        encoded = base64.b64encode(serialized).decode("ascii")

        # Simulate the receiver decoding the transported text.
        received_bytes = base64.b64decode(encoded.encode("ascii"))
        task = deserialize_task(received_bytes)

        # Execute the recovered task.
        self.assertEqual(execute_task(task), 30)


if __name__ == "__main__":
    unittest.main()
