
import json
import unittest

from meshweaver.serialization import serialize_task, deserialize_task


def add(a, b):
    return a + b


def calculate_total(price, quantity, discount=0):
    return (price * quantity) - discount


class TestSerializationCompatibility(unittest.TestCase):

    def test_task_can_be_encoded_for_json_and_executed(self):
        encoded_task = serialize_task(add, args=(10, 20))

        message = json.dumps({
            "type": "EXECUTE_TASK",
            "data": encoded_task,
        })

        received_message = json.loads(message)
        task = deserialize_task(received_message["data"])
        result = task["function"](*task["args"], **task["kwargs"])

        self.assertEqual(received_message["type"], "EXECUTE_TASK")
        self.assertEqual(result, 30)

    def test_mixed_arguments_survive_json_transport(self):
        encoded_task = serialize_task(
            calculate_total,
            args=(100, 3),
            kwargs={"discount": 50},
        )

        message = json.dumps({
            "type": "EXECUTE_TASK",
            "data": encoded_task,
        })

        received_message = json.loads(message)
        task = deserialize_task(received_message["data"])

        self.assertEqual(task["args"], (100, 3))
        self.assertEqual(task["kwargs"], {"discount": 50})
        self.assertEqual(
            task["function"](*task["args"], **task["kwargs"]),
            250,
        )


if __name__ == "__main__":
    unittest.main()
