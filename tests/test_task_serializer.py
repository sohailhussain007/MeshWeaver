
import unittest

from serialization.task_serializer import (
    serialize_task,
    deserialize_task,
    execute_task,
)


def add(a, b):
    return a + b


def greet(name, greeting="Hello"):
    return f"{greeting}, {name}!"


class TestTaskSerializer(unittest.TestCase):

    def test_serialize_returns_bytes(self):
        data = serialize_task(add, (2, 3))
        self.assertIsInstance(data, bytes)

    def test_deserialize_preserves_arguments(self):
        data = serialize_task(add, (4, 5))
        task = deserialize_task(data)

        self.assertEqual(task["args"], (4, 5))
        self.assertEqual(task["kwargs"], {})

    def test_execute_task_returns_result(self):
        data = serialize_task(add, (2, 3))
        task = deserialize_task(data)

        self.assertEqual(execute_task(task), 5)

    def test_keyword_arguments(self):
        data = serialize_task(greet, ("Rannjana",), {"greeting": "Hi"})
        task = deserialize_task(data)

        self.assertEqual(execute_task(task), "Hi, Rannjana!")

    def test_invalid_function_is_rejected(self):
        with self.assertRaises(TypeError):
            serialize_task("not a function", (1, 2))

    def test_invalid_data_is_rejected(self):
        with self.assertRaises(TypeError):
            deserialize_task("not bytes")


if __name__ == "__main__":
    unittest.main()
