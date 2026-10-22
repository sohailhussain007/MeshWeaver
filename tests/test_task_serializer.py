
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


def count_items(items):
    return len(items)


def get_user_name(user):
    return user["name"]


def get_status():
    return "ready"

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

    def test_round_trip_preserves_function(self):
        data = serialize_task(add, (10, 20))
        task = deserialize_task(data)

        self.assertEqual(task["function"](task["args"][0], task["args"][1]), 30)

    def test_round_trip_preserves_keyword_arguments(self):
        data = serialize_task(greet, ("Rannjana",), {"greeting": "Welcome"})
        task = deserialize_task(data)

        self.assertEqual(task["kwargs"]["greeting"], "Welcome")
        self.assertEqual(execute_task(task), "Welcome, Rannjana!")

    def test_execute_task_propagates_function_error(self):
        def failing_task():
            raise ValueError("Task failed")

        data = serialize_task(failing_task)
        task = deserialize_task(data)

        with self.assertRaisesRegex(ValueError, "Task failed"):
            execute_task(task)

    def test_execute_task_propagates_type_error(self):
        def invalid_operation():
            return 1 + "text"

        data = serialize_task(invalid_operation)
        task = deserialize_task(data)

        with self.assertRaises(TypeError):
            execute_task(task)


    def test_list_argument(self):
        data = serialize_task(count_items, ([10, 20, 30],))
        task = deserialize_task(data)

        self.assertEqual(task["args"], ([10, 20, 30],))
        self.assertEqual(execute_task(task), 3)

    def test_dictionary_argument(self):
        user = {"name": "Rannjana", "role": "developer"}
        data = serialize_task(get_user_name, (user,))
        task = deserialize_task(data)

        self.assertEqual(task["args"][0], user)
        self.assertEqual(execute_task(task), "Rannjana")

    def test_task_without_arguments(self):
        data = serialize_task(get_status)
        task = deserialize_task(data)

        self.assertEqual(task["args"], ())
        self.assertEqual(execute_task(task), "ready")

if __name__ == "__main__":
    unittest.main()
