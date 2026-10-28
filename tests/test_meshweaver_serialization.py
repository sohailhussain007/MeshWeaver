
import unittest

from meshweaver.serialization import serialize_task, deserialize_task


def add(a, b):
    return a + b


class TestMeshWeaverSerialization(unittest.TestCase):

    def test_serialization_returns_base64_string(self):
        encoded = serialize_task(add, (10, 20))
        self.assertIsInstance(encoded, str)

    def test_round_trip_restores_task(self):
        encoded = serialize_task(add, (10, 20))
        task = deserialize_task(encoded)

        self.assertEqual(task["args"], (10, 20))
        self.assertEqual(task["kwargs"], {})
        self.assertEqual(task["function"](*task["args"]), 30)

    def test_keyword_arguments(self):
        encoded = serialize_task(add, kwargs={"a": 10, "b": 20})
        task = deserialize_task(encoded)

        self.assertEqual(task["function"](**task["kwargs"]), 30)

    def test_non_callable_function_is_rejected(self):
        with self.assertRaises(TypeError):
            serialize_task("not a function")

    def test_non_string_payload_is_rejected(self):
        with self.assertRaises(TypeError):
            deserialize_task(b"not a string")

    def test_invalid_base64_is_rejected(self):
        with self.assertRaises(ValueError):
            deserialize_task("not valid base64!")

if __name__ == "__main__":
    unittest.main()
