import json
import unittest
from protocol import create_message, encode_message, decode_message
class TestProtocol(unittest.TestCase):
    def test_create_message(self):
        message = create_message(
            sender_id="node_A",
            message_type="HELLO",
            payload={"text": "Hello"},
        )
        self.assertEqual(message["sender_id"], "node_A")
        self.assertEqual(message["message_type"], "HELLO")
        self.assertEqual(message["payload"], {"text": "Hello"})
        self.assertIn("message_id", message)
    def test_encode_decode_message(self):
        message = create_message(
            sender_id="node_A",
            message_type="TASK",
            payload={"task": "example"},
        )
        encoded = encode_message(message)
        decoded = decode_message(encoded)
        self.assertEqual(decoded, message)
    def test_invalid_message(self):
        invalid_data = b'{"sender_id": "node_A"}'
        with self.assertRaises(ValueError):
            decode_message(invalid_data)
    def test_json_list_is_rejected(self):
        invalid_data = b'["node_A", "HELLO"]'
        with self.assertRaises(ValueError):
            decode_message(invalid_data)
    def test_missing_required_fields(self):
        invalid_data = b'{"sender_id": "node_A"}'
        with self.assertRaises(ValueError):
            decode_message(invalid_data)
    def test_invalid_json_is_rejected(self):
        invalid_data = b'{"sender_id":'
        with self.assertRaises(json.JSONDecodeError):
            decode_message(invalid_data)
    def test_non_json_text_is_rejected(self):
        invalid_data = b"Hello MeshWeaver"
        with self.assertRaises(json.JSONDecodeError):
            decode_message(invalid_data)
if __name__ == "__main__":
    unittest.main()