import unittest
from peer import Peer
from peer_registry import PeerRegistry
from protocol import create_message, encode_message, decode_message
class TestPeerRegistry(unittest.TestCase):
    def setUp(self):
        self.registry = PeerRegistry()
        self.peer = Peer(
            peer_id="node_A",
            host="127.0.0.1",
            port=5000
        )
    def create_hello_message(self):
        return create_message(
            sender_id="node_B",
            message_type="HELLO",
            payload={
                "peer_id": "node_B",
                "host": "192.168.1.10",
                "port": 5001
            }
        )
    def test_register_peer(self):
        result = self.registry.register_peer(self.peer)
        self.assertTrue(result)
        self.assertEqual(
            self.registry.get_peer("node_A"),
            self.peer
        )
    def test_duplicate_peer_registration(self):
        self.registry.register_peer(self.peer)
        result = self.registry.register_peer(self.peer)
        self.assertFalse(result)
    def test_get_all_peers(self):
        self.registry.register_peer(self.peer)
        peers = self.registry.get_all_peers()
        self.assertEqual(len(peers), 1)
    def test_unregister_peer(self):
        self.registry.register_peer(self.peer)
        result = self.registry.unregister_peer("node_A")
        self.assertTrue(result)
        self.assertIsNone(self.registry.get_peer("node_A"))
    def test_unregister_unknown_peer(self):
        result = self.registry.unregister_peer("unknown")
        self.assertFalse(result)
    def test_register_hello_message(self):
        message = self.create_hello_message()
        decoded = decode_message(encode_message(message))
        result = self.registry.register_hello_message(decoded)
        self.assertTrue(result)
        peer = self.registry.get_peer("node_B")
        self.assertIsNotNone(peer)
        self.assertEqual(peer.host, "192.168.1.10")
        self.assertEqual(peer.port, 5001)
    def test_register_duplicate_hello_message(self):
        message = self.create_hello_message()
        decoded = decode_message(encode_message(message))
        self.assertTrue(
            self.registry.register_hello_message(decoded)
        )
        self.assertFalse(
            self.registry.register_hello_message(decoded)
        )
    def test_reject_non_hello_message(self):
        message = create_message(
            sender_id="node_B",
            message_type="TASK",
            payload={"task": "example"}
        )
        with self.assertRaises(ValueError):
            self.registry.register_hello_message(message)
    def test_reject_non_dictionary_message(self):
        with self.assertRaises(ValueError):
            self.registry.register_hello_message("invalid")
    def test_reject_missing_payload(self):
        message = {
            "message_type": "HELLO",
            "sender_id": "node_B"
        }
        with self.assertRaises(ValueError):
            self.registry.register_hello_message(message)
    def test_reject_invalid_peer_id(self):
        message = self.create_hello_message()
        message["payload"]["peer_id"] = ""
        with self.assertRaises(ValueError):
            self.registry.register_hello_message(message)
    def test_reject_sender_id_mismatch(self):
        message = self.create_hello_message()
        message["sender_id"] = "node_C"
        with self.assertRaises(ValueError):
            self.registry.register_hello_message(message)
    def test_reject_invalid_host(self):
        message = self.create_hello_message()
        message["payload"]["host"] = ""
        with self.assertRaises(ValueError):
            self.registry.register_hello_message(message)
    def test_reject_invalid_port(self):
        invalid_ports = [0, -1, 65536, "5001", True]
        for port in invalid_ports:
            with self.subTest(port=port):
                message = self.create_hello_message()
                message["payload"]["port"] = port
                with self.assertRaises(ValueError):
                    self.registry.register_hello_message(message)

if __name__ == "__main__":
    unittest.main()
