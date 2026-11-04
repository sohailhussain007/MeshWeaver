import unittest
from peer import Peer
from peer_registry import PeerRegistry


class TestPeerRegistry(unittest.TestCase):

    def setUp(self):
        self.registry = PeerRegistry()
        self.peer = Peer(
            peer_id="node_A",
            host="127.0.0.1",
            port=5000
        )

    def test_register_peer(self):
        self.assertTrue(self.registry.register_peer(self.peer))
        self.assertEqual(self.registry.get_peer("node_A"), self.peer)

    def test_duplicate_peer(self):
        self.registry.register_peer(self.peer)
        self.assertFalse(self.registry.register_peer(self.peer))

    def test_get_all_peers(self):
        self.registry.register_peer(self.peer)
        self.assertEqual(len(self.registry.get_all_peers()), 1)

    def test_unregister_peer(self):
        self.registry.register_peer(self.peer)
        self.assertTrue(self.registry.unregister_peer("node_A"))
        self.assertIsNone(self.registry.get_peer("node_A"))

    def test_register_hello_message(self):
        message = {
            "message_id": "msg_1",
            "sender_id": "node_B",
            "message_type": "HELLO",
            "payload": {
                "peer_id": "node_B",
                "host": "127.0.0.1",
                "port": 5001
            }
        }

        self.assertTrue(self.registry.register_hello_message(message))
        self.assertIsNotNone(self.registry.get_peer("node_B"))

    def test_invalid_hello_message(self):
        message = {
            "sender_id": "node_B",
            "message_type": "PING",
            "payload": {}
        }

        with self.assertRaises(ValueError):
            self.registry.register_hello_message(message)

    def test_hello_sender_mismatch(self):
        message = {
            "sender_id": "node_A",
            "message_type": "HELLO",
            "payload": {
                "peer_id": "node_B",
                "host": "127.0.0.1",
                "port": 5001
            }
        }

        with self.assertRaises(ValueError):
            self.registry.register_hello_message(message)


if __name__ == "__main__":
    unittest.main()
