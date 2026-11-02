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


if __name__ == "__main__":
    unittest.main()