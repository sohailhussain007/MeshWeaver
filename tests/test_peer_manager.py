
import unittest
from peer import Peer
from peer_manager import PeerManager


class TestPeerManager(unittest.TestCase):

    def setUp(self):
        self.manager = PeerManager()
        self.peer1 = Peer(peer_id="peer-101", host="127.0.0.1", port=5000)
        self.peer2 = Peer(peer_id="peer-102", host="127.0.0.1", port=5001)

    def test_add_peer(self):
        self.assertTrue(self.manager.add_peer(self.peer1))
        self.assertEqual(len(self.manager.get_all_peers()), 1)

    def test_add_invalid_peer(self):
        with self.assertRaises(TypeError):
            self.manager.add_peer("invalid-peer")

    def test_duplicate_peer(self):
        self.manager.add_peer(self.peer1)
        with self.assertRaises(ValueError):
            self.manager.add_peer(self.peer1)

    def test_get_peer(self):
        self.manager.add_peer(self.peer1)
        result = self.manager.get_peer("peer-101")
        self.assertEqual(result, self.peer1)

    def test_get_missing_peer(self):
        self.assertIsNone(self.manager.get_peer("unknown"))

    def test_has_peer(self):
        self.manager.add_peer(self.peer1)
        self.assertTrue(self.manager.has_peer("peer-101"))
        self.assertFalse(self.manager.has_peer("unknown"))

    def test_remove_peer(self):
        self.manager.add_peer(self.peer1)
        self.assertTrue(self.manager.remove_peer("peer-101"))
        self.assertEqual(len(self.manager.get_all_peers()), 0)

    def test_remove_missing_peer(self):
        self.assertFalse(self.manager.remove_peer("unknown"))

    def test_get_all_peers(self):
        self.manager.add_peer(self.peer1)
        self.manager.add_peer(self.peer2)
        result = self.manager.get_all_peers()
        self.assertEqual(len(result), 2)

    def test_get_peer_count(self):
        self.assertEqual(self.manager.get_peer_count(), 0)
        self.manager.add_peer(self.peer1)
        self.manager.add_peer(self.peer2)
        self.assertEqual(self.manager.get_peer_count(), 2)

    def test_clear_all_peers(self):
        self.manager.add_peer(self.peer1)
        self.manager.add_peer(self.peer2)

        removed_count = self.manager.clear_all_peers()

        self.assertEqual(removed_count, 2)
        self.assertEqual(self.manager.get_peer_count(), 0)

    def test_clear_empty_peer_manager(self):
        removed_count = self.manager.clear_all_peers()

        self.assertEqual(removed_count, 0)
        self.assertEqual(self.manager.get_peer_count(), 0)

    def test_search_peers_by_host(self):
        self.manager.add_peer(self.peer1)
        self.manager.add_peer(self.peer2)

        results = self.manager.search_peers(host="127.0.0.1")

        self.assertEqual(len(results), 2)

    def test_search_peers_by_port(self):
        self.manager.add_peer(self.peer1)
        self.manager.add_peer(self.peer2)

        results = self.manager.search_peers(port=5000)

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0], self.peer1)

    def test_search_peers_no_match(self):
        self.manager.add_peer(self.peer1)

        results = self.manager.search_peers(host="192.168.1.1")

        self.assertEqual(results, [])


if __name__ == "__main__":
    unittest.main()
