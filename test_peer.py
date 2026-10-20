import unittest
from peer import Peer
class TestPeer(unittest.TestCase):
    def test_peer_creation(self):
        peer = Peer(host="127.0.0.1", port=5000)
        self.assertTrue(peer.peer_id)
        self.assertEqual(peer.host, "127.0.0.1")
        self.assertEqual(peer.port, 5000)
    def test_peer_to_dict(self):
        peer = Peer(
            peer_id="node_A",
            host="127.0.0.1",
            port=5000,
        )
        info = peer.to_dict()
        self.assertEqual(info["peer_id"], "node_A")
        self.assertEqual(info["host"], "127.0.0.1")
        self.assertEqual(info["port"], 5000)
if __name__ == "__main__":
    unittest.main()