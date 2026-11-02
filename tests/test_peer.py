import unittest
from peer import Peer


class TestPeer(unittest.TestCase):

    def test_peer_creation(self):
        peer = Peer(host="127.0.0.1", port=5000)

        self.assertEqual(peer.host, "127.0.0.1")
        self.assertEqual(peer.port, 5000)
        self.assertIsNotNone(peer.peer_id)

    def test_peer_to_dict(self):
        peer = Peer(peer_id="node-01", host="127.0.0.1", port=5000)

        result = peer.to_dict()

        self.assertEqual(result["peer_id"], "node-01")
        self.assertEqual(result["host"], "127.0.0.1")
        self.assertEqual(result["port"], 5000)

    def test_invalid_port(self):
        with self.assertRaises(ValueError):
            Peer(port=70000)


if __name__ == "__main__":
    unittest.main()