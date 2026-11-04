import unittest
from peer import Peer


class TestPeer(unittest.TestCase):

    def test_peer_creation(self):
        peer = Peer(host="127.0.0.1", port=5000)
        self.assertTrue(peer.peer_id)
        self.assertEqual(peer.host, "127.0.0.1")
        self.assertEqual(peer.port, 5000)

    def test_peer_to_dict(self):
        peer = Peer(peer_id="node_A", host="127.0.0.1", port=5000)
        self.assertEqual(peer.to_dict(), {
            "peer_id": "node_A",
            "host": "127.0.0.1",
            "port": 5000,
        })

    def test_invalid_ports(self):
        for port in [-1, 65536, "5000", True]:
            with self.subTest(port=port):
                with self.assertRaises(ValueError):
                    Peer(port=port)

    def test_invalid_peer_id(self):
        for peer_id in ["", 123]:
            with self.subTest(peer_id=peer_id):
                with self.assertRaises(ValueError):
                    Peer(peer_id=peer_id)

    def test_empty_host(self):
        with self.assertRaises(ValueError):
            Peer(host="")


if __name__ == "__main__":
    unittest.main()
