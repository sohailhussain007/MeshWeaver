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
                "port": 5001,
                "node_id": "a" * 40,
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
    def test_register_multiple_peers(self):
        peer2 = Peer(
            peer_id="node_B",
            host="127.0.0.1",
            port=5001
        )
        result1 = self.registry.register_peer(self.peer)
        result2 = self.registry.register_peer(peer2)
        self.assertTrue(result1)
        self.assertTrue(result2)
        self.assertEqual(len(self.registry.get_all_peers()), 2)
        self.assertEqual(
            self.registry.get_peer("node_A"),
            self.peer
        )
        self.assertEqual(
            self.registry.get_peer("node_B"),
            peer2
        )
    def test_get_all_peers(self):
        peer2 = Peer(
            peer_id="node_B",
            host="127.0.0.1",
            port=5001
        )
        self.registry.register_peer(self.peer)
        self.registry.register_peer(peer2)
        peers = self.registry.get_all_peers()
        self.assertEqual(len(peers), 2)
        self.assertIn(self.peer, peers)
        self.assertIn(peer2, peers)
    def test_unregister_peer(self):
        self.registry.register_peer(self.peer)
        result = self.registry.unregister_peer("node_A")
        self.assertTrue(result)
        self.assertIsNone(
            self.registry.get_peer("node_A")
        )
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
    def test_update_cpu_load(self):
        self.registry.register_peer(self.peer)
        result = self.registry.update_cpu_load(
            "node_A",
            35.5
        )
        self.assertTrue(result)
        self.assertEqual(
            self.registry.cpu_loads["node_A"],
            35.5
        )
    def test_reject_invalid_cpu_load(self):
        self.registry.register_peer(self.peer)
        invalid_loads = [-1, 101, "50", None, True]
        for cpu_load in invalid_loads:
            with self.subTest(cpu_load=cpu_load):
                with self.assertRaises(ValueError):
                    self.registry.update_cpu_load(
                        "node_A",
                        cpu_load
                    )
    def test_update_unknown_peer_cpu_load(self):
        result = self.registry.update_cpu_load(
            "unknown",
            30
        )
        self.assertFalse(result)
    def test_select_lowest_cpu_peer(self):
        peer2 = Peer(
            peer_id="node_B",
            host="127.0.0.1",
            port=5001
        )
        peer3 = Peer(
            peer_id="node_C",
            host="127.0.0.1",
            port=5002
        )
        self.registry.register_peer(self.peer)
        self.registry.register_peer(peer2)
        self.registry.register_peer(peer3)
        self.registry.update_cpu_load("node_A", 75)
        self.registry.update_cpu_load("node_B", 25)
        self.registry.update_cpu_load("node_C", 50)
        selected_peer = self.registry.select_lowest_cpu_peer()
        self.assertEqual(selected_peer, peer2)
    def test_select_peer_without_cpu_data(self):
        result = self.registry.select_lowest_cpu_peer()
        self.assertIsNone(result)
if __name__ == "__main__":
    unittest.main()
