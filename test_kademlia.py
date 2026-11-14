import unittest
from kademlia import generate_node_id, calculate_xor_distance
class TestKademlia(unittest.TestCase):
    def test_generate_node_id(self):
        node_id = generate_node_id()
        self.assertEqual(len(node_id), 40)
        self.assertTrue(all(c in "0123456789abcdef" for c in node_id))
    def test_xor_distance(self):
        id1 = "0000000000000000000000000000000000000001"
        id2 = "0000000000000000000000000000000000000011"
        self.assertEqual(calculate_xor_distance(id1, id2), 16)
    def test_same_node_distance(self):
        node_id = generate_node_id()
        self.assertEqual(calculate_xor_distance(node_id, node_id), 0)
    def test_xor_distance_symmetry(self):
        id1 = generate_node_id()
        id2 = generate_node_id()
        self.assertEqual(
            calculate_xor_distance(id1, id2),
            calculate_xor_distance(id2, id1)
        )
    def test_invalid_node_id(self):
        with self.assertRaises(ValueError):
            calculate_xor_distance("invalid", "0000000000000000000000000000000000000001")
if __name__ == "__main__":
    unittest.main()
