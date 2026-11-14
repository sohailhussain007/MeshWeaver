import unittest
from kademlia import (
    generate_node_id,
    calculate_xor_distance,
    RoutingTable
)
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
            calculate_xor_distance(
                "invalid",
                "0000000000000000000000000000000000000001"
            )
    # Routing Table Tests
    def test_routing_table_creation(self):
        routing_table = RoutingTable("0" * 40)
        self.assertEqual(len(routing_table.buckets), 160)
    def test_add_node(self):
        routing_table = RoutingTable("0" * 40)
        node_id = "0" * 39 + "1"
        self.assertTrue(routing_table.add_node(node_id))
    def test_duplicate_node(self):
        routing_table = RoutingTable("0" * 40)
        node_id = "0" * 39 + "1"
        routing_table.add_node(node_id)
        self.assertFalse(routing_table.add_node(node_id))
    def test_reject_local_node(self):
        local_id = "0" * 40
        routing_table = RoutingTable(local_id)
        self.assertFalse(routing_table.add_node(local_id))
    def test_remove_node(self):
        routing_table = RoutingTable("0" * 40)
        node_id = "0" * 39 + "1"
        routing_table.add_node(node_id)
        self.assertTrue(routing_table.remove_node(node_id))
        self.assertFalse(routing_table.remove_node(node_id))
    def test_find_closest_nodes(self):
        routing_table = RoutingTable("0" * 40)
        node1 = "0" * 39 + "1"
        node2 = "0" * 39 + "2"
        node3 = "0" * 39 + "3"
        routing_table.add_node(node1)
        routing_table.add_node(node2)
        routing_table.add_node(node3)
        closest = routing_table.find_closest_nodes(node1)
        self.assertEqual(closest[0], node1)
    def test_bucket_capacity(self):
        routing_table = RoutingTable("0" * 40, bucket_size=1)
        node1 = "0" * 39 + "2"
        node2 = "0" * 39 + "3"
        self.assertTrue(routing_table.add_node(node1))
        self.assertFalse(routing_table.add_node(node2))
if __name__ == "__main__":
    unittest.main()
