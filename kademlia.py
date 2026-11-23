import secrets
def generate_node_id():
    """Generate a random 160-bit Kademlia node ID."""
    return secrets.token_hex(20)
def calculate_xor_distance(node_id1, node_id2):
    """Calculate XOR distance between two Kademlia node IDs."""
    if not isinstance(node_id1, str) or not isinstance(node_id2, str):
        raise ValueError("Node IDs must be strings")
    if len(node_id1) != 40 or len(node_id2) != 40:
        raise ValueError("Node IDs must contain 40 hexadecimal characters")
    try:
        id1 = int(node_id1, 16)
        id2 = int(node_id2, 16)
    except ValueError:
        raise ValueError("Node IDs must contain only hexadecimal characters")
    return id1 ^ id2
class RoutingTable:
    """Manage Kademlia nodes using 160 K-buckets."""
    def __init__(self, local_node_id, bucket_size=20):
        if not isinstance(local_node_id, str):
            raise ValueError("Local node ID must be a string")
        if len(local_node_id) != 40:
            raise ValueError("Node ID must contain 40 hexadecimal characters")
        try:
            int(local_node_id, 16)
        except ValueError:
            raise ValueError("Node ID must contain hexadecimal characters")
        if not isinstance(bucket_size, int) or isinstance(bucket_size, bool) or bucket_size <= 0:
            raise ValueError("Bucket size must be a positive integer")
        self.local_node_id = local_node_id.lower()
        self.bucket_size = bucket_size
        self.buckets = [[] for _ in range(160)]
    def add_node(self, node_id):
        """Add a node to the appropriate K-bucket."""
        if not isinstance(node_id, str) or len(node_id) != 40:
            raise ValueError("Node ID must contain 40 hexadecimal characters")
        try:
            int(node_id, 16)
        except ValueError:
            raise ValueError("Node ID must contain hexadecimal characters")
        node_id = node_id.lower()
        distance = calculate_xor_distance(self.local_node_id, node_id)
        if distance == 0:
            return False
        bucket_index = distance.bit_length() - 1
        bucket = self.buckets[bucket_index]
        if node_id in bucket:
            return False
        if len(bucket) >= self.bucket_size:
            return False
        bucket.append(node_id)
        return True
    def remove_node(self, node_id):
        """Remove a node from the routing table."""
        for bucket in self.buckets:
            if node_id in bucket:
                bucket.remove(node_id)
                return True
        return False
    def find_closest_nodes(self, target_node_id, count=20):
        """Return the closest nodes to a target ID."""
        if not isinstance(target_node_id, str) or len(target_node_id) != 40:
            raise ValueError("Target node ID must contain 40 hexadecimal characters")
        try:
            int(target_node_id, 16)
        except ValueError:
            raise ValueError("Target node ID must contain hexadecimal characters")
        if not isinstance(count, int) or isinstance(count, bool) or count <= 0:
            raise ValueError("Count must be a positive integer")
        all_nodes = [
            node_id
            for bucket in self.buckets
            for node_id in bucket
        ]
        return sorted(
            all_nodes,
            key=lambda node_id: calculate_xor_distance(node_id, target_node_id)
        )[:count]
