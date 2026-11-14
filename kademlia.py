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
