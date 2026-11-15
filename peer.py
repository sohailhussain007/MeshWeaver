import uuid
from kademlia import generate_node_id
class Peer:
    """Represents a node in the MeshWeaver network."""
    def __init__(self, peer_id=None, host="127.0.0.1", port=0, node_id=None):
        if peer_id is not None:
            if not isinstance(peer_id, str) or not peer_id.strip():
                raise ValueError("peer_id must be a non-empty string")
        if not isinstance(host, str) or not host.strip():
            raise ValueError("host must be a non-empty string")
        if not isinstance(port, int) or isinstance(port, bool):
            raise ValueError("Port must be an integer")
        if not 0 <= port <= 65535:
            raise ValueError("Port must be between 0 and 65535")
        self.peer_id = peer_id if peer_id is not None else str(uuid.uuid4())
        self.host = host
        self.port = port
        self.node_id = node_id if node_id is not None else generate_node_id()
        if not isinstance(self.node_id, str) or len(self.node_id) != 40:
            raise ValueError("Node ID must contain 40 hexadecimal characters")
        try:
            int(self.node_id, 16)
        except ValueError:
            raise ValueError("Node ID must contain hexadecimal characters")
        self.node_id = self.node_id.lower()
    def to_dict(self):
        """Return peer information as a dictionary."""
        return {
            "peer_id": self.peer_id,
            "node_id": self.node_id,
            "host": self.host,
            "port": self.port,
        }
    def __repr__(self):
        return (
            f"Peer(peer_id={self.peer_id!r}, "
            f"host={self.host!r}, port={self.port})"
        )
if __name__ == "__main__":
    peer = Peer(host="127.0.0.1", port=5000)
    print("Peer created successfully!")
    print(peer)
    print("Peer information:", peer.to_dict())