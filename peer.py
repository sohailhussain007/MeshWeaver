import uuid
class Peer:
    """Represents a node in the MeshWeaver network."""
    def __init__(self, peer_id=None, host="127.0.0.1", port=0):
        self.peer_id = peer_id or str(uuid.uuid4())
        self.host = host
        self.port = port
    def to_dict(self):
        """Return peer information as a dictionary."""
        return {
            "peer_id": self.peer_id,
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