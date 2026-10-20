import uuid
class Peer:
    """Represents a node in the MeshWeaver network."""
    def __init__(self, peer_id=None, host="127.0.0.1", port=0):
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