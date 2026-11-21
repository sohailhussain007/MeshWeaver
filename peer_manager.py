
class PeerManager:
    """Manages peers in the MeshWeaver network."""

    def __init__(self):
        self.peers = {}

    def add_peer(self, peer):
        """Add a peer to the network."""
        if peer.peer_id in self.peers:
            raise ValueError("Peer already exists")

        self.peers[peer.peer_id] = peer
        return True

    def get_peer(self, peer_id):
        """Get a peer using its ID."""
        return self.peers.get(peer_id)

    def remove_peer(self, peer_id):
        """Remove a peer using its ID."""
        if peer_id not in self.peers:
            return False

        del self.peers[peer_id]
        return True

    def get_all_peers(self):
        """Return all peers in the network."""
        return list(self.peers.values())
