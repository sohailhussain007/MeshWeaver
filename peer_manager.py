
from peer import Peer


class PeerManager:
    """Manages peers in the MeshWeaver network."""

    def __init__(self):
        self.peers = {}

    def add_peer(self, peer):
        """Add a peer to the network."""
        if not isinstance(peer, Peer):
            raise TypeError("Only Peer objects can be added")

        if peer.peer_id in self.peers:
            raise ValueError("Peer already exists")

        self.peers[peer.peer_id] = peer
        return True

    def get_peer(self, peer_id):
        """Get a peer using its ID."""
        return self.peers.get(peer_id)

    def has_peer(self, peer_id):
        """Check whether a peer exists."""
        return peer_id in self.peers

    def remove_peer(self, peer_id):
        """Remove a peer using its ID."""
        if not self.has_peer(peer_id):
            return False

        del self.peers[peer_id]
        return True

    def get_all_peers(self):
        """Return all peers in the network."""
        return list(self.peers.values())

    def get_peer_count(self):
        """Return the total number of registered peers."""
        return len(self.peers)

    def clear_all_peers(self):
        """Remove all registered peers and return the removed count."""
        removed_count = self.get_peer_count()
        self.peers.clear()
        return removed_count
    