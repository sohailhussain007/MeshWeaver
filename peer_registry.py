class PeerRegistry:
    """Manages discovered peers in the MeshWeaver network."""
    def __init__(self):
        self.peers = {}
    def register_peer(self, peer):
        """Register a new peer."""
        if peer.peer_id in self.peers:
            return False
        self.peers[peer.peer_id] = peer
        return True
    def get_peer(self, peer_id):
        """Find a peer using its ID."""
        return self.peers.get(peer_id)
    def get_all_peers(self):
        """Return all registered peers."""
        return list(self.peers.values())
    def unregister_peer(self, peer_id):
        """Remove a registered peer."""
        return self.peers.pop(peer_id, None) is not None