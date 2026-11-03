from peer import Peer


class PeerRegistry:
    """Manages discovered peers in the MeshWeaver network."""

    def __init__(self):
        self.peers = {}

    def register_peer(self, peer):
        """Register a new peer."""
        if not isinstance(peer, Peer):
            raise ValueError("peer must be a Peer instance")

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

    def register_hello_message(self, message):
        """Register a peer from a validated HELLO message."""
        if not isinstance(message, dict):
            raise ValueError("Message must be a dictionary")

        if message.get("message_type") != "HELLO":
            raise ValueError("Message must be a HELLO message")

        payload = message.get("payload")

        if not isinstance(payload, dict):
            raise ValueError("HELLO payload must be a dictionary")

        peer_id = payload.get("peer_id")
        host = payload.get("host")
        port = payload.get("port")

        if not isinstance(peer_id, str) or not peer_id.strip():
            raise ValueError("HELLO peer_id must be a non-empty string")

        if message.get("sender_id") != peer_id:
            raise ValueError("HELLO peer_id must match sender_id")

        if not isinstance(host, str) or not host.strip():
            raise ValueError("HELLO host must be a non-empty string")

        if (
            not isinstance(port, int)
            or isinstance(port, bool)
            or not 1 <= port <= 65535
        ):
            raise ValueError("HELLO port must be between 1 and 65535")

        peer = Peer(
            peer_id=peer_id,
            host=host,
            port=port,
        )

        return self.register_peer(peer)
