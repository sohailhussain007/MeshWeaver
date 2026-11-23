from peer import Peer
import time


class PeerRegistry:
    """Manages discovered peers in the MeshWeaver network."""

    def __init__(self):
        self.peers = {}
        self.cpu_loads = {}
        self.last_seen = {}

    def register_peer(self, peer):
        """Register a new peer."""
        if not isinstance(peer, Peer):
            raise ValueError("peer must be a Peer instance")

        if peer.peer_id in self.peers:
            self.last_seen[peer.peer_id] = time.monotonic()
            return False

        self.peers[peer.peer_id] = peer
        self.last_seen[peer.peer_id] = time.monotonic()
        return True

    def get_peer(self, peer_id):
        return self.peers.get(peer_id)

    def get_all_peers(self):
        return list(self.peers.values())

    def unregister_peer(self, peer_id):
        """Remove a registered peer and its tracking data."""
        self.cpu_loads.pop(peer_id, None)
        self.last_seen.pop(peer_id, None)
        return self.peers.pop(peer_id, None) is not None

    def update_last_seen(self, peer_id):
        """Refresh peer heartbeat timestamp."""
        if peer_id not in self.peers:
            return False

        self.last_seen[peer_id] = time.monotonic()
        return True

    def remove_stale_peers(self, timeout):
        """Remove peers that have not sent a heartbeat within timeout."""
        now = time.monotonic()

        stale_ids = [
            peer_id
            for peer_id, last_seen in self.last_seen.items()
            if now - last_seen > timeout
        ]

        for peer_id in stale_ids:
            self.unregister_peer(peer_id)

        return stale_ids

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
        node_id = payload.get("node_id")

        if not isinstance(node_id, str) or len(node_id) != 40:
            raise ValueError("HELLO node_id must contain 40 hexadecimal characters")

        try:
            int(node_id, 16)
        except ValueError:
            raise ValueError("HELLO node_id must contain hexadecimal characters")

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
            node_id=node_id,
        )
        return self.register_peer(peer)

    def update_cpu_load(self, peer_id, cpu_load):
        """Update the CPU load of a registered peer."""
        if peer_id not in self.peers:
            return False

        if (
            not isinstance(cpu_load, (int, float))
            or isinstance(cpu_load, bool)
            or not 0 <= cpu_load <= 100
        ):
            raise ValueError("CPU load must be between 0 and 100")

        self.cpu_loads[peer_id] = float(cpu_load)
        return True

    def select_lowest_cpu_peer(self):
        """Select the available peer with the lowest CPU load."""
        available_peers = [
            peer_id for peer_id in self.cpu_loads
            if peer_id in self.peers
        ]

        if not available_peers:
            return None

        selected_peer_id = min(
            available_peers,
            key=lambda peer_id: self.cpu_loads[peer_id]
        )
        return self.peers[selected_peer_id]

    def has_peer(self, peer_id):
        return peer_id in self.peers

    def get_peer_count(self):
        return len(self.peers)

    def clear_all_peers(self):
        removed_count = self.get_peer_count()
        for peer_id in list(self.peers):
            self.unregister_peer(peer_id)
        return removed_count

    def search_peers(self, host=None, port=None):
        results = []
        for peer in self.peers.values():
            if host is not None and peer.host != host:
                continue
            if port is not None and peer.port != port:
                continue
            results.append(peer)
        return results
