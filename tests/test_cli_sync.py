from peer import Peer
from peer_manager import PeerManager
from peer_registry import PeerRegistry


def test_peer_removal_from_both_managers():
    peer = Peer(host="127.0.0.1", port=5000)

    manager = PeerManager()
    registry = PeerRegistry()

    manager.add_peer(peer)
    registry.register_peer(peer)

    manager.remove_peer(peer.peer_id)
    registry.unregister_peer(peer.peer_id)

    assert manager.get_peer(peer.peer_id) is None
    assert registry.get_peer(peer.peer_id) is None
