from meshweaver.cli_utils import (
    display_node_status,
    display_peer_info,
    display_peer_list,
    display_received_task,
    display_task_result,
)

from peer import Peer
from peer_registry import PeerRegistry
from peer_manager import PeerManager


def main():
    peer = Peer(host="127.0.0.1", port=5000)

    # Existing PeerRegistry functionality
    registry = PeerRegistry()
    registry.register_peer(peer)

    # Faizan's PeerManager functionality
    manager = PeerManager()

    if manager.add_peer(peer):
        print("Peer added to PeerManager successfully!")

    found_peer = manager.get_peer(peer.peer_id)

    if found_peer:
        print("Peer found successfully!")
        print(found_peer)
    else:
         print("Peer not found!")

    # Display peer information and list
    display_node_status(peer.peer_id, "Running")
    display_peer_info(peer)
    display_peer_list(registry.get_all_peers())

    # Display task information
    display_received_task("add(10, 20)", "Completed")
    display_task_result(30)

    # Remove peer from PeerManager
    if manager.remove_peer(peer.peer_id):
        registry.unregister_peer(peer.peer_id)
        print("Peer removed successfully from PeerManager and PeerRegistry!")
    else:
        print("Peer not found!")

if __name__ == "__main__":
    main()