
from cli_utils import (
    display_node_status,
    display_peer_info,
    display_peer_list,
    display_received_task,
    display_task_result,
)

from peer import Peer
from peer_manager import PeerManager


def main():
    # Create PeerManager
    manager = PeerManager()

    # Create a Peer
    peer = Peer(host="127.0.0.1", port=5000)

    # Add peer to PeerManager
    manager.add_peer(peer)

    print("Peer added to PeerManager successfully!")

    # Search peer using ID
    found_peer = manager.get_peer(peer.peer_id)

    if found_peer:
        print("Peer found successfully!")
        print(found_peer)
    else:
        print("Peer not found!")

    # Display all peers
    all_peers = manager.get_all_peers()
    display_peer_list(all_peers)

    # Remove peer from PeerManager
    removed = manager.remove_peer(peer.peer_id)

    if removed:
        print("Peer removed successfully!")
    else:
        print("Peer not found!")

    # Display node status
    display_node_status(peer.peer_id, "Running")

    # Display peer information
    display_peer_info(peer)

    # Display received task
    display_received_task("add(10, 20)", "Completed")

    # Display task result
    display_task_result(30)


if __name__ == "__main__":
    main()
