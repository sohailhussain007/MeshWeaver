from meshweaver.cli_utils import (
    display_node_status,
    display_peer_info,
    display_peer_list,
    display_received_task,
    display_task_result,
)

from peer import Peer
from peer_registry import PeerRegistry


def main():
    peer = Peer(host="127.0.0.1", port=5000)

    registry = PeerRegistry()
    registry.register_peer(peer)

    display_node_status(peer.peer_id, "Running")
    display_peer_info(peer)
    display_peer_list(registry.get_all_peers())
    display_received_task("add(10, 20)", "Completed")
    display_task_result(30)


if __name__ == "__main__":
    main()

