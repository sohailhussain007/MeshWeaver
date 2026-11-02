from cli_utils import (
    display_node_status,
    display_peer_info,
    display_received_task,
    display_task_result,
)

from peer import Peer


def main():
    # Create a Peer
    peer = Peer(host="127.0.0.1", port=5000)

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