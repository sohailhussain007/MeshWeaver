
import unittest
from unittest.mock import patch

from cli import main
from cli_utils import (
    display_node_status,
    display_peer_info,
    display_received_task,
    display_task_result,
)
from peer import Peer
from peer_manager import PeerManager


class TestCLI(unittest.TestCase):

    @patch("builtins.print")
    def test_main_execution(self, mock_print):
        main()

        output = "\n".join(
            str(call.args[0]) for call in mock_print.call_args_list
        )

        self.assertIn("Peer added to PeerManager successfully!", output)
        self.assertIn("Peer found successfully!", output)
        self.assertIn("Peer removed successfully!", output)
        self.assertIn("Running", output)
        self.assertIn("Completed", output)
        self.assertIn("30", output)

    @patch("builtins.print")
    def test_display_node_status(self, mock_print):
        display_node_status("node-01", "Running")

        output = "\n".join(
            str(call.args[0]) for call in mock_print.call_args_list
        )

        self.assertIn("node-01", output)
        self.assertIn("Running", output)

    @patch("builtins.print")
    def test_display_peer_info(self, mock_print):
        peer = Peer(
            peer_id="node-01",
            host="127.0.0.1",
            port=5000
        )

        display_peer_info(peer)

        output = "\n".join(
            str(call.args[0]) for call in mock_print.call_args_list
        )

        self.assertIn("node-01", output)
        self.assertIn("127.0.0.1", output)
        self.assertIn("5000", output)

    @patch("builtins.print")
    def test_display_received_task(self, mock_print):
        display_received_task("add(10, 20)", "Completed")

        output = "\n".join(
            str(call.args[0]) for call in mock_print.call_args_list
        )

        self.assertIn("add(10, 20)", output)
        self.assertIn("Completed", output)

    @patch("builtins.print")
    def test_display_task_result(self, mock_print):
        display_task_result(30)

        output = "\n".join(
            str(call.args[0]) for call in mock_print.call_args_list
        )

        self.assertIn("30", output)

    def test_cli_add_peer(self):
        manager = PeerManager()
        peer = Peer(peer_id="node-101", port=5000)

        self.assertTrue(manager.add_peer(peer))
        self.assertEqual(manager.get_peer("node-101"), peer)

    def test_cli_search_peer(self):
        manager = PeerManager()
        peer = Peer(peer_id="node-102", port=5001)

        manager.add_peer(peer)

        self.assertEqual(manager.get_peer("node-102"), peer)

    def test_cli_remove_peer(self):
        manager = PeerManager()
        peer = Peer(peer_id="node-103", port=5002)

        manager.add_peer(peer)

        self.assertTrue(manager.remove_peer("node-103"))
        self.assertIsNone(manager.get_peer("node-103"))


if __name__ == "__main__":
    unittest.main()
