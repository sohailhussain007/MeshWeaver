import unittest
from unittest.mock import patch

from meshweaver.cli_utils import (
    display_node_status,
    display_peer_info,
    display_received_task,
    display_task_result,
)
from peer import Peer


class TestCLI(unittest.TestCase):

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
            port=5000,
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


if __name__ == "__main__":
    unittest.main()
