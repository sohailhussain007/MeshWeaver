
import unittest
from unittest.mock import patch

from cli_utils import (
    display_node_status,
    display_received_task,
    display_task_result,
)


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
