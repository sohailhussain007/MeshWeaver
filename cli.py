
from cli_utils import (
    display_node_status,
    display_received_task,
    display_task_result,
)


def main():
    display_node_status("node-01", "Running")
    display_received_task("add(10, 20)", "Completed")
    display_task_result(30)


if __name__ == "__main__":
    main()
