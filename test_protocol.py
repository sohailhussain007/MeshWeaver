
from meshweaver.protocol import create_message, parse_message


def main():
    original_data = {"node_id": "node_1"}

    raw_message = create_message("PING", original_data)
    print("Created message:", raw_message)

    parsed_message = parse_message(raw_message)
    print("Parsed message:", parsed_message)

    assert parsed_message["type"] == "PING"
    assert parsed_message["data"] == original_data

    print("Protocol test passed!")


if __name__ == "__main__":
    main()
