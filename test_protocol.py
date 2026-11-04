

from meshweaver.protocol import create_message, parse_message


def main():
    original_data = {"node_id": "node_1"}

    # Test 1: Valid message
    raw_message = create_message("PING", original_data)
    print("Created message:", raw_message)

    parsed_message = parse_message(raw_message)
    print("Parsed message:", parsed_message)

    assert parsed_message["type"] == "PING"
    assert parsed_message["data"] == original_data

    # Test 2: Empty message type must be rejected
    try:
        create_message("", original_data)
        raise AssertionError("Empty message type was accepted")
    except ValueError:
        print("Empty message type rejected!")

    # Test 3: Non-string message type must be rejected
    try:
        create_message(123, original_data)
        raise AssertionError("Non-string message type was accepted")
    except ValueError:
        print("Non-string message type rejected!")

    # Test 4: Parsed message with empty type must be rejected
    try:
        parse_message('{"type": "", "data": {}}')
        raise AssertionError("Parsed empty message type was accepted")
    except ValueError:
        print("Parsed empty message type rejected!")

    print("All protocol tests passed!")


if __name__ == "__main__":
    main()
