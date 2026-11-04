
import json


def create_message(message_type, data):
    """Create a JSON message for communication between nodes."""
    if not isinstance(message_type, str) or not message_type:
        raise ValueError("Message type must be a non-empty string")

    message = {
        "type": message_type,
        "data": data,
    }
    return json.dumps(message)


def parse_message(raw_message):
    """Convert a JSON message into a validated Python dictionary."""
    message = json.loads(raw_message)

    if not isinstance(message, dict):
        raise ValueError("Message must be a JSON object")

    if "type" not in message or "data" not in message:
        raise ValueError("Message must contain 'type' and 'data'")

    if not isinstance(message["type"], str) or not message["type"]:
        raise ValueError("Message type must be a non-empty string")

    return message

