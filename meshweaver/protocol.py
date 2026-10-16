
import json


def create_message(message_type, data):
    """Create a JSON message for communication between nodes."""
    message = {
        "type": message_type,
        "data": data,
    }
    return json.dumps(message)


def parse_message(raw_message):
    """Convert a JSON message into a Python dictionary."""
    message = json.loads(raw_message)

    if not isinstance(message, dict):
        raise ValueError("Message must be a JSON object")

    if "type" not in message or "data" not in message:
        raise ValueError("Message must contain 'type' and 'data'")

    return message
