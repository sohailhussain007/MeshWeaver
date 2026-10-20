import json
from uuid import uuid4
def create_message(sender_id, message_type, payload=None):
    """Create a standard MeshWeaver message."""
    return {
        "message_id": str(uuid4()),
        "sender_id": sender_id,
        "message_type": message_type,
        "payload": payload or {},
    }
def encode_message(message):
    """Convert a message dictionary to UTF-8 bytes."""
    return json.dumps(message).encode("utf-8")
def decode_message(data):
    """Convert UTF-8 message bytes back to a dictionary."""
    message = json.loads(data.decode("utf-8"))
    if not isinstance(message, dict):
        raise ValueError("Message must be a JSON object")
    required_fields = {
        "message_id",
        "sender_id",
        "message_type",
        "payload",
    }
    if not required_fields.issubset(message):
        raise ValueError("Message is missing required fields")
    if not isinstance(message["sender_id"], str) or not message["sender_id"]:
        raise ValueError("sender_id must be a non-empty string")
    if not isinstance(message["message_type"], str) or not message["message_type"]:
        raise ValueError("message_type must be a non-empty string")
    if not isinstance(message["payload"], dict):
        raise ValueError("payload must be a dictionary")
    return message
if __name__ == "__main__":
    message = create_message(
        sender_id="node_A",
        message_type="HELLO",
        payload={"text": "Hello from MeshWeaver!"},
    )
    encoded = encode_message(message)
    decoded = decode_message(encoded)
    print("Original message:", message)
    print("Encoded message:", encoded)
    print("Decoded message:", decoded)
    print("Protocol test successful!")