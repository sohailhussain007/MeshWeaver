import json
from uuid import uuid4


def create_message(sender_id, message_type, payload=None):
    """Create a standard MeshWeaver peer message."""
    return {
        "message_id": str(uuid4()),
        "sender_id": sender_id,
        "message_type": message_type,
        "payload": {} if payload is None else payload,
    }


def create_hello_message(peer):
    """Create a HELLO message to announce a peer."""
    return create_message(
        sender_id=peer.peer_id,
        message_type="HELLO",
        payload={
            "peer_id": peer.peer_id,
            "host": peer.host,
            "port": peer.port,
        },
    )


def encode_message(message):
    """Convert a message dictionary to UTF-8 bytes."""
    return json.dumps(message).encode("utf-8")


def decode_message(data):
    """Convert UTF-8 message bytes back to a validated dictionary."""
    try:
        message = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError, AttributeError) as error:
        raise ValueError("Invalid message data") from error

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

    for field in ("message_id", "sender_id", "message_type"):
        if not isinstance(message[field], str) or not message[field].strip():
            raise ValueError(f"{field} must be a non-empty string")

    if not isinstance(message["payload"], dict):
        raise ValueError("payload must be a dictionary")

    if message["message_type"] == "HELLO":
        payload = message["payload"]

        if not isinstance(payload.get("peer_id"), str) or not payload["peer_id"].strip():
            raise ValueError("HELLO peer_id must be a non-empty string")

        if payload["peer_id"] != message["sender_id"]:
            raise ValueError("HELLO peer_id must match sender_id")

        if not isinstance(payload.get("host"), str) or not payload["host"].strip():
            raise ValueError("HELLO host must be a non-empty string")

        port = payload.get("port")

        if not isinstance(port, int) or isinstance(port, bool) or not 1 <= port <= 65535:
            raise ValueError("HELLO port must be between 1 and 65535")

    return message
