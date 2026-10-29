import pytest

from meshweaver.protocol import create_message, parse_message


def test_parse_invalid_json():
    with pytest.raises(ValueError):
        parse_message("{invalid json}")


def test_parse_message_missing_type():
    with pytest.raises(ValueError):
        parse_message('{"data": {}}')


def test_parse_message_missing_data():
    with pytest.raises(ValueError):
        parse_message('{"type": "PING"}')


def test_parse_message_non_object():
    with pytest.raises(ValueError):
        parse_message('["PING", {}]')


def test_create_message_rejects_empty_type():
    with pytest.raises(ValueError):
        create_message("", {})
