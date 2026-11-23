import base64
import pytest

from meshweaver.serialization import deserialize_task


def test_deserialize_task_handles_unpickling_error():
    invalid_pickle = base64.b64encode(b"not a valid pickle").decode("ascii")

    with pytest.raises(ValueError, match="Invalid serialized task data"):
        deserialize_task(invalid_pickle)
