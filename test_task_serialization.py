
from meshweaver.serialization import serialize_task, deserialize_task


def add(a, b):
    return a + b


def main():
    encoded_task = serialize_task(add, args=(10, 20))
    print("Task serialized successfully!")

    task = deserialize_task(encoded_task)
    result = task["function"](*task["args"], **task["kwargs"])

    print("Result:", result)

    assert result == 30
    print("Serialization test passed!")


if __name__ == "__main__":
    main()
