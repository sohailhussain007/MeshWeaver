
import cloudpickle
import base64

def add(a, b):
    return a + b


task = {
    "function": add,
    "args": (10, 20),
    "kwargs": {},
}

serialized_task = cloudpickle.dumps(task)
encoded_task = base64.b64encode(serialized_task).decode("ascii")
decoded_bytes = base64.b64decode(encoded_task)
print("Decoded bytes match:", decoded_bytes == serialized_task)

restored_task = cloudpickle.loads(decoded_bytes)
print("Restored function result:", restored_task["function"](*restored_task["args"]))
print("Encoded task type:", type(encoded_task))
print("Serialized data type:", type(serialized_task))
print("Task serialized successfully!")

restored_task = cloudpickle.loads(serialized_task)

function = restored_task["function"]
args = restored_task["args"]
kwargs = restored_task["kwargs"]

result = function(*args, **kwargs)

print("Result:", result)

