# MeshWeaver

MeshWeaver is a Python-based distributed task execution system that uses asynchronous UDP communication to connect peers, discover nodes, and forward tasks between them.

## Features

### Peer Communication
- Asynchronous UDP server and client communication.
- Peer registration using HELLO messages.
- Heartbeat request and acknowledgment.
- PING/PONG health checks.

### Peer Discovery
- Peer discovery through HELLO_ACK messages.
- Kademlia-based node identification and routing table support.
- Peer JOINED and LEFT notifications.
- Periodic stale-peer cleanup.

### Distributed Task Execution
- Python task serialization using Cloudpickle and Base64.
- CPU-load-based worker selection and task forwarding.
- Task result and error responses.
- Task retry mechanism.
- Duplicate-task detection to prevent repeated execution.

### Reliability and Error Handling
- Concurrent task execution limits.
- Task timeout and cancellation handling.
- Handling of corrupted serialized task data.
- Graceful UDP server and client shutdown.

## Technologies Used
- Python
- Asyncio
- UDP Networking
- Cloudpickle
- Psutil
- Base64
- JSON
- Pytest

## Installation

Clone the repository:

```bash
git clone https://github.com/sohailhussain007/MeshWeaver.git
cd MeshWeaver
```

Create and activate a virtual environment (Windows PowerShell):

```powershell
python -m venv myenv
.\myenv\Scripts\Activate.ps1
```

Install dependencies:

```powershell
python -m pip install -r requirements.txt
```

## Running the Project

Start the UDP server:

```powershell
python -m meshweaver.udp_server
```

Start a UDP client in another terminal:

```powershell
python -m meshweaver.udp_client
```

You can start additional clients to test peer discovery and join/leave notifications.

## Running Tests

Run the complete test suite:

```powershell
python -m pytest -v
```

## Task Serialization Example

```python
from meshweaver.serialization import serialize_task, deserialize_task

def add(a, b):
    return a + b

encoded_task = serialize_task(add, args=(10, 20))
task = deserialize_task(encoded_task)

result = task["function"](*task["args"], **task["kwargs"])
print(result)  # 30
```

## Security Note

Cloudpickle can deserialize executable Python objects. Only deserialize data received from trusted sources.

## Project Documentation

Additional integration notes are available in `docs/integration-review.md`.
