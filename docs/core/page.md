# P2PNode — Component Documentation

## Overview

`P2PNode` is the top-level orchestrator for a peer-to-peer file-sharing node. It groups all subsystems — networking, file management, downloading, and peer discovery — into a single controllable object that represents one running participant in the P2P network.

**Module:** `node.py`  
**Logger name:** `Node`

---

## Architecture

```
P2PNode
├── PeerServer          → Accepts incoming peer connections, routes messages to handlers
├── FileService         → Serves file listings and file data to requesting peers
├── DownloadService     → Connects to remote peers and fetches files
└── DiscoveryService    → Registers with tracker, discovers and maintains peer list
```

The node follows a **register → start → discover** boot sequence, ensuring handlers are in place before the server accepts any connections, and peer discovery happens only after the node is publicly reachable.

---

## Class: `P2PNode`

### Constructor

```python
P2PNode(port: int, tracker_host: str, tracker_port: int)
```

Initialises all core components and sets the node's identity.

| Parameter      | Type  | Description                                             |
| -------------- | ----- | ------------------------------------------------------- |
| `port`         | `int` | Port this node listens on for incoming peer connections |
| `tracker_host` | `str` | Hostname or IP address of the tracker server            |
| `tracker_port` | `int` | Port number of the tracker server                       |

**Initialised attributes:**

| Attribute                | Type               | Description                                       |
| ------------------------ | ------------------ | ------------------------------------------------- |
| `self.port`              | `int`              | Node's network identity                           |
| `self.server`            | `PeerServer`       | Handles incoming connections                      |
| `self.file_service`      | `FileService`      | Data layer — lists and serves files               |
| `self.download_service`  | `DownloadService`  | Client-side file transfer engine                  |
| `self.discovery_service` | `DiscoveryService` | Peer registration and discovery                   |
| `self.running`           | `bool`             | Lifecycle flag; `False` until `start()` is called |

---

### Methods

#### `start() → None`

Transitions the node from _configured_ to _actively participating_ in the P2P network. Executes the following steps in order:

1. **Register handlers** — maps message types to handler functions before accepting any connections.
2. **Start server** — opens the socket and begins listening on `self.port`. The node becomes reachable to peers at this point.
3. **Register with tracker** — advertises this node's IP, port, and availability to the tracker.
4. **Fetch peer list** — pulls the current list of available peers from the tracker, enabling outgoing connections and file requests.
5. **Set `self.running = True`** — marks the node as active.

```python
node.start()
# [Node] Starting P2P Node...
# [Node] Handlers registered
# [Node] Connected peers available: 4
# [Node] Node started successfully
```

---

#### `stop() → None`

Gracefully shuts down the node.

1. Sets `self.running = False`.
2. **Deregisters** from the tracker so other peers stop routing requests here.
3. **Stops the server** — closes the listening socket.

```python
node.stop()
# [Node] Stopping node...
# [Node] Node stopped
```

---

#### `download(host: str, port: int, filename: str) → None`

Initiates a file download from a specific remote peer. Delegates all transfer logic to `DownloadService`.

| Parameter  | Type  | Description                               |
| ---------- | ----- | ----------------------------------------- |
| `host`     | `str` | IP address or hostname of the target peer |
| `port`     | `int` | Listening port of the target peer         |
| `filename` | `str` | Name of the file to fetch                 |

Internally, `DownloadService.download_file()` handles:

- Opening a socket to the target peer
- Sending a `REQUEST_FILE` message
- Receiving the file in chunks
- Reassembling and writing to disk
- Retry / resume logic
- Integrity verification (checksum)

```python
node.download("192.168.1.42", 8080, "report.pdf")
# [Node] Downloading 'report.pdf' from 192.168.1.42:8080
```

---

#### `_register_handlers() → None` _(private)_

Wires message type strings to their handler functions on the server. Called internally by `start()` before the server is brought up.

| Message Type   | Handler                           | Behaviour                                        |
| -------------- | --------------------------------- | ------------------------------------------------ |
| `LIST_FILES`   | `FileService.handle_list_files`   | Returns list of files available on this node     |
| `REQUEST_FILE` | `FileService.handle_file_request` | Reads and streams the requested file to the peer |

This is analogous to URL routing in a web framework — incoming message type maps directly to a handler function.

```
Incoming message "LIST_FILES"
    └── server looks up handler
        └── calls handle_list_files()
            └── returns file list to requesting peer
```

---

## Lifecycle Diagram

```
Instantiate P2PNode
        │
        ▼
  __init__() ── Configures all components, running = False
        │
        ▼
   start()
    ├── _register_handlers()   ← handlers ready before server is up
    ├── server.start()         ← node is now reachable
    ├── discovery.register()   ← node is now discoverable
    └── discovery.refresh_peers() ← node knows who else is out there
        │
        ▼
  running = True
  [Node operational]
        │
        ▼  (on shutdown)
   stop()
    ├── running = False
    ├── discovery.deregister()
    └── server.stop()
```

---

## Design Principles

**Separation of concerns** — `P2PNode` is purely an orchestration layer. It does not contain file I/O, socket management, or discovery logic itself; those responsibilities belong to the respective service classes.

**Boot order safety** — handlers are registered _before_ the server starts listening, preventing any window where a message could arrive with no registered handler.

**Observability** — all lifecycle transitions and key metrics (e.g. peer count) are logged via the structured logger rather than `print()`, enabling production-grade traceability with timestamps and module context.

---

## Dependencies

| Import                                        | Role                                    |
| --------------------------------------------- | --------------------------------------- |
| `utils.logger.get_logger`                     | Structured logger                       |
| `network.server.PeerServer`                   | Socket server and message routing       |
| `services.file_service.FileService`           | File listing and serving                |
| `services.download_service.DownloadService`   | Outgoing file downloads                 |
| `services.discovery_service.DiscoveryService` | Tracker registration and peer discovery |

---

## Example Usage

```python
from node import P2PNode

# Create a node on port 9000, connecting to tracker at localhost:5000
node = P2PNode(port=9000, tracker_host="localhost", tracker_port=5000)

# Start the node (registers, listens, discovers peers)
node.start()

# Download a file from a known peer
node.download("192.168.1.10", 9001, "dataset.csv")

# Gracefully shut down
node.stop()
```
