# P2P File Sharing System — Complete Documentation

[Core concepts docs] (https://banyancloud-my.sharepoint.com/:w:/r/personal/a_praveen_banyancloud_io/_layouts/15/Doc.aspx?sourcedoc=%7B18FD80E4-D63B-4B42-96DF-5FD57BC8833A%7D&file=Document.docx&action=editNew&mobileredirect=true&wdOrigin=APPHOME-WEB.DIRECT%2CAPPHOME-WEB.BANNER.NEWBLANK&wdPreviousSession=6b8c05ce-0bf3-43e5-83df-83286b65bdef&wdPreviousSessionSrc=AppHomeWeb&ct=1778041032977)

## Table of Contents

- [P2P File Sharing System — Complete Documentation](#p2p-file-sharing-system--complete-documentation)
  - [Table of Contents](#table-of-contents)
  - [System Overview](#system-overview)
  - [Architecture](#architecture)
  - [Module Reference](#module-reference)
    - [`config.py`](#configpy)
    - [`logger.py`](#loggerpy)
    - [`protocol.py`](#protocolpy)
    - [`client.py` — `PeerClient`](#clientpy--peerclient)
    - [`server.py` — `PeerServer`](#serverpy--peerserver)
    - [`tracker.py` — `Tracker`](#trackerpy--tracker)
    - [`discovery_service.py` — `DiscoveryService`](#discovery_servicepy--discoveryservice)
    - [`file_service.py` — `FileService`](#file_servicepy--fileservice)
    - [`download_service.py` — `DownloadService`](#download_servicepy--downloadservice)
    - [`node.py` — `P2PNode`](#nodepy--p2pnode)
  - [Message Protocol](#message-protocol)
    - [Discovery messages (Node ↔ Tracker)](#discovery-messages-node--tracker)
    - [File listing messages (Node ↔ Node)](#file-listing-messages-node--node)
    - [File transfer messages (Node ↔ Node)](#file-transfer-messages-node--node)
    - [Error messages](#error-messages)
  - [End-to-End Flows](#end-to-end-flows)
    - [1. Node Startup](#1-node-startup)
    - [2. Discovering Peers](#2-discovering-peers)
    - [3. Listing Remote Files](#3-listing-remote-files)
    - [4. Downloading a File](#4-downloading-a-file)
    - [5. Serving a File Request](#5-serving-a-file-request)
    - [6. Node Shutdown](#6-node-shutdown)
  - [Data Flow Diagram](#data-flow-diagram)
  - [Directory Layout](#directory-layout)
  - [Configuration Reference](#configuration-reference)
  - [Error Handling Strategy](#error-handling-strategy)
  - [Security Considerations](#security-considerations)

---

## System Overview

This is a **peer-to-peer file sharing system** built in Python. Unlike centralised file servers, every participant (a _node_) can both serve and request files simultaneously. A lightweight **Tracker** server acts as a directory — it keeps track of which peers are online and hands out their addresses, but never handles file data itself.

```
         ┌─────────────────────────────────────────┐
         │              Tracker Server              │
         │  • Maintains peer registry (ip, port)    │
         │  • Responds to REGISTER / GET_PEERS /    │
         │    DEREGISTER requests only              │
         └──────────────┬──────────────────────────┘
                        │ peer discovery only
           ┌────────────┼────────────┐
           │            │            │
      ┌────▼───┐   ┌────▼───┐   ┌───▼────┐
      │ Node A │◄──│ Node B │──►│ Node C │
      └────────┘   └────────┘   └────────┘
         direct peer-to-peer file transfers
```

**Key principles:**

- The Tracker knows _who_ is online but never touches file data.
- File transfers happen **directly between peers** over TCP.
- Every file transfer is **integrity-verified** via SHA-256 checksum.
- Serving a file requires **explicit user approval** — no file is sent without consent.
- All network messages are framed JSON over TCP with a 4-byte length header.

---

## Architecture

The system is organised into four layers:

```
┌─────────────────────────────────────────────────────────┐
│                    Application Layer                    │
│  P2PNode  ·  Tracker                                    │
├─────────────────────────────────────────────────────────┤
│                     Service Layer                       │
│  FileService  ·  DownloadService  ·  DiscoveryService   │
├─────────────────────────────────────────────────────────┤
│                     Network Layer                       │
│  PeerServer  ·  PeerClient  ·  Protocol                 │
├─────────────────────────────────────────────────────────┤
│                     Utility Layer                       │
│  Config  ·  Logger                                      │
└─────────────────────────────────────────────────────────┘
```

| Layer           | Responsibility                                               |
| --------------- | ------------------------------------------------------------ |
| **Application** | Orchestrates components; owns lifecycle (start/stop)         |
| **Service**     | Business logic — file ops, downloads, peer discovery         |
| **Network**     | Raw TCP send/receive, message framing, connection management |
| **Utility**     | Configuration constants, structured logging                  |

---

## Module Reference

---

### `config.py`

Central configuration for the entire system. All other modules import constants from here — nothing is hardcoded elsewhere.

**Directory paths:**

| Constant        | Default path            | Purpose                                  |
| --------------- | ----------------------- | ---------------------------------------- |
| `BASE_DIR`      | Project root            | Resolved from `config.py` location       |
| `DOWNLOADS_DIR` | `<root>/downloads`      | Where received files are saved           |
| `SHARED_DIR`    | `<root>/shared_files`   | Files this node makes available to peers |
| `TEMP_DIR`      | `<root>/.tmp_transfers` | Partial downloads (resume support)       |

All three directories are created automatically at import time via `mkdir(parents=True, exist_ok=True)`.

**Network constants:**

| Constant             | Value             | Purpose                          |
| -------------------- | ----------------- | -------------------------------- |
| `DEFAULT_PORT`       | `5000`            | Default port for peer servers    |
| `TRACKER_PORT`       | `5002`            | Default port for the Tracker     |
| `BACKLOG`            | `5`               | Max queued TCP connections       |
| `SOCKET_TIMEOUT`     | `30.0` s          | Timeout for socket operations    |
| `CHUNK_SIZE`         | `524288` (512 KB) | File read/write chunk size       |
| `TRANSFER_TIMEOUT`   | `60.0` s          | Max wait between chunks          |
| `CHECKSUM_ALGORITHM` | `"sha256"`        | Integrity verification algorithm |

**Message type constants:**

Rather than using raw strings throughout the codebase, all protocol message types are defined here as named constants, grouped by function:

```
Discovery:    MSG_REGISTER, MSG_DEREGISTER, MSG_GET_PEERS, MSG_PEER_LIST
File listing: MSG_LIST_FILES, MSG_FILE_LIST
Transfer:     MSG_REQUEST_FILE, MSG_APPROVED, MSG_REJECTED,
              MSG_TRANSFER_START, MSG_TRANSFER_DONE, MSG_FILE_CHUNK
Generic:      MSG_ACK, MSG_ERROR
```

---

### `logger.py`

A lightweight structured logger with ANSI colour support. Every module obtains a named logger via `get_logger(name)`.

**Log levels and colours:**

| Method             | Label   | Colour  | Use case                                    |
| ------------------ | ------- | ------- | ------------------------------------------- |
| `log.info(msg)`    | `INFO`  | Cyan    | General lifecycle events                    |
| `log.success(msg)` | `OK`    | Green   | Confirmations of completed operations       |
| `log.warn(msg)`    | `WARN`  | Yellow  | Non-fatal problems, degraded operation      |
| `log.error(msg)`   | `ERROR` | Red     | Failures that stop an operation             |
| `log.debug(msg)`   | `DEBUG` | Magenta | Verbose detail, only shown when debug is on |

**Output format:**

```
[HH:MM:SS] [ModuleName] [LEVEL] message text
```

**Colour detection:** `supports_color()` checks `sys.stdout.isatty()` — colours are disabled automatically when output is piped to a file or CI system.

**Debug mode:** Call `enable_debug()` once at startup to activate `log.debug()` output globally.

---

### `protocol.py`

Handles all message serialisation, framing, and provides constructor helpers for every message type used in the system.

**Wire format:**

Every message is a JSON object, framed with a 4-byte big-endian unsigned integer length header:

```
┌──────────────────┬──────────────────────────────────────┐
│   4 bytes        │   N bytes                            │
│  payload length  │  UTF-8 encoded JSON payload          │
│  (big-endian)    │                                      │
└──────────────────┴──────────────────────────────────────┘
```

This framing enables the receiver to know exactly how many bytes to read, regardless of TCP packet boundaries.

**Core functions:**

| Function                      | Description                                                                                     |
| ----------------------------- | ----------------------------------------------------------------------------------------------- |
| `send_message(sock, message)` | Serialises dict → JSON → bytes, prepends length header, sends atomically via `sendall`          |
| `recv_exact(sock, size)`      | Reads exactly `size` bytes from socket, looping over partial reads                              |
| `recv_message(sock)`          | Reads length header, then payload; rejects messages over **10 MB** to prevent memory exhaustion |

**Message constructors:**

| Constructor                               | Returns                                                  |
| ----------------------------------------- | -------------------------------------------------------- |
| `make_list_files()`                       | `{"type": "LIST_FILES"}`                                 |
| `make_file_list(files)`                   | `{"type": "FILE_LIST", "files": [...]}`                  |
| `make_request_file(filename)`             | `{"type": "REQUEST_FILE", "filename": "..."}`            |
| `make_approved(filename, size, checksum)` | `{"type": "APPROVED", "filename", "size", "checksum"}`   |
| `make_rejected(filename, reason)`         | `{"type": "REJECTED", "filename", "reason"}`             |
| `make_chunk(index, data)`                 | `{"type": "FILE_CHUNK", "index": N, "data": "<base64>"}` |
| `make_done(filename)`                     | `{"type": "TRANSFER_DONE", "filename": "..."}`           |

> **Why base64 for chunks?** File data is arbitrary binary. JSON must be valid UTF-8 text. Base64 encodes binary to ASCII safely, at a ~33% size overhead per chunk.

---

### `client.py` — `PeerClient`

A thin, stateful wrapper around a TCP socket for **outgoing** connections. Used by `DownloadService` and `DiscoveryService`.

```python
PeerClient(host: str, port: int = 5000, timeout: float = 10)
```

| Method                      | Description                                                                                                             |
| --------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| `connect()`                 | Opens TCP socket, sets timeout, performs handshake. Raises `ConnectionError` on failure. Guards against double-connect. |
| `send(message)`             | Calls `protocol.send_message()`. Sets `connected = False` on failure.                                                   |
| `receive()`                 | Calls `protocol.recv_message()`. Sets `connected = False` on failure.                                                   |
| `send_and_receive(message)` | Convenience: `send()` then `receive()` in sequence.                                                                     |
| `close()`                   | Closes socket silently; sets `connected = False`. Safe to call multiple times.                                          |
| `_ensure_connected()`       | Private guard called before any send/receive; raises `RuntimeError` if not connected.                                   |

**State machine:**

```
Instantiated
    │
    ▼
connect() called
    ├── success → connected = True
    └── failure → raises ConnectionError
         │
         ▼
    send() / receive() calls
         │
         ├── success → returns data
         └── failure → connected = False, raises ConnectionError
              │
              ▼
         close() → connected = False
```

---

### `server.py` — `PeerServer`

A multi-threaded TCP server for **incoming** peer connections. Runs on a background daemon thread and dispatches each connection to a `ThreadPoolExecutor`.

```python
PeerServer(host: str = "0.0.0.0", port: int = 5000, max_workers: int = 10)
```

**Key design decisions:**

- **Daemon thread for `_run()`** — the server loop runs in the background; the main thread is free to run the CLI or other logic.
- **`ThreadPoolExecutor` with `max_workers=10`** — bounds concurrency; prevents resource exhaustion from a flood of connections.
- **`SO_REUSEADDR`** — allows immediate restart without OS port timeout (TIME_WAIT state).
- **`settimeout(1.0)` on server socket** — `accept()` unblocks every second so the `while self.running` check can exit cleanly.
- **`_requester_ip` injection** — the server adds the peer's IP address to every message dict before passing it to a handler, so handlers don't need raw socket access to know who is calling.

| Method                                | Description                                                                            |
| ------------------------------------- | -------------------------------------------------------------------------------------- |
| `register_handler(msg_type, handler)` | Maps a message type string to a callable `handler(conn, message)`                      |
| `start()`                             | Launches `_run()` on a daemon thread; returns immediately                              |
| `_run()`                              | Binds socket, enters accept loop, submits each connection to the thread pool           |
| `_handle_client(conn, addr)`          | Reads messages in a loop, routes to registered handler; closes connection on exception |
| `stop()`                              | Sets `running = False`, shuts down socket with `SHUT_RDWR`, shuts down thread pool     |

**Handler contract:**

```python
def my_handler(conn: socket.socket, message: dict) -> None:
    # read message fields
    # call send_message(conn, response) to reply
    # do NOT close conn — the server does that
```

---

### `tracker.py` — `Tracker`

The central **peer registry server**. Accepts short-lived TCP connections, processes a single request per connection, and maintains a set of active `(ip, port)` peers.

```python
Tracker(host: str = "0.0.0.0", port: int = 5002)
```

`start()` is a **blocking call** — it runs the accept loop directly on the calling thread. Spawn in a separate thread or process if needed alongside other components.

Each incoming connection is handled in its own **daemon thread** (one-shot: one message in, one response out, then close).

**Peer registry (`self.peers`):**

- Type: `set` of `(ip: str, port: int)` tuples
- In-memory only — lost on restart
- No TTL or heartbeat — entries persist until explicit `DEREGISTER`

**Request handling:**

| Incoming message | Required field   | Action                                              | Response                               |
| ---------------- | ---------------- | --------------------------------------------------- | -------------------------------------- |
| `REGISTER`       | `port`           | Adds `(client_ip, port)` to `self.peers`            | `ACK`                                  |
| `REGISTER`       | _(missing port)_ | No change                                           | `ERROR: Missing port`                  |
| `GET_PEERS`      | —                | Serialises `self.peers`                             | `PEER_LIST` with `[{host, port}, ...]` |
| `DEREGISTER`     | `port`           | Removes `(client_ip, port)` if present (idempotent) | `ACK`                                  |
| _(unknown)_      | —                | No change                                           | `ERROR: Unknown type`                  |

> **Port distinction:** The `port` field in `REGISTER`/`DEREGISTER` is the peer's _server listening port_, not the ephemeral source port of the TCP connection. The Tracker uses `addr[0]` (client IP) combined with the message `port` to form the registry entry.

---

### `discovery_service.py` — `DiscoveryService`

Manages this node's relationship with the Tracker. Acts as the client-side counterpart to `Tracker`.

```python
DiscoveryService(port: int, tracker_host: str, tracker_port: int)
```

Each method opens a fresh `PeerClient` connection to the Tracker, performs one request/response exchange, then closes it. The Tracker is treated as a stateless endpoint; connections are not reused.

| Method            | Sends               | Receives            | Side effect                            |
| ----------------- | ------------------- | ------------------- | -------------------------------------- |
| `register()`      | `REGISTER {port}`   | `ACK`               | Node becomes visible to other peers    |
| `refresh_peers()` | `GET_PEERS`         | `PEER_LIST`         | Updates `self.peers`; returns the list |
| `deregister()`    | `DEREGISTER {port}` | _(fire and forget)_ | Node is removed from tracker registry  |

**Error handling:** All three methods catch exceptions individually. A unreachable Tracker during `deregister()` is logged as a warning but does not raise — shutdown should always complete.

`self.peers` stores the last known list as `[{"host": str, "port": int}, ...]` and is returned by `refresh_peers()` for use by `P2PNode`.

---

### `file_service.py` — `FileService`

The most complex service — handles both sides of the file listing protocol and the complete server-side flow of a file transfer, including user approval.

```python
FileService()   # creates SHARED_DIR if it doesn't exist
```

**Local helpers:**

| Method                         | Description                                                                                                           |
| ------------------------------ | --------------------------------------------------------------------------------------------------------------------- |
| `get_files()`                  | Iterates `SHARED_DIR`, returns `[{filename, size}, ...]` for all files                                                |
| `_safe_path(filename)`         | Resolves `SHARED_DIR / filename`, blocks directory traversal by checking the resolved path starts within `SHARED_DIR` |
| `_compute_checksum(file_path)` | SHA-256 over file read in `CHUNK_SIZE` chunks; must match `DownloadService._compute_checksum()` exactly               |

**Client side — listing remote files:**

`list_remote_files(host, port)` opens a connection, sends `LIST_FILES`, and returns the file list. Returns `[]` on any error so the caller can continue with other peers without crashing.

**Server side — responding to `LIST_FILES`:**

`handle_list_files(conn, message)` calls `get_files()` and sends a `FILE_LIST` response. Registered as a handler on `PeerServer` by `P2PNode._register_handlers()`.

**Server side — responding to `REQUEST_FILE`:**

`handle_file_request(conn, message)` runs a multi-step validation pipeline before sending any data:

```
1. Filename present in message?              → ERROR if not
2. File in get_files() list?                 → REJECTED if not
3. _safe_path() passes?                      → REJECTED if traversal detected
4. File exists on disk?                      → REJECTED if not
5. Push to approval_queue → wait for user    → REJECTED if user declines
6. User approved → compute checksum          → send APPROVED
7. Send file in chunks                       → send TRANSFER_DONE
```

**Approval mechanism:**

File requests do not auto-approve. The flow uses a `threading.Event` + a one-element list (`result_box`) as a thread-safe signal between the network handler thread and the CLI thread:

```
Handler thread                    CLI thread
──────────────                    ──────────
Push {filename, event, result_box}
    to approval_queue
Wait on event.wait()  ←──────── CLI reads queue
                                 Prompts user
                                 Sets result_box[0] = "approved" / "rejected"
                      ──────────►Sets event
Resume after event
Read result_box[0]
Send APPROVED or REJECTED
```

**File sending** (`_send_file`):

Reads the file in `CHUNK_SIZE` blocks, base64-encodes each, sends as `FILE_CHUNK {index, data}`, then sends `TRANSFER_DONE`. Chunk index starts at 0 and increments sequentially.

---

### `download_service.py` — `DownloadService`

The client-side engine for receiving files from peers.

```python
DownloadService()   # creates DOWNLOADS_DIR if it doesn't exist
```

**`download_file(host, port, filename)`:**

Opens a `PeerClient` connection to the target peer and runs the full download protocol:

```
1. Send REQUEST_FILE {filename}
2. Receive APPROVED or REJECTED
   └── REJECTED  → log reason, return
   └── unexpected → log error, return
   └── APPROVED  → extract size + checksum
                   checksum must be present or abort
3. Call _receive_file() to collect chunks
4. Close connection (via finally)
```

**`_receive_file(client, filename, expected_checksum)`:**

Collects chunks into `received_chunks: dict[int, bytes]` — keyed by chunk index to handle any ordering:

```
Loop:
  receive message
  ├── FILE_CHUNK  → base64-decode, store in received_chunks[index]
  ├── TRANSFER_DONE →
  │    1. Check received_chunks is not empty
  │    2. Sort by index → ordered_chunks
  │    3. Verify no gaps: set(range(N)) == set(received_chunks.keys())
  │    4. Write chunks to disk at DOWNLOADS_DIR / filename
  │    5. Compute SHA-256 of written file
  │    6. Compare against expected_checksum from APPROVED
  │       └── mismatch → delete file, log error, return
  │       └── match    → log success, break
  └── other → log warning, continue
```

**`_compute_checksum(file_path)`:**

SHA-256 over the saved file using the same `CHUNK_SIZE`-based iteration as `FileService._compute_checksum()`. Both must use identical logic for checksums to match.

---

### `node.py` — `P2PNode`

The top-level orchestrator. Composes all services into a single operable unit.

```python
P2PNode(port: int, tracker_host: str, tracker_port: int)
```

**Startup sequence (`start()`):**

```
1. _register_handlers()     — wire message types to handlers
2. server.start()           — node becomes reachable
3. discovery.register()     — node becomes discoverable
4. discovery.refresh_peers() — node learns about its peers
5. running = True
```

Handlers registered: `LIST_FILES → file_service.handle_list_files`, `REQUEST_FILE → file_service.handle_file_request`

**Shutdown sequence (`stop()`):**

```
1. running = False
2. discovery.deregister()   — remove from tracker
3. server.stop()            — close listening socket + thread pool
```

**`download(host, port, filename)`:** Delegates entirely to `DownloadService.download_file()`. `P2PNode` only provides the orchestration entry point.

---

## Message Protocol

All messages are JSON objects with a `"type"` field. The complete set of message types:

### Discovery messages (Node ↔ Tracker)

```json
// Register
{"type": "REGISTER", "port": 9000}
{"type": "ACK"}

// Peer discovery
{"type": "GET_PEERS"}
{"type": "PEER_LIST", "peers": [{"host": "1.2.3.4", "port": 9000}]}

// Deregister
{"type": "DEREGISTER", "port": 9000}
{"type": "ACK"}
```

### File listing messages (Node ↔ Node)

```json
{"type": "LIST_FILES"}
{"type": "FILE_LIST", "files": [{"filename": "doc.pdf", "size": 204800}]}
```

### File transfer messages (Node ↔ Node)

```json
// Request
{"type": "REQUEST_FILE", "filename": "doc.pdf"}

// Approval / rejection
{"type": "APPROVED", "filename": "doc.pdf", "size": 204800, "checksum": "abc123..."}
{"type": "REJECTED", "filename": "doc.pdf", "reason": "Rejected by user"}

// Chunked transfer
{"type": "FILE_CHUNK", "index": 0, "data": "<base64-encoded bytes>"}
{"type": "FILE_CHUNK", "index": 1, "data": "<base64-encoded bytes>"}
{"type": "TRANSFER_DONE", "filename": "doc.pdf"}
```

### Error messages

```json
{ "type": "ERROR", "reason": "Description of problem" }
```

---

## End-to-End Flows

### 1. Node Startup

```
P2PNode.start()
│
├─► _register_handlers()
│    └── server.register_handler("LIST_FILES",   file_service.handle_list_files)
│    └── server.register_handler("REQUEST_FILE", file_service.handle_file_request)
│
├─► server.start()
│    └── daemon thread: bind → listen → accept loop (ThreadPoolExecutor)
│    └── [Node is now reachable on self.port]
│
├─► discovery_service.register()
│    └── PeerClient → Tracker: REGISTER {port}
│    └── Tracker: ACK
│    └── [Node is now listed in tracker registry]
│
└─► discovery_service.refresh_peers()
     └── PeerClient → Tracker: GET_PEERS
     └── Tracker: PEER_LIST [{host, port}, ...]
     └── self.peers = [...]
     └── [Node now knows who else is online]
```

---

### 2. Discovering Peers

```
discovery_service.refresh_peers()
│
├─► PeerClient.connect(tracker_host, tracker_port)
├─► send: {"type": "GET_PEERS"}
├─► receive: {"type": "PEER_LIST", "peers": [...]}
└─► returns: [{"host": "...", "port": ...}, ...]
```

---

### 3. Listing Remote Files

```
file_service.list_remote_files(host, port)
│
├─► PeerClient.connect(host, port)
├─► send: {"type": "LIST_FILES"}
├─► receive: {"type": "FILE_LIST", "files": [...]}
└─► returns: [{"filename": "...", "size": ...}, ...]

                         Remote peer
                         server._handle_client()
                         └─► handler("LIST_FILES")
                              └─► file_service.handle_list_files()
                                   └─► get_files() → send FILE_LIST
```

---

### 4. Downloading a File

```
node.download(host, port, filename)
└─► download_service.download_file(host, port, filename)
     │
     ├─► PeerClient.connect(host, port)
     ├─► send: REQUEST_FILE {filename}
     │
     ├─► receive: APPROVED {size, checksum}   ← or REJECTED → abort
     │
     └─► _receive_file(client, filename, checksum)
          │
          ├─► loop: receive FILE_CHUNK {index, data}
          │    └── base64-decode → received_chunks[index]
          │
          └─► receive TRANSFER_DONE
               ├── sort chunks by index
               ├── verify no gaps
               ├── write assembled file to DOWNLOADS_DIR
               ├── compute SHA-256 of saved file
               └── compare vs expected_checksum
                    ├── match   → ✓ download complete
                    └── mismatch → delete file, log error
```

---

### 5. Serving a File Request

```
Incoming: REQUEST_FILE {filename}
└─► server routes to file_service.handle_file_request(conn, message)
     │
     ├── filename present? → ERROR if not
     ├── filename in shared files? → REJECTED if not
     ├── _safe_path() passes? → REJECTED if traversal
     ├── file exists on disk? → REJECTED if not
     │
     ├── push to approval_queue {filename, requester, event, result_box}
     │
     ├── [block: event.wait()]
     │         │
     │         ▼
     │   CLI thread reads approval_queue
     │   Prompts user: "Allow download of X by Y? [y/n]"
     │   Sets result_box[0] = "approved" or "rejected"
     │   Sets event
     │         │
     ├── [unblock]
     │
     ├── result == "approved":
     │    ├── compute SHA-256 checksum
     │    ├── send APPROVED {size, checksum}
     │    └── _send_file()
     │         ├── read file in CHUNK_SIZE blocks
     │         ├── base64-encode each block
     │         ├── send FILE_CHUNK {index, data} per block
     │         └── send TRANSFER_DONE
     │
     └── result == "rejected":
          └── send REJECTED {reason: "Rejected by user"}
```

---

### 6. Node Shutdown

```
node.stop()
│
├─► running = False
│
├─► discovery_service.deregister()
│    └── PeerClient → Tracker: DEREGISTER {port}
│    └── [Node removed from tracker registry]
│    └── (error here is caught and logged, not raised)
│
└─► server.stop()
     ├── server_socket.shutdown(SHUT_RDWR)  ← unblocks accept()
     ├── server_socket.close()
     └── pool.shutdown(wait=False)
```

---

## Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                          Node A (Requester)                         │
│                                                                     │
│  P2PNode                                                            │
│   └─ DownloadService                                                │
│        └─ PeerClient ─────────────────────────────────────────────►│
│                          TCP: REQUEST_FILE                          │
│◄──────────────────────────────────────────────────────────────────  │
│                          TCP: APPROVED {checksum}                   │
│◄──────────────────────────────────────────────────────────────────  │
│                          TCP: FILE_CHUNK x N                        │
│◄──────────────────────────────────────────────────────────────────  │
│                          TCP: TRANSFER_DONE                         │
│  verify SHA-256                                                     │
│  write to DOWNLOADS_DIR                                             │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│                          Node B (Sender)                            │
│                                                                     │
│  PeerServer                                                         │
│   └─ FileService.handle_file_request()                              │
│        ├─ validate path + existence                                 │
│        ├─ push to approval_queue                                    │
│        ├─ [wait for CLI thread]                                     │
│        ├─ compute SHA-256                                           │
│        └─ _send_file() → chunks from SHARED_DIR                    │
└─────────────────────────────────────────────────────────────────────┘

┌──────────────────────┐
│    Tracker Server    │  (involved only in registration/discovery,
│                      │   never in file transfers)
│  self.peers = set()  │
│  REGISTER   → add    │
│  DEREGISTER → remove │
│  GET_PEERS  → return │
└──────────────────────┘
```

---

## Directory Layout

```
project_root/
│
├── tracker.py                  # Run this first — standalone tracker process
│
├── utils/
│   ├── config.py               # All constants and directory setup
│   └── logger.py               # Structured colour logger
│
├── network/
│   ├── protocol.py             # Wire format: framing, serialisation, message constructors
│   ├── client.py               # PeerClient — outgoing TCP connections
│   └── server.py               # PeerServer — incoming TCP connections
│
├── services/
│   ├── discovery_service.py    # Tracker registration and peer discovery
│   ├── file_service.py         # File listing, serving, approval gate
│   └── download_service.py     # File requesting, chunk assembly, verification
│
├── node.py                     # P2PNode — top-level orchestrator
│
├── shared_files/               # ← put files here to share them (auto-created)
├── downloads/                  # ← received files land here (auto-created)
└── .tmp_transfers/             # ← partial downloads (auto-created)
```

---

## Configuration Reference

To customise the system, edit `utils/config.py`:

| What to change    | Constant           | Notes                                          |
| ----------------- | ------------------ | ---------------------------------------------- |
| Node listen port  | `DEFAULT_PORT`     | Must not conflict with Tracker port            |
| Tracker port      | `TRACKER_PORT`     | Must match `tracker.py` startup port           |
| Chunk size        | `CHUNK_SIZE`       | Larger = fewer messages, more memory per chunk |
| Socket timeout    | `SOCKET_TIMEOUT`   | Increase on slow networks                      |
| Transfer timeout  | `TRANSFER_TIMEOUT` | Time between chunks before abort               |
| Download location | `DOWNLOADS_DIR`    | Can be an absolute path                        |
| Shared folder     | `SHARED_DIR`       | Only files here are ever offered to peers      |

---

## Error Handling Strategy

The system uses a consistent, layered approach to errors:

| Layer              | Strategy                                                              |
| ------------------ | --------------------------------------------------------------------- |
| `protocol.py`      | Raises `ValueError` for bad messages; rejects payloads over 10 MB     |
| `PeerClient`       | Wraps socket errors as `ConnectionError`; marks `connected = False`   |
| `PeerServer`       | Catches handler exceptions per-connection; logs and continues serving |
| `DiscoveryService` | Catches all exceptions per method; logs and returns gracefully        |
| `FileService`      | Validates each precondition and sends a typed rejection message       |
| `DownloadService`  | Deletes corrupt files; logs specific failure reason at each stage     |
| `Tracker`          | Per-connection exception handling; accept loop continues on error     |

**The golden rule:** a failure in one peer connection must never crash the server or affect other connections.

---

## Security Considerations

**Path traversal prevention:** `FileService._safe_path()` resolves the full path and checks it starts within `SHARED_DIR`. A filename like `../../etc/passwd` is caught and rejected before any disk access.

**Explicit approval gate:** No file is ever sent without the local user typing an explicit confirmation. The network handler thread blocks on `event.wait()` until the CLI thread provides an answer.

**Checksum verification:** The sender computes SHA-256 before transfer and includes it in `APPROVED`. The receiver recomputes SHA-256 after writing to disk. A mismatch causes the file to be deleted immediately.

**Message size limit:** `recv_message()` rejects any payload over **10 MB** to prevent a malicious peer from exhausting memory by sending a crafted oversized message.

**No authentication:** There is currently no peer authentication or encryption. All communication is plaintext TCP. For production use, TLS and peer identity verification should be added.
