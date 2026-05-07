# Services

Business logic layer of the application. There are three services, each owning a distinct responsibility: peer discovery (`discovery.py`), receiving files (`download.py`), and serving/listing files (`file.py`).

---

## `discovery.py` — `DiscoveryService`

Handles the lifecycle of this peer's relationship with the tracker: registering on startup, keeping the local peer list fresh, and cleanly deregistering on shutdown.

### Constructor

```python
DiscoveryService(port: int, tracker_host: str, tracker_port: int)
```

| Parameter      | Description                                                                                  |
| -------------- | -------------------------------------------------------------------------------------------- |
| `port`         | The port this peer is listening on — sent to the tracker so other peers know how to reach it |
| `tracker_host` | Hostname or IP of the tracker server                                                         |
| `tracker_port` | Port the tracker is listening on                                                             |

Initializes an empty `self.peers` list, populated on the first `refresh_peers()` call.

### Methods

#### `register()`

Announces this peer to the tracker by sending a `REGISTER` message with the peer's port. Expects an `ACK` in response. Logs a warning if the tracker returns anything unexpected, and an error if it's unreachable. Safe to call at startup without crashing the application on failure.

#### `refresh_peers() -> list[dict]`

Sends `GET_PEERS` to the tracker and updates `self.peers` with the returned `PEER_LIST`. Returns the updated list directly so callers can use it immediately. Falls back gracefully on network errors — `self.peers` retains its previous value.

#### `deregister()`

Sends a `DEREGISTER` message to the tracker on shutdown. Fire-and-forget — no response is expected or waited on. Suppresses all exceptions so it never blocks or crashes the shutdown sequence.

### Tracker Protocol Summary

| Direction | Message type | Payload            |
| --------- | ------------ | ------------------ |
| → Tracker | `REGISTER`   | `{ port }`         |
| ← Tracker | `ACK`        | —                  |
| → Tracker | `GET_PEERS`  | —                  |
| ← Tracker | `PEER_LIST`  | `{ peers: [...] }` |
| → Tracker | `DEREGISTER` | `{ port }`         |

---

## `download.py` — `DownloadService`

Manages the full lifecycle of downloading a file from a remote peer: negotiating approval, receiving binary chunks, reassembling them in order, and verifying integrity via SHA-256 checksum.

### Constructor

```python
DownloadService()
```

Ensures `DOWNLOADS_DIR` exists, creating it if needed. No network connections are opened at construction time.

### Methods

#### `download_file(host: str, port: int, filename: str)`

Top-level entry point for downloading a file. Internally:

1. Opens a connection to the peer at `host:port`.
2. Sends a `REQUEST_FILE` message for `filename`.
3. Waits for `APPROVED` or `REJECTED`:
   - On `REJECTED` — logs the reason and returns.
   - On `APPROVED` — reads `size` and `checksum` from the metadata, then delegates to `_receive_file()`.
4. Aborts early if the approval message is missing a checksum (integrity cannot be guaranteed).

The connection is always closed in a `finally` block regardless of outcome.

#### `_receive_file(client, filename, expected_checksum)` _(internal)_

Loops over incoming messages until `TRANSFER_DONE`:

- `FILE_CHUNK` — base64-decodes the payload and stores it in a `dict[int, bytes]` keyed by chunk index. Malformed chunks (missing index or data) are skipped with a warning rather than crashing.
- `TRANSFER_DONE` — triggers reassembly: sorts chunks by index, checks for gaps in the sequence, writes the file, then verifies the SHA-256 checksum against `expected_checksum`.
  - If checksum fails, the corrupt file is deleted from disk.
  - Any unexpected message type mid-transfer is logged and skipped.

#### `_compute_checksum(file_path: Path) -> str` _(internal)_

Reads the file in `CHUNK_SIZE` blocks and returns its SHA-256 hex digest. Uses the same algorithm and chunk size as `FileService._compute_checksum` so the two sides produce identical hashes.

### Download Protocol Flow

```
Client                          Peer
  |                               |
  |── REQUEST_FILE ──────────────>|
  |<─ APPROVED (size, checksum) ──|
  |<─ FILE_CHUNK (index=0) ───────|
  |<─ FILE_CHUNK (index=1) ───────|
  |         ...                   |
  |<─ TRANSFER_DONE ──────────────|
  |                               |
```

---

## `file.py` — `FileService`

Server-side counterpart to `DownloadService`. Handles incoming connections from other peers: listing shared files and serving them after user approval. Also contains the client-side logic for querying a remote peer's file list.

### Constructor

```python
FileService()
```

Ensures `SHARED_DIR` exists and logs its resolved absolute path. Initializes `self.approval_queue` — a `queue.Queue` used to pass incoming file requests from the background network thread to the CLI thread for manual user approval.

### Methods

#### `get_files() -> list[dict]`

Scans `SHARED_DIR` and returns a list of metadata dicts for every file present:

```python
[{ "filename": "notes.txt", "size": 2048 }, ...]
```

#### `list_remote_files(host: str, port: int) -> list[dict]`

Client-side method. Connects to a peer, sends `LIST_FILES`, and returns the `files` array from the `FILE_LIST` response. Returns an empty list on any error so a single unreachable peer doesn't abort a broader listing sweep.

#### `handle_list_files(conn, message)` _(server-side)_

Responds to an incoming `LIST_FILES` message by calling `get_files()` and writing a `FILE_LIST` response back over `conn`.

#### `handle_file_request(conn, message)` _(server-side)_

The main gate for outbound file transfers. Runs through several validation steps before anything is sent:

1. Filename must be present in the message.
2. File must appear in `get_files()` (i.e. it's actually shared).
3. Path is resolved through `_safe_path()` to block directory traversal.
4. File must exist on disk at the resolved path.

If all checks pass, a request dict is placed on `self.approval_queue` and the handler **blocks** until the CLI thread signals a decision via a `threading.Event`. On approval, it computes the checksum, sends `APPROVED` with file metadata, and calls `_send_file()`. On rejection, it sends `REJECTED`.

#### `_send_file(conn, file_path: Path)` _(internal)_

Reads `file_path` in `CHUNK_SIZE` blocks, base64-encodes each one, and sends it as a `FILE_CHUNK` message with a sequential index. Sends `TRANSFER_DONE` after the last chunk.

#### `_safe_path(filename: str) -> Path` _(internal)_

Resolves `SHARED_DIR / filename` and confirms the result still lives inside `SHARED_DIR`. Raises `ValueError` on any traversal attempt (e.g. `../../etc/passwd`).

#### `_compute_checksum(file_path: Path) -> str` _(internal)_

SHA-256 over the full file, read in `CHUNK_SIZE` pieces. Identical implementation to `DownloadService._compute_checksum` — both must stay in sync.

### Approval Queue Contract

`handle_file_request` runs on a background **network thread**. User approval must happen on the **CLI thread**. The hand-off uses a shared dict placed on `self.approval_queue`:

```python
{
    "filename":     str,              # file being requested
    "requester":    str,              # requester's IP address
    "file_path":    Path,             # resolved path on disk
    "result_event": threading.Event,  # set by CLI thread when decision is made
    "result_box":   list,             # result_box[0] = "approved" | "rejected"
}
```

The network thread blocks on `result_event.wait()`. The CLI thread reads from the queue, prompts the user, writes `"approved"` or `"rejected"` into `result_box[0]`, then calls `result_event.set()`.

### File Serving Protocol Flow

```
Peer (requester)                This node
       |                              |
       |── REQUEST_FILE ─────────────>|
       |                    [user prompted for approval]
       |<─ APPROVED (size, checksum) ─|
       |<─ FILE_CHUNK (index=0) ──────|
       |<─ FILE_CHUNK (index=1) ──────|
       |          ...                 |
       |<─ TRANSFER_DONE ─────────────|
       |                              |
```
