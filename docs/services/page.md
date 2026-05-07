# Services

Business logic layer of the application. There are three services, each owning a distinct responsibility: peer discovery (`discovery.py`), receiving files (`download.py`), and serving/listing files (`file.py`).

---

## `discovery.py` — `DiscoveryService`

Handles the full lifecycle of this peer's relationship with the tracker: detecting the local LAN IP, registering on startup, keeping the registration alive via periodic heartbeats, filtering self from the peer list, and cleanly deregistering on shutdown.

### Module-Level Constant

| Constant             | Type    | Default | Description                                                                                                           |
| -------------------- | ------- | ------- | --------------------------------------------------------------------------------------------------------------------- |
| `HEARTBEAT_INTERVAL` | `float` | `30.0`  | Seconds between heartbeat messages to the tracker. Must stay below the tracker's `PEER_TTL` (90 s) to prevent expiry. |

### Module-Level Helper: `get_lan_ip() -> str`

Determines the machine's outbound LAN IP by opening a **UDP socket** toward `8.8.8.8:80` and reading the local address the OS selected for routing. No packets are actually sent — `connect()` on a UDP socket only sets routing metadata without a handshake. Falls back to `"127.0.0.1"` if no default route exists (e.g. the machine is offline). Called once in `__init__` and stored as `self.local_ip`.

### Constructor

```python
DiscoveryService(port: int, tracker_host: str, tracker_port: int)
```

| Parameter      | Description                                                                                  |
| -------------- | -------------------------------------------------------------------------------------------- |
| `port`         | The port this peer is listening on — sent to the tracker so other peers know how to reach it |
| `tracker_host` | Hostname or IP of the tracker server                                                         |
| `tracker_port` | Port the tracker is listening on                                                             |

**Initialised attributes:**

| Attribute                | Type                       | Description                                                                                                       |
| ------------------------ | -------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| `self.port`              | `int`                      | This peer's server port                                                                                           |
| `self.tracker_host`      | `str`                      | Tracker hostname or IP                                                                                            |
| `self.tracker_port`      | `int`                      | Tracker port                                                                                                      |
| `self.local_ip`          | `str`                      | LAN IP detected at startup via `get_lan_ip()`. Used to filter self from the tracker's peer list and for logging.  |
| `self.peers`             | `list[dict]`               | Cached peer list `[{"host": "...", "port": ...}, ...]`. Updated by `refresh_peers()`. Protected by `_peers_lock`. |
| `self._peers_lock`       | `threading.Lock`           | Guards all reads and writes to `self.peers`                                                                       |
| `self._heartbeat_thread` | `threading.Thread \| None` | Background daemon thread running `_heartbeat_loop()`. `None` until `register()` is called.                        |
| `self._running`          | `bool`                     | Lifecycle flag; controls the heartbeat loop. Set to `True` by `register()`, `False` by `deregister()`.            |

### Methods

#### `register()`

Announces this peer to the tracker and starts the background heartbeat loop. Two-step internally:

1. Calls `_send_register()` — sends a single `REGISTER` message and handles the response.
2. Sets `self._running = True` and starts `_heartbeat_thread` as a daemon thread named `"DiscoveryHeartbeat"`.

> **Behaviour change vs previous version:** `register()` now also starts the heartbeat loop. The old version only sent a single `REGISTER` and relied on the tracker to keep the entry indefinitely. The new tracker has a TTL, so the heartbeat is mandatory for staying registered.

#### `_send_register()` _(private)_

Sends a single `REGISTER` message to the tracker via a `PeerClient` with a 10 s timeout. Expects `ACK` in response; logs a warning on anything else. All exceptions are caught and logged as errors — the application continues even if the initial registration fails (the heartbeat loop will retry implicitly via `HEARTBEAT` auto-registration on the tracker side).

#### `_heartbeat_loop()` _(private, daemon thread)_

Runs continuously while `self._running` is `True`. On each iteration:

1. Sleeps `HEARTBEAT_INTERVAL` (30 s).
2. Checks `self._running` again — allows `deregister()` to stop the loop mid-sleep cleanly.
3. Opens a fresh `PeerClient` connection, sends `{"type": "HEARTBEAT", "port": self.port}`, and expects `ACK`.
4. On any exception, logs a warning with the retry interval and continues — a single missed heartbeat does not abort the loop.
5. `client.close()` is always called in a `finally` block.

> **New in this version:** The heartbeat loop did not exist in the previous implementation. It is essential because the tracker now expires peers after `PEER_TTL` (90 s). The 30 s interval provides a 3× safety margin.

#### `refresh_peers() -> list[dict]`

Fetches the current peer list from the tracker, **filters out this node**, updates `self.peers`, and returns a thread-safe snapshot.

**Self-filtering logic:** The tracker includes the calling peer in its own `PEER_LIST` response. Without filtering, the CLI would show the local node as a downloadable peer. The filter compares each entry's `host` and `port` against `self.local_ip` and `self.port` respectively. The number of removed entries is logged at DEBUG level.

**Thread safety:** `self.peers` is updated inside `with self._peers_lock`. Returns via `get_peers_safe()` (a locked copy) rather than the raw list.

**Error behaviour:** On any exception, logs an error and falls through to `return self.get_peers_safe()`, which returns the previous cached value unchanged.

#### `deregister()`

Stops the heartbeat loop and removes this node from the tracker.

1. Sets `self._running = False` — the heartbeat loop exits on its next wake.
2. Opens a `PeerClient`, sends `{"type": "DEREGISTER", "port": self.port}` (fire-and-forget, no response read).
3. On exception, logs a warning noting that the peer will expire via TTL instead — graceful degradation rather than a hard failure.
4. `client.close()` always called in `finally`.

#### `get_peers_safe() -> list[dict]`

Returns a shallow copy of `self.peers` under `_peers_lock`. Use this from any thread that needs a stable snapshot of the peer list.

### Tracker Protocol Summary

| Direction | Message type | Payload            | When sent                                                 |
| --------- | ------------ | ------------------ | --------------------------------------------------------- |
| → Tracker | `REGISTER`   | `{ port }`         | Once at startup via `_send_register()`                    |
| ← Tracker | `ACK`        | —                  | Expected response to `REGISTER`                           |
| → Tracker | `HEARTBEAT`  | `{ port }`         | Every `HEARTBEAT_INTERVAL` seconds by `_heartbeat_loop()` |
| ← Tracker | `ACK`        | —                  | Expected response to `HEARTBEAT`                          |
| → Tracker | `GET_PEERS`  | —                  | On each `refresh_peers()` call                            |
| ← Tracker | `PEER_LIST`  | `{ peers: [...] }` | Response to `GET_PEERS`; self-entry filtered out          |
| → Tracker | `DEREGISTER` | `{ port }`         | Once at shutdown via `deregister()`                       |

### Lifecycle Diagram

```
DiscoveryService()
    │
    ├── get_lan_ip()  →  self.local_ip
    │
    ▼
register()
    ├── _send_register()  →  REGISTER → Tracker → ACK
    └── spawn daemon thread → _heartbeat_loop()
             └── while _running:
                      sleep(30s) → HEARTBEAT → Tracker → ACK

                      (repeated every 30s until deregister())

refresh_peers()  [called by CLI / P2PNode on demand]
    └── GET_PEERS → Tracker → PEER_LIST
         └── filter out self (local_ip:port)
              └── update self.peers (under lock)

deregister()
    ├── self._running = False        ← stops heartbeat loop
    └── DEREGISTER → Tracker
```

---

## `download.py` — `DownloadService`

Manages the full lifecycle of downloading a file from a remote peer: negotiating approval, receiving binary chunks, reassembling them in order, verifying size and SHA-256 integrity, and cleaning up corrupt files.

### Module-Level Helper: `format_bytes(n: int) -> str`

Converts a byte count to a human-readable string (`"B"`, `"KB"`, `"MB"`, `"GB"`, `"TB"`). Used in progress and error log lines throughout the service. Shared with `FileService` (both define identical copies).

### Constructor

```python
DownloadService()
```

Ensures `DOWNLOADS_DIR` exists via `Path.mkdir(parents=True, exist_ok=True)`. No network connections at construction time.

### Methods

#### `download_file(host: str, port: int, filename: str)`

Top-level entry point. Opens a `PeerClient` with `timeout=TRANSFER_TIMEOUT` (360 s) — the long timeout is intentional because the remote user may take time to approve the request at their CLI.

**Steps:**

1. Connects to `host:port` and sends a `REQUEST_FILE` message (via `make_request_file(filename)`).
2. Calls `client.receive()` and branches on the response type:
   - `REJECTED` → logs the `reason` field and returns.
   - Not `APPROVED` → logs an error with the unexpected type and returns.
   - `APPROVED` → extracts `size` and `checksum` from the metadata.
3. Aborts immediately if `checksum` is absent — integrity cannot be guaranteed without it.
4. Delegates to `_receive_file()` with the approved metadata.
5. `client.close()` always called in `finally`.

**Exception handling:** `ConnectionError` is caught separately from generic `Exception` so it can emit a port-specific hint about firewall rules. Other exceptions are logged generically.

#### `_receive_file(client, filename, expected_size, expected_checksum)` _(private)_

Loops over incoming messages until `TRANSFER_DONE` or a connection error.

**Chunk accumulation:**

- `FILE_CHUNK` messages: extracts `index` (int) and `data` (base64 string). Missing either field → warning + `continue`. Decodes via `base64.b64decode()` and stores in `received_chunks: dict[int, bytes]`. Tracks `total_received` bytes and `last_chunk_index` for error context.
- Progress is logged at DEBUG level every 10 chunks (index % 10 == 0) and always on chunk 0. If `expected_size` is known, the log line includes a percentage.
- `ConnectionError` from `client.receive()` (covers both socket timeouts and dropped connections) is caught, logs the last received chunk index, and returns early.

**On `TRANSFER_DONE`:**

1. Checks `received_chunks` is non-empty.
2. **Gap detection:** builds `expected_indices = set(range(len(received_chunks)))` and compares with `actual_indices = set(received_chunks.keys())`. If they differ, logs the missing indices and returns without writing.
3. Writes chunks to `DOWNLOADS_DIR / filename` in sorted index order.
4. **Size verification (new):** reads `output_path.stat().st_size` and compares against `expected_size`. On mismatch, deletes the file and returns. The old version did not verify size.
5. **Checksum verification:** computes SHA-256 via `_compute_checksum()` and compares against `expected_checksum`. On mismatch, deletes the file and returns.
6. On success, logs `✓ Download complete and verified`.

**Outer exception handler:** any unhandled exception calls `output_path.unlink(missing_ok=True)` to avoid leaving a partial file on disk.

#### `_compute_checksum(file_path: Path) -> str` _(private)_

Reads `file_path` in `CHUNK_SIZE` blocks using `iter(lambda: f.read(CHUNK_SIZE), b"")` and returns the SHA-256 hex digest. Must stay byte-for-byte identical to `FileService._compute_checksum()` — both sides must produce the same hash for the same file content.

### Download Protocol Flow

```
Client (DownloadService)        Remote Peer (FileService)
  │                                       │
  │── REQUEST_FILE ──────────────────────>│
  │                             [user prompted for approval]
  │<─ APPROVED (size, checksum) ──────────│
  │<─ FILE_CHUNK (index=0) ───────────────│
  │<─ FILE_CHUNK (index=1) ───────────────│
  │            ...                        │
  │<─ TRANSFER_DONE ──────────────────────│
  │                                       │
  [gap check → write → size verify → checksum verify]
```

### Verification Steps on `TRANSFER_DONE`

```
received_chunks: dict[int, bytes]
    │
    ├── non-empty check
    ├── gap check: set(range(N)) == set(received_chunks.keys())
    ├── write sorted chunks to DOWNLOADS_DIR / filename
    ├── size check: file.stat().st_size == expected_size     ← new in this version
    └── checksum: sha256(file) == expected_checksum
         ├── pass → log ✓
         └── fail → unlink + return
```

---

## `file.py` — `FileService`

Server-side counterpart to `DownloadService`. Handles incoming connections from other peers: listing shared files and serving them after user approval. Also contains the client-side logic for querying a remote peer's file list.

### Module-Level Constants

| Constant           | Type    | Default | Description                                                                                                                                                                                                                     |
| ------------------ | ------- | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `APPROVAL_TIMEOUT` | `float` | `300.0` | Seconds to wait for the CLI user to approve or reject a file request before auto-rejecting. Designed to be less than `TRANSFER_TIMEOUT` (360 s) on the client side, so the client is still connected when the decision arrives. |

### Module-Level Helper: `format_bytes(n: int) -> str`

Identical to the one in `download.py`. Both must stay in sync if the logic changes.

### Constructor

```python
FileService()
```

Ensures `SHARED_DIR` exists and logs its resolved absolute path. Initialises `self.approval_queue`.

**Initialised attributes:**

| Attribute             | Type          | Description                                                                                                                            |
| --------------------- | ------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| `self.approval_queue` | `queue.Queue` | FIFO queue for pending file requests. Background network threads `put()` requests; the CLI thread `get()`s them and signals decisions. |

### Methods

#### `get_files() -> list[dict]`

Iterates `SHARED_DIR` with `Path.iterdir()`, filters to files only (`path.is_file()`), and returns a list of metadata dicts:

```python
[{"filename": "notes.txt", "size": 2048}, ...]
```

#### `_safe_path(filename: str) -> Path` _(private)_

Resolves `SHARED_DIR / filename` with `.resolve()` and confirms the result still starts with `str(SHARED_DIR.resolve())`. Raises `ValueError` on any traversal attempt (e.g. `../../etc/passwd`). Called by `handle_file_request()` before any disk access.

#### `_compute_checksum(file_path: Path) -> str` _(private)_

SHA-256 over the full file, read in `CHUNK_SIZE` pieces. Must stay identical to `DownloadService._compute_checksum()`.

#### `list_remote_files(host: str, port: int) -> list[dict]`

Client-side. Connects to a peer, sends `make_list_files()`, receives `FILE_LIST`, and returns the `files` array. Returns `[]` on any error (wrong response type, connection failure) so a single unreachable peer doesn't abort a broader sweep. `client.close()` always called in `finally`.

#### `handle_list_files(conn, message: dict)` _(server-side)_

Responds to an incoming `LIST_FILES` request. Extracts `_requester_ip` from the message (injected by the server dispatcher, not sent by the peer) for logging. Calls `get_files()` and writes `{"type": "FILE_LIST", "files": [...]}` back over `conn`. Exceptions during send are caught and logged.

#### `handle_file_request(conn, message: dict)` _(server-side)_

The main gate for outbound transfers. Runs on a background network thread. Blocks until the CLI thread signals a decision or `APPROVAL_TIMEOUT` expires.

**Validation sequence (all failures send `REJECTED` or `ERROR` and return):**

1. `filename` must be present in `message`, else sends `ERROR` with `"Filename missing"`.
2. `filename` must appear in `get_files()` (i.e. currently shared), else sends `REJECTED` with `"File not shared"`.
3. `_safe_path(filename)` must succeed without raising `ValueError`, else sends `REJECTED` with `"Invalid file path"`.
4. Resolved path must exist on disk (`file_path.exists()`), else sends `REJECTED` with `"File not found on disk"`.

**Approval hand-off:**

Constructs a request dict and puts it on `self.approval_queue`:

```python
{
    "filename":     str,              # file being requested
    "requester":    str,              # requester's IP (from message["_requester_ip"])
    "file_path":    Path,             # resolved absolute path
    "result_event": threading.Event,  # set by CLI thread when decision is made
    "result_box":   list,             # result_box[0] = "approved" | "rejected"
}
```

Then calls `result_event.wait(timeout=APPROVAL_TIMEOUT)`. The return value is `True` if the event was set within the timeout, `False` if it expired.

- **Timeout (False):** logs a warning and sends `REJECTED` with `"Approval timed out"`.
- **Rejected (`result_box[0] != "approved"`):** sends `REJECTED` with `"Rejected by user"`.
- **Approved:** computes checksum + file size, sends `APPROVED` with metadata, calls `_send_file()`.

#### `_send_file(conn, file_path: Path)` _(private)_

Reads `file_path` in `CHUNK_SIZE` blocks, base64-encodes each chunk via `base64.b64encode(...).decode("utf-8")`, and sends it as `make_chunk(chunk_index, encoded)` with a sequential zero-based index. Tracks `bytes_sent` for progress logging. After all chunks, sends `make_done(file_path.name)` and logs `✓ Transfer complete` with chunk count. Exceptions during transfer are caught and logged; the connection is closed by the server dispatcher after this method returns.

### Approval Queue Contract

`handle_file_request` runs on a **background network thread**. User approval must happen on the **CLI thread**. The hand-off mechanism:

```
Network thread                          CLI thread
──────────────────────────────────────────────────────────────
approval_queue.put({                    request = approval_queue.get()
  "filename":     ...,                  # prompt user
  "requester":    ...,
  "file_path":    ...,                  request["result_box"][0] = "approved"
  "result_event": Event,                request["result_event"].set()
  "result_box":   [None],
})
result_event.wait(APPROVAL_TIMEOUT)    ←─ unblocks here
```

The CLI thread must always call `result_event.set()` regardless of the decision, or the network thread will hang until `APPROVAL_TIMEOUT` fires.

### File Serving Protocol Flow

```
Peer (requester)                  This node (FileService)
       │                                    │
       │── REQUEST_FILE ───────────────────>│
       │                           [validation: shared? safe? exists?]
       │                           [approval_queue.put → CLI user prompt]
       │                           [result_event.wait(300s)]
       │<─ APPROVED (size, checksum) ────────│  (if approved)
       │<─ FILE_CHUNK (index=0) ─────────────│
       │<─ FILE_CHUNK (index=1) ─────────────│
       │            ...                      │
       │<─ TRANSFER_DONE ────────────────────│
       │                                     │
       OR
       │<─ REJECTED (reason) ────────────────│  (if rejected / timeout / validation fail)
```

### `handle_file_request` Decision Tree

```
REQUEST_FILE received
    │
    ├── filename missing?          → ERROR "Filename missing"
    ├── not in get_files()?        → REJECTED "File not shared"
    ├── _safe_path() raises?       → REJECTED "Invalid file path"
    ├── file_path.exists() False?  → REJECTED "File not found on disk"
    │
    └── approval_queue.put(request)
         └── result_event.wait(300s)
              ├── timed out         → REJECTED "Approval timed out"
              ├── result == rejected → REJECTED "Rejected by user"
              └── result == approved
                   ├── _compute_checksum()
                   ├── file_path.stat().st_size
                   ├── send_message(APPROVED)
                   └── _send_file()
```
