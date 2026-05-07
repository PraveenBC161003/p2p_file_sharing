# P2PNode — Component Documentation

## Overview

`P2PNode` is the top-level orchestrator for a peer-to-peer file-sharing node. It groups all subsystems — networking, file management, downloading, and peer discovery — into a single controllable object that represents one running participant in the P2P network.

**Module:** `node.py`  
**Logger name:** `Node`  
**Entry point:** Instantiated and driven by the CLI layer.

---

## Architecture

```
P2PNode
├── PeerServer           → Accepts inbound peer connections, dispatches to handlers
├── FileService          → Serves file listings and file data to requesting peers
├── DownloadService      → Connects outbound to remote peers and fetches files
└── DiscoveryService     → Registers with tracker, sends heartbeats, maintains peer list
```

The node follows a strict **register-handlers → start-server → register-with-tracker → discover-peers → populate-cache → start-auto-refresh** boot sequence. Handlers are always in place before the server accepts any connections; peer discovery happens only after the node is publicly reachable.

---

## Module-Level Constant

| Constant                | Type    | Default | Description                                                                                                                                                                           |
| ----------------------- | ------- | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `AUTO_REFRESH_INTERVAL` | `float` | `60.0`  | Seconds between automatic peer list and remote file cache refreshes in `_auto_refresh_loop`. Keeps the node current as peers join or leave without requiring manual CLI intervention. |

---

## Class: `P2PNode`

### Constructor

```python
P2PNode(port: int, tracker_host: str, tracker_port: int)
```

| Parameter      | Type  | Description                                             |
| -------------- | ----- | ------------------------------------------------------- |
| `port`         | `int` | Port this node listens on for incoming peer connections |
| `tracker_host` | `str` | Hostname or IP address of the tracker server            |
| `tracker_port` | `int` | Port number of the tracker server                       |

**Initialised attributes:**

| Attribute                  | Type                       | Description                                                                                                                                                                             |
| -------------------------- | -------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `self.port`                | `int`                      | Node's network identity                                                                                                                                                                 |
| `self.server`              | `PeerServer`               | Handles inbound connections on `port`                                                                                                                                                   |
| `self.file_service`        | `FileService`              | Data layer — lists and serves shared files                                                                                                                                              |
| `self.download_service`    | `DownloadService`          | Client-side file transfer engine                                                                                                                                                        |
| `self.discovery_service`   | `DiscoveryService`         | Tracker registration, heartbeat loop, and peer list management                                                                                                                          |
| `self.running`             | `bool`                     | Lifecycle flag; `False` until `start()` sets it, `False` again after `stop()`                                                                                                           |
| `self._remote_files_cache` | `dict[str, dict]`          | Keyed by `"host:port"`. Value structure: `{"peer_index": int, "host": str, "port": int, "files": [...]}`. Populated by `_refresh_remote_files()`, read by `get_remote_files_display()`. |
| `self._remote_files_lock`  | `threading.Lock`           | Guards all reads and writes to `_remote_files_cache`                                                                                                                                    |
| `self._refresh_thread`     | `threading.Thread \| None` | Background daemon thread running `_auto_refresh_loop()`. `None` until `start()` sets it.                                                                                                |

> **Remote file cache is new.** The old version had no `_remote_files_cache`. The node now proactively fetches and stores file listings from all known peers so the CLI can display them instantly without a live network query on each command.

---

### Methods

#### `start() → None`

Transitions the node from _configured_ to _actively participating_. Executes six steps in strict order:

1. **`_register_handlers()`** — wires `LIST_FILES` and `REQUEST_FILE` to their `FileService` handlers _before_ the server socket opens. No message can arrive without a handler in place.
2. **`server.start()`** — opens the TCP socket; the node becomes reachable to peers on the network.
3. **`discovery_service.register()`** — sends `REGISTER` to the tracker and starts the background heartbeat loop. The node is now discoverable.
4. **`time.sleep(0.5)` + `discovery_service.refresh_peers()`** — waits half a second to give the server time to finish binding, then fetches the current peer list. The sleep is intentional: if this is the first peer to start and another node is already running, the sleep ensures both sides are in a connectable state before the peer exchange happens.
5. **`self.running = True`** — marks the node as active.
6. **`_refresh_remote_files()`** — contacts all known peers concurrently and populates `_remote_files_cache`.
7. **Spawn `_auto_refresh_loop` daemon thread** (`name="NodeAutoRefresh"`) — starts the background loop that keeps peers and cached files current.

Logs the node's listening address and LAN IP (from `discovery_service.local_ip`) on success.

```python
node.start()
# [Node] Starting P2P Node...
# [Node] Handlers registered: LIST_FILES, REQUEST_FILE
# [Node] Initial peer count: 3
# [Node] Node started successfully
# [Node] Listening on 0.0.0.0:9000
# [Node] LAN IP: 192.168.1.5
```

---

#### `stop() → None`

Gracefully shuts down the node in order:

1. Sets `self.running = False` — signals `_auto_refresh_loop` to exit on its next iteration.
2. **`discovery_service.deregister()`** — sends `DEREGISTER` to the tracker and stops the heartbeat thread.
3. **`server.stop()`** — closes the listening socket, shuts down worker pool.

```python
node.stop()
# [Node] Stopping node...
# [Node] Node stopped
```

---

#### `refresh_peers() -> list[dict]` _(public)_

Manually triggers a full refresh cycle: re-fetches the peer list from the tracker via `discovery_service.refresh_peers()`, then immediately calls `_refresh_remote_files()` to rebuild the file cache from the updated peer set. Returns the new peer list.

Called by the CLI `peers` command. Also triggers the same cache rebuild that the auto-refresh loop does, so a manual refresh produces fully up-to-date state.

---

#### `_auto_refresh_loop()` _(private, daemon thread)_

Runs while `self.running` is `True`. On each iteration:

1. Sleeps `AUTO_REFRESH_INTERVAL` (60 s).
2. Checks `self.running` again — allows `stop()` to terminate the loop mid-sleep without waiting.
3. Calls `discovery_service.refresh_peers()` then `_refresh_remote_files()`.
4. Exceptions are caught and logged as warnings — a failed refresh does not kill the loop.

---

#### `_refresh_remote_files()` _(private)_

Rebuilds `_remote_files_cache` by contacting every peer in the current peer list concurrently.

**Steps:**

1. Calls `discovery_service.get_peers_safe()` for a thread-safe snapshot of the peer list.
2. If no peers, logs at DEBUG and returns early.
3. Clears `_remote_files_cache` under `_remote_files_lock` before re-populating — ensures stale entries from departed peers are removed.
4. Spawns one daemon thread per peer, each targeting `_fetch_peer_files(idx, peer)`. Concurrency keeps startup fast even when many peers are online.
5. Joins all threads with a `timeout=15` wall-clock limit per thread — slow or unreachable peers do not block the refresh indefinitely.
6. Logs the total file count and peer count from the freshly populated cache.

> **Why clear before repopulating:** A peer that went offline between refreshes would otherwise leave its old file entries in the cache. Clearing first and re-fetching guarantees the cache reflects only currently reachable peers.

---

#### `_fetch_peer_files(idx: int, peer: dict)` _(private, per-peer thread)_

Contacts a single peer and caches its file list. Runs in a daemon thread spawned by `_refresh_remote_files()`.

1. Extracts `host` and `port` from `peer`. Builds `peer_id = f"{host}:{port}"`.
2. Calls `file_service.list_remote_files(host, port)`.
3. Under `_remote_files_lock`, writes to `_remote_files_cache[peer_id]`:

```python
{
    "peer_index": idx,   # stable index for CLI display
    "host":       host,
    "port":       port,
    "files":      files, # list[dict] from FILE_LIST response
}
```

4. On any exception, logs a warning with `peer_id` and swallows the error — one unreachable peer does not abort the cache rebuild for all others.

---

#### `_register_handlers()` _(private)_

Wires message type strings to their `FileService` handler methods on `self.server`.

| Message Type   | Handler                            | Behaviour                                             |
| -------------- | ---------------------------------- | ----------------------------------------------------- |
| `LIST_FILES`   | `file_service.handle_list_files`   | Returns this node's shared file list to the requester |
| `REQUEST_FILE` | `file_service.handle_file_request` | Validates, prompts for approval, and streams the file |

Must be called before `server.start()`. The server's `_handle_client()` looks up handlers in a dict — an unregistered type is logged as a warning and ignored.

---

#### `get_peers_display() -> list[dict]` _(public, CLI helper)_

Returns a snapshot of the current peer list formatted for CLI display. Each entry includes a stable `index` (position in `get_peers_safe()` output), `host`, `port`, and a `"host:port"` `id` string.

```python
[
    {"index": 0, "host": "192.168.1.6", "port": 9001, "id": "192.168.1.6:9001"},
    {"index": 1, "host": "192.168.1.7", "port": 9002, "id": "192.168.1.7:9002"},
]
```

The `index` values here are the same indices used by `download()` and `get_peer_for_download()`. They are positional (derived from enumeration, not stored) and can change between calls if the peer list changes.

---

#### `get_remote_files_display() -> list[dict]` _(public, CLI helper)_

Returns all cached remote files flattened into a single list, formatted for CLI display. Acquires `_remote_files_lock` for the full read. Each entry:

```python
{
    "filename":  str,   # file name on the remote peer
    "size":      int,   # bytes
    "from_peer": int,   # peer_index — matches indices in get_peers_display()
    "host":      str,   # peer's host
    "port":      int,   # peer's port
}
```

The `from_peer` index can be passed directly to `download()` to fetch the file.

---

#### `get_peer_for_download(peer_index: int) -> dict | None`

Resolves a numeric peer index to a `{"index", "host", "port", "id"}` dict by calling `get_peers_display()` and indexing into the result. Returns `None` if `peer_index` is out of range.

#### `download(peer_index: int, filename: str)`

Resolves `peer_index` to a host+port via `get_peer_for_download()`. If the index is invalid, logs an error message with the valid range and returns. Otherwise delegates to `download_service.download_file(host, port, filename)`.

> **Signature change:** The old `download()` took `(host, port, filename)` directly. It now takes `(peer_index, filename)` so the CLI can use the same stable index system as `get_peers_display()` and `get_remote_files_display()`.

---

## Lifecycle Diagram

```
P2PNode()
    │
    ├── PeerServer(port=port)
    ├── FileService()
    ├── DownloadService()
    ├── DiscoveryService(port, tracker_host, tracker_port)
    ├── _remote_files_cache = {}
    ├── _remote_files_lock  = Lock()
    └── running = False
         │
         ▼
    start()
         ├── _register_handlers()          → LIST_FILES, REQUEST_FILE wired
         ├── server.start()                → TCP socket open, node reachable
         ├── discovery_service.register()  → REGISTER sent + heartbeat loop started
         ├── time.sleep(0.5)               → let server finish binding
         ├── discovery_service.refresh_peers() → peer list populated
         ├── self.running = True
         ├── _refresh_remote_files()       → _remote_files_cache populated (concurrent)
         └── spawn daemon "NodeAutoRefresh" → _auto_refresh_loop()
                  └── while running:
                           sleep(60s)
                           → refresh_peers()
                           → _refresh_remote_files()
         │
         ▼
    [Node operational]
    CLI commands drive:
         ├── refresh_peers()              → manual full refresh
         ├── get_peers_display()          → peer list for display
         ├── get_remote_files_display()   → cached file list for display
         └── download(peer_index, file)   → resolves peer → download_file()
         │
         ▼  (on shutdown)
    stop()
         ├── self.running = False         → stops auto-refresh loop
         ├── discovery_service.deregister()
         └── server.stop()
```

---

## Remote File Cache Structure

```
_remote_files_cache: dict[str, dict]

Key:   "192.168.1.6:9001"        ← "host:port" string

Value: {
    "peer_index": 0,             ← positional index in current peer list
    "host":       "192.168.1.6",
    "port":       9001,
    "files": [
        {"filename": "notes.txt", "size": 2048},
        {"filename": "data.csv",  "size": 1048576},
    ]
}
```

The cache is cleared entirely before each rebuild. Access to it (reads and writes) must always be done under `_remote_files_lock`.

---

## Concurrency Model

| Thread                    | Daemon | Lifecycle                                           | Shares                                 |
| ------------------------- | ------ | --------------------------------------------------- | -------------------------------------- |
| Main / CLI thread         | No     | Alive for the full process life                     | Calls all public methods               |
| `PeerServer` accept loop  | Yes    | From `server.start()` to `stop()`                   | —                                      |
| `PeerServer` pool workers | Yes    | Per inbound connection                              | `file_service` (thread-safe by design) |
| `DiscoveryHeartbeat`      | Yes    | From `register()` to `deregister()`                 | —                                      |
| `NodeAutoRefresh`         | Yes    | From `start()` to `stop()`                          | `_remote_files_cache` (via lock)       |
| Per-peer fetch threads    | Yes    | Short-lived, one per `_refresh_remote_files()` call | `_remote_files_cache` (via lock)       |

All mutations to `_remote_files_cache` go through `_remote_files_lock`. `discovery_service.peers` is similarly protected by its own lock inside `DiscoveryService`.

---

## Design Principles

**Separation of concerns** — `P2PNode` is purely an orchestration layer. It contains no file I/O, socket management, or protocol logic; those belong to the respective service and network classes.

**Boot order safety** — handlers registered before server starts; server starts before tracker registration; tracker registration before peer discovery. No step can expose a window where messages arrive without a ready handler.

**Cache-first CLI** — the remote file cache means CLI commands (`list`, `remote`) return instantly from memory rather than making synchronous network calls. The cache is always rebuilt after any peer list change (manual or auto).

**Fault isolation** — failures in `_fetch_peer_files()` and `_auto_refresh_loop()` are caught per-peer/per-cycle and logged as warnings. A single unreachable peer or a transient network error never propagates up to the CLI or kills the node.

---

## Dependencies

| Import                                        | Role                                                                  |
| --------------------------------------------- | --------------------------------------------------------------------- |
| `threading`                                   | `Lock` for cache, daemon threads (stdlib)                             |
| `time`                                        | `sleep(0.5)` in `start()`, `sleep(interval)` in auto-refresh (stdlib) |
| `utils.logger.get_logger`                     | Structured logger                                                     |
| `network.server.PeerServer`                   | Socket server and message routing                                     |
| `services.file_service.FileService`           | File listing and serving                                              |
| `services.download_service.DownloadService`   | Outgoing file downloads                                               |
| `services.discovery_service.DiscoveryService` | Tracker registration, heartbeat, and peer discovery                   |

---

## Example Usage

```python
from node import P2PNode

node = P2PNode(port=9000, tracker_host="192.168.1.1", tracker_port=5002)
node.start()

# Show available peers
for p in node.get_peers_display():
    print(f"[{p['index']}] {p['id']}")

# Show all cached remote files
for f in node.get_remote_files_display():
    print(f"  {f['filename']} ({f['size']} bytes) — peer [{f['from_peer']}]")

# Download by peer index
node.download(peer_index=0, filename="dataset.csv")

node.stop()
```
