# P2P File Sharing System — Complete Documentation

[Core concepts docs]("https://banyancloud-my.sharepoint.com/:w:/r/personal/a_praveen_banyancloud_io/_layouts/15/Doc.aspx?sourcedoc=%7B18FD80E4-D63B-4B42-96DF-5FD57BC8833A%7D&file=Document.docx&action=editNew&mobileredirect=true&wdOrigin=APPHOME-WEB.DIRECT%2CAPPHOME-WEB.BANNER.NEWBLANK&wdPreviousSession=6b8c05ce-0bf3-43e5-83df-83286b65bdef&wdPreviousSessionSrc=AppHomeWeb&ct=1778041032977")

## Table of Contents

- [P2P File Sharing System — Complete Documentation](#p2p-file-sharing-system--complete-documentation)
  - [Table of Contents](#table-of-contents)
  - [System Overview](#system-overview)
  - [Architecture](#architecture)
  - [Module Reference](#module-reference)
    - [`config.py`](#configpy)
    - [`logger.py`](#loggerpy)
    - [`protocol.py`](#protocolpy)
      - [`send_message(sock, message: dict)`](#send_messagesock-message-dict)
      - [`recv_message(sock) -> dict`](#recv_messagesock---dict)
      - [`recv_exact(sock, size: int) -> bytes` _(private)_](#recv_exactsock-size-int---bytes-private)
    - [`client.py` — `PeerClient`](#clientpy--peerclient)
    - [`server.py` — `PeerServer`](#serverpy--peerserver)
    - [`tracker.py` — `Tracker`](#trackerpy--tracker)
    - [`discovery_service.py` — `DiscoveryService`](#discovery_servicepy--discoveryservice)
    - [`file_service.py` — `FileService`](#file_servicepy--fileservice)
    - [`download_service.py` — `DownloadService`](#download_servicepy--downloadservice)
    - [`node.py` — `P2PNode`](#nodepy--p2pnode)
  - [Message Protocol](#message-protocol)
    - [Discovery (Node ↔ Tracker)](#discovery-node--tracker)
    - [File Listing (Node ↔ Node)](#file-listing-node--node)
    - [File Transfer (Node ↔ Node)](#file-transfer-node--node)
    - [Error](#error)
  - [End-to-End Flows](#end-to-end-flows)
    - [1. Node Startup](#1-node-startup)
    - [2. Heartbeat (background, every 30 s)](#2-heartbeat-background-every-30-s)
    - [3. Tracker TTL Expiry (background, every 30 s on Tracker)](#3-tracker-ttl-expiry-background-every-30-s-on-tracker)
    - [4. Listing Remote Files (CLI: `remote` command)](#4-listing-remote-files-cli-remote-command)
    - [5. Downloading a File](#5-downloading-a-file)
    - [6. Serving a File Request](#6-serving-a-file-request)
    - [7. Node Shutdown](#7-node-shutdown)
  - [Data Flow Diagram](#data-flow-diagram)
  - [Directory Layout](#directory-layout)
  - [Configuration Reference](#configuration-reference)
  - [Timeout Reference](#timeout-reference)
  - [Error Handling Strategy](#error-handling-strategy)
  - [Security Considerations](#security-considerations)

---

## System Overview

This is a **peer-to-peer file sharing system** built in Python. Every participant (a _node_) can both serve and request files simultaneously. A lightweight **Tracker** server acts as a directory — it keeps track of which peers are online, expires stale registrations via TTL, and hands out peer addresses on request, but never handles file data itself.

```
         ┌──────────────────────────────────────────────┐
         │              Tracker Server                   │
         │  • Peer registry: (ip, port) → last_seen ts  │
         │  • TTL-based expiry (90 s) + reaper thread    │
         │  • REGISTER / HEARTBEAT / GET_PEERS /         │
         │    DEREGISTER only — no file data             │
         └──────────────┬───────────────────────────────┘
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
- Peers must send a **heartbeat every 30 s** or be expired by the Tracker's reaper after 90 s.
- Every file transfer is **integrity-verified** via SHA-256 checksum and exact byte-size check.
- Serving a file requires **explicit user approval** — no file is sent without consent. Approval times out after 300 s and auto-rejects.
- All network messages are length-prefixed JSON over TCP (4-byte big-endian header, max 100 MB payload).

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

**Dependency rule:** each layer only imports from the same layer or below. Services import from Network and Utils; Network imports from Utils only. Application imports everything.

---

## Module Reference

---

### `config.py`

Central configuration. All other modules import constants from here — nothing is hardcoded elsewhere. Directories are created as a **side-effect of import** via `Path.mkdir(parents=True, exist_ok=True)`, so no explicit setup call is needed.

**`BASE_DIR` resolution:** `Path(__file__).resolve().parent.parent` — two levels above `config.py` itself, anchored at the project root regardless of the working directory.

**Directory paths:**

| Constant        | Resolved path                    | Purpose                                       | Auto-created |
| --------------- | -------------------------------- | --------------------------------------------- | ------------ |
| `BASE_DIR`      | `<project_root>/`                | Anchor for all relative paths                 | No           |
| `DOWNLOADS_DIR` | `<project_root>/downloads/`      | Where received files are saved                | Yes          |
| `SHARED_DIR`    | `<project_root>/shared_files/`   | Files this node makes available to peers      | Yes          |
| `TEMP_DIR`      | `<project_root>/.tmp_transfers/` | Partial downloads (reserved for resume logic) | Yes          |

**Network constants:**

| Constant         | Type    | Value  | Purpose                                      |
| ---------------- | ------- | ------ | -------------------------------------------- |
| `DEFAULT_PORT`   | `int`   | `5000` | Default listening port for peer servers      |
| `TRACKER_PORT`   | `int`   | `5002` | Default port for the Tracker                 |
| `BACKLOG`        | `int`   | `5`    | Max queued TCP connections (used by Tracker) |
| `SOCKET_TIMEOUT` | `float` | `30.0` | General idle socket timeout                  |

**Transfer constants:**

| Constant             | Type    | Value      | Purpose                                                                                                                                                                        |
| -------------------- | ------- | ---------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `CHUNK_SIZE`         | `int`   | `524288`   | 512 KB — file read/write block size and protocol chunk size. Both sides must use the same value for checksums to match.                                                        |
| `TRANSFER_TIMEOUT`   | `float` | `360.0`    | `PeerClient` socket timeout during downloads. Intentionally above `FileService.APPROVAL_TIMEOUT` (300 s) so the client stays connected while the server-side user is deciding. |
| `CHECKSUM_ALGORITHM` | `str`   | `"sha256"` | Algorithm used by both `FileService._compute_checksum()` and `DownloadService._compute_checksum()`. Both must match.                                                           |

**Protocol message type constants:** String constants matching the `"type"` field in every wire message. Grouped by function. Used as named imports in any code that compares message types, avoiding raw string literals.

```
Discovery:    MSG_REGISTER, MSG_DEREGISTER, MSG_GET_PEERS, MSG_PEER_LIST
              MSG_ACK (tracker → peer response to REGISTER/DEREGISTER/HEARTBEAT)
File listing: MSG_LIST_FILES, MSG_FILE_LIST
Transfer:     MSG_REQUEST_FILE, MSG_APPROVED, MSG_REJECTED,
              MSG_TRANSFER_START, MSG_TRANSFER_DONE, MSG_FILE_CHUNK
Generic:      MSG_ACK, MSG_ERROR
```

> `MSG_TRANSFER_START` is defined in config but not currently emitted by any module. It is reserved for a future streaming handshake step.

---

### `logger.py`

A lightweight structured logger with ANSI colour support. Every module obtains a named `Logger` instance via `get_logger(name)`. All state is module-level (`_COLOR_ENABLED`, `_DEBUG`) — there is no per-instance state beyond the name, so `enable_debug()` affects all existing and future instances immediately.

**Module-level state:**

| Variable         | Type   | Default                      | Description                                                  |
| ---------------- | ------ | ---------------------------- | ------------------------------------------------------------ |
| `_COLOR_ENABLED` | `bool` | Result of `supports_color()` | Set once at import time; never changes after that            |
| `_DEBUG`         | `bool` | `False`                      | Flipped to `True` by `enable_debug()`; global to all loggers |

**`supports_color()`:** Checks `hasattr(sys.stdout, "isatty") and sys.stdout.isatty()`. Returns `False` in pipes, CI systems, and redirected output — ANSI codes are stripped in those contexts.

**Output format:**

```
[HH:MM:SS] [ModuleName] [LEVEL] message text
```

**Log levels:**

| Method             | Label   | Colour  | ANSI       | Use case                                                  |
| ------------------ | ------- | ------- | ---------- | --------------------------------------------------------- |
| `log.info(msg)`    | `INFO`  | Cyan    | `\033[36m` | General lifecycle events                                  |
| `log.success(msg)` | `OK`    | Green   | `\033[32m` | Confirmations of completed operations                     |
| `log.warn(msg)`    | `WARN`  | Yellow  | `\033[33m` | Non-fatal problems, degraded state                        |
| `log.warning(msg)` | `WARN`  | Yellow  | `\033[33m` | Alias for `warn()` — both spellings work                  |
| `log.error(msg)`   | `ERROR` | Red     | `\033[31m` | Failures that stop an operation                           |
| `log.debug(msg)`   | `DEBUG` | Magenta | `\033[35m` | Verbose detail; suppressed unless `enable_debug()` called |

The timestamp `[HH:MM:SS]` is always dim (`\033[2m`); the logger name `[ModuleName]` is always blue (`\033[34m`).

---

### `protocol.py`

Handles all message serialisation, framing, and provides constructor helpers for every message type in the system. Every send and receive in the entire codebase goes through the two core functions here.

**Wire format:**

Every message is a UTF-8 JSON object, prefixed by a 4-byte big-endian unsigned integer (`struct` format `">I"`) that encodes the payload byte length:

```
┌──────────────────────┬──────────────────────────────────┐
│  4-byte header       │  N-byte JSON payload              │
│  struct.pack(">I",N) │  { "type": "...", ... }           │
└──────────────────────┴──────────────────────────────────┘
```

`_LENGTH_SIZE = struct.calcsize(">I") = 4`. Length-prefixed framing means the receiver knows exactly how many bytes to read — no delimiter scanning, no partial-read ambiguity across TCP packet boundaries.

**Size limit:** Both `send_message()` and `recv_message()` enforce a **100 MB cap**. Payloads over this are rejected before any socket write (send side) or before the payload read (receive side), preventing memory exhaustion from malformed or malicious messages.

**Core functions:**

#### `send_message(sock, message: dict)`

1. `json.dumps(message).encode("utf-8")` — raises `ValueError` on serialisation failure.
2. Checks `len(payload) > 100 MB` — raises `ValueError` if exceeded.
3. `struct.pack(">I", len(payload))` — 4-byte header.
4. `sock.sendall(header + payload)` — atomic write; OS handles partial sends internally.
5. Logs `type` and byte count at DEBUG.

Exception mapping (all become `ConnectionError`):

| Caught            | Raised as                                          |
| ----------------- | -------------------------------------------------- |
| `socket.timeout`  | `ConnectionError("Send timeout")`                  |
| `BrokenPipeError` | `ConnectionError("Connection lost (broken pipe)")` |
| Any other         | `ConnectionError("Failed to send message: ...")`   |

> `BrokenPipeError` occurs when the remote end closes the connection before the local side finishes writing (e.g. a peer rejects and immediately closes). Without catching it, it would propagate as an unhandled `OSError`.

#### `recv_message(sock) -> dict`

1. `recv_exact(sock, 4)` — reads the length header; raises `ConnectionError` on failure.
2. `struct.unpack(">I", raw_header)[0]` — raises `ValueError` on malformed header.
3. Rejects `payload_length > 100 MB` — raises `ValueError`.
4. Rejects `payload_length == 0` — raises `ValueError("Empty message payload")`. Zero-length reads on stream sockets behave unexpectedly and are never valid.
5. `recv_exact(sock, payload_length)` — reads the body.
6. `json.loads(raw_payload.decode("utf-8"))` — raises `ValueError` on invalid JSON.
7. Logs `type` and byte count at DEBUG.

#### `recv_exact(sock, size: int) -> bytes` _(private)_

Loops `sock.recv(size - len(data))` until exactly `size` bytes are accumulated. One `recv()` call on a stream socket is not guaranteed to return the full requested amount — this loop is mandatory. Error messages include `(got N/size bytes)` progress context for debugging partial reads.

**Message constructors** (all return plain dicts; never build these inline):

| Constructor                               | `"type"` value  | Other fields                    |
| ----------------------------------------- | --------------- | ------------------------------- |
| `make_list_files()`                       | `LIST_FILES`    | —                               |
| `make_file_list(files)`                   | `FILE_LIST`     | `files`                         |
| `make_request_file(filename)`             | `REQUEST_FILE`  | `filename`                      |
| `make_approved(filename, size, checksum)` | `APPROVED`      | `filename`, `size`, `checksum`  |
| `make_rejected(filename, reason)`         | `REJECTED`      | `filename`, `reason`            |
| `make_chunk(index, data)`                 | `FILE_CHUNK`    | `index`, `data` (base64 string) |
| `make_done(filename)`                     | `TRANSFER_DONE` | `filename`                      |

> `make_chunk` expects `data` to already be a base64-encoded string. Raw bytes cannot travel through JSON. Encoding is the caller's responsibility (`FileService._send_file` does `base64.b64encode(chunk).decode("utf-8")` before calling `make_chunk`).

---

### `client.py` — `PeerClient`

A stateful wrapper around a single outbound TCP socket. Used by `DownloadService`, `DiscoveryService`, and `FileService` for all outgoing connections. Each service method that needs to talk to a remote peer creates a fresh `PeerClient`, uses it, and closes it — connections are never reused across calls.

```python
PeerClient(host: str, port: int = 5000, timeout: float = 30)
```

`timeout` is applied to the socket via `settimeout()` and governs all operations on that socket (connect, send, receive). Callers that need different timeouts instantiate with an explicit value: `DiscoveryService` uses `timeout=10`; `DownloadService` uses `timeout=TRANSFER_TIMEOUT` (360 s).

**State machine:**

```
[created]  sock=None, connected=False
    │
    ▼ connect()
    ├── success → connected=True
    └── failure → raises ConnectionError (sock may be partially created)
         │
         ▼
    [connected]  connected=True
         │
         ├── send() / receive()
         │    ├── success → returns
         │    └── failure → connected=False, raises ConnectionError
         │                  [dead — _ensure_connected rejects further calls]
         └── close() → connected=False
                        [closed — safe to discard]
```

**`connect()`:** Creates socket, applies timeout, calls `sock.connect()`. Guards against double-connect. All OS errors are normalised to `ConnectionError`:

| Exception caught         | `ConnectionError` message                   |
| ------------------------ | ------------------------------------------- |
| `socket.timeout`         | `"Connection timeout to {host}:{port}"`     |
| `ConnectionRefusedError` | `"Connection refused by {host}:{port}"`     |
| `OSError`                | `"Failed to connect to {host}:{port}: {e}"` |

**`send(message)` / `receive()`:** Both call `_ensure_connected()` first, delegate to `protocol.send_message()` / `protocol.recv_message()`, and on any failure set `connected = False` before re-raising as `ConnectionError`. This `socket.timeout → ConnectionError` normalisation means all callers only need one `except ConnectionError` branch — it covers both timeouts and network failures.

**`_ensure_connected()`:** Checks both `self.connected` and `self.sock is not None`. Both checks are required — `connected` can be `False` even with a non-None `sock` if a prior send/receive failed.

**`send_and_receive(message)`:** `send()` then `receive()`. Used by `DiscoveryService` for all tracker interactions.

**`close()`:** Wraps `sock.close()` in `try/except`. Sets `connected = False`. Safe to call multiple times or before `connect()`.

---

### `server.py` — `PeerServer`

A multi-threaded TCP server for **inbound** peer connections. The accept loop runs on a background daemon thread; each accepted connection is dispatched to a `ThreadPoolExecutor` worker. The `PeerServer` never reads message semantics — it only routes by `"type"` field to registered handlers.

```python
PeerServer(host: str = "0.0.0.0", port: int = 5000, max_workers: int = 20)
```

> **Always bind on `"0.0.0.0"`** — binding on `"127.0.0.1"` accepts only loopback connections and makes the server invisible to other LAN devices.

**Module-level timeout constants:**

| Constant           | Value   | Applied to                                                                                                           |
| ------------------ | ------- | -------------------------------------------------------------------------------------------------------------------- |
| `RECV_TIMEOUT`     | `30.0`  | Reference constant (not directly applied; documents general intent)                                                  |
| `CONNECT_TIMEOUT`  | `15.0`  | Each new `conn` socket, set before reading the first message                                                         |
| `APPROVAL_TIMEOUT` | `300.0` | Re-applied to `conn` before dispatching `REQUEST_FILE` handlers; extends socket lifetime to cover user approval wait |

**Socket configuration in `_run()`:**

1. `SO_REUSEADDR` — allows immediate rebind after restart (bypasses OS `TIME_WAIT`).
2. `SO_REUSEPORT` (attempted) — allows multiple processes to share the port (useful in tests). Silently passes on `AttributeError` (Windows does not expose this).
3. `settimeout(1.0)` on server socket — `accept()` unblocks every second so `while self.running` can exit cleanly without blocking indefinitely.
4. Bind failure on `OSError` — logs a "check for port conflict" hint, sets `self.running = False`, returns early.

**Per-connection handling (`_handle_client`):**

Each worker runs one full connection lifecycle:

1. `conn.settimeout(CONNECT_TIMEOUT)` — 15 s to receive the first message.
2. `recv_message(conn)` — catches `socket.timeout` and general exceptions separately; both log a warning and return (fall through to `finally`).
3. Validates `isinstance(message, dict)` and `message.get("type")` is non-empty.
4. Injects `message["_requester_ip"] = ip` — the peer's IP from `addr[0]`, not from the message. Handlers use this field for logging without needing socket access.
5. Looks up handler; logs warning and returns if none registered.
6. **`REQUEST_FILE` special case:** `conn.settimeout(APPROVAL_TIMEOUT)` — re-extends the socket timeout to 300 s before dispatching, so the connection is not killed while the user is being prompted.
7. Calls `handler(conn, message)` inside `try/except Exception` — a crashing handler is logged but does not affect other connections.
8. `finally`: `conn.close()` (suppressed exception) + DEBUG log.

**`stop()`:** `self.running = False` → `server_socket.shutdown(SHUT_RDWR)` (unblocks `accept()`) → `server_socket.close()` → `pool.shutdown(wait=False)`. The two-step shutdown/close is wrapped in independent `try/except` blocks.

**Handler contract:**

```python
def my_handler(conn: socket.socket, message: dict) -> None:
    # message always has: {"type": str, "_requester_ip": str, ...}
    # "_requester_ip" is injected by _handle_client, NOT sent by the peer.
    # Write responses via send_message(conn, ...).
    # Do NOT close conn — _handle_client closes it in finally.
```

**Threading model:**

```
Main thread
  └── server.start()
        └── daemon thread "PeerServer": _run()
              ├── SO_REUSEADDR + SO_REUSEPORT, settimeout(1.0)
              ├── bind → listen(50)
              └── accept loop
                    └── pool worker: _handle_client(conn, addr)
                          ├── conn.settimeout(CONNECT_TIMEOUT=15s)
                          ├── recv_message()
                          ├── inject _requester_ip
                          ├── [if REQUEST_FILE] conn.settimeout(APPROVAL_TIMEOUT=300s)
                          ├── handler(conn, message)
                          └── conn.close()  ← always, via finally
                    (up to max_workers=20 concurrent workers)
```

---

### `tracker.py` — `Tracker`

The central peer registry. Accepts short-lived TCP connections, processes one request per connection, maintains a TTL-based dict of active peers, and runs a reaper thread that evicts stale entries.

```python
Tracker(host: str = "0.0.0.0", port: int = 5002)
```

`start()` is a **blocking call** — it runs the accept loop on the calling thread. Wrap in a `try/except KeyboardInterrupt` and call `stop()` for graceful shutdown.

**Module-level constant:**

| Constant   | Value  | Description                                                                                      |
| ---------- | ------ | ------------------------------------------------------------------------------------------------ |
| `PEER_TTL` | `90.0` | Seconds a peer stays registered without a heartbeat. Peers should send `HEARTBEAT` every ≤ 30 s. |

**Peer registry (`self.peers`):**

```python
self.peers: dict[tuple[str, int], float]
# Key:   (ip, port)    — the peer's server address (NOT the ephemeral connection port)
# Value: last_seen     — Unix timestamp (time.time()); updated on REGISTER and HEARTBEAT
```

All reads and writes to `self.peers` must be done inside `with self._peers_lock` (`threading.Lock`).

**Reaper thread (`_reap_dead_peers`):** Daemon thread started by `start()`. Wakes every `PEER_TTL / 3` seconds (30 s default), acquires the lock, and deletes every key whose `now - last_seen > PEER_TTL`. Logs each eviction at INFO level.

**`start()` behaviour:**

- Sets `self.running = True`, starts reaper daemon.
- Creates TCP socket with `SO_REUSEADDR` and `settimeout(1.0)` (allows clean shutdown polling).
- On bind `OSError`: logs error, sets `self.running = False`, returns early without entering accept loop.
- Each accepted connection is dispatched to a daemon thread targeting `_handle_client()`.

**`stop()` behaviour:** `self.running = False` → `server_socket.shutdown(SHUT_RDWR)` → `server_socket.close()`. Both steps individually try/except'd.

**`_handle_client(conn, addr)` — request routing:**

Sets `conn.settimeout(10.0)` before reading. The client's IP is always `addr[0]`; the registered port comes from the message payload.

| Message Type | Required | Behaviour                                                                                        | Response                                      |
| ------------ | -------- | ------------------------------------------------------------------------------------------------ | --------------------------------------------- |
| `REGISTER`   | `port`   | Acquires lock → upserts `(ip, port) → time.time()`; logs with total peer count                   | `{"type": "ACK"}`                             |
| `REGISTER`   | missing  | Logs warning; no state change                                                                    | `{"type": "ERROR", "reason": "Missing port"}` |
| `GET_PEERS`  | —        | Acquires lock → snapshots keys as `[{"host", "port"}]`; logs count at DEBUG                      | `{"type": "PEER_LIST", "peers": [...]}`       |
| `DEREGISTER` | `port`   | Acquires lock → deletes key if present (logs count); if absent, logs warning. Both return `ACK`. | `{"type": "ACK"}`                             |
| `HEARTBEAT`  | `port`   | Acquires lock → updates timestamp if present (DEBUG); if absent, inserts (auto-register, INFO)   | `{"type": "ACK"}`                             |
| _(unknown)_  | —        | Logs warning; no state change                                                                    | `{"type": "ERROR", "reason": "Unknown type"}` |

`conn.close()` is always called in a `finally` block.

---

### `discovery_service.py` — `DiscoveryService`

Manages this node's full lifecycle with the Tracker: detecting LAN IP, registering, keeping the registration alive via heartbeat, filtering self from the peer list, and deregistering on shutdown.

```python
DiscoveryService(port: int, tracker_host: str, tracker_port: int)
```

**Module-level constant:**

| Constant             | Value  | Description                                                                                           |
| -------------------- | ------ | ----------------------------------------------------------------------------------------------------- |
| `HEARTBEAT_INTERVAL` | `30.0` | Seconds between heartbeats. Must stay below `Tracker.PEER_TTL` (90 s). 30 s gives a 3× safety margin. |

**`get_lan_ip() -> str` (module-level helper):**

Opens a UDP socket and calls `connect("8.8.8.8", 80)`. No packets are sent — UDP `connect()` only sets routing metadata, causing the OS to select the correct outbound interface. Reads the local address with `getsockname()[0]`. Falls back to `"127.0.0.1"` on any exception (offline machine). Called once in `__init__`; result stored as `self.local_ip`.

**Key attributes:**

| Attribute                | Type                       | Description                                                                      |
| ------------------------ | -------------------------- | -------------------------------------------------------------------------------- |
| `self.local_ip`          | `str`                      | This node's LAN IP. Used to filter self from peer lists and for logging.         |
| `self.peers`             | `list[dict]`               | Cached peer list. Protected by `_peers_lock`. Updated only by `refresh_peers()`. |
| `self._peers_lock`       | `threading.Lock`           | Guards all reads/writes to `self.peers`.                                         |
| `self._heartbeat_thread` | `threading.Thread \| None` | Daemon thread running `_heartbeat_loop()`. `None` until `register()`.            |
| `self._running`          | `bool`                     | Controls `_heartbeat_loop()`. `True` from `register()` to `deregister()`.        |

**`register()`:** Calls `_send_register()` (single `REGISTER` → `ACK`), then starts the heartbeat daemon thread named `"DiscoveryHeartbeat"`.

**`_heartbeat_loop()`:** While `_running`: sleep 30 s → check `_running` again → open fresh `PeerClient` → send `HEARTBEAT {port}` → expect `ACK`. Exceptions logged as warnings; loop continues. `client.close()` always called in `finally`.

**`refresh_peers()`:**

1. Opens fresh `PeerClient` → `GET_PEERS` → `PEER_LIST`.
2. **Self-filter:** removes entries where `host == self.local_ip and port == self.port`. The Tracker includes the calling node in its own peer list; without this filter the CLI would show the local node as a downloadable peer.
3. Acquires `_peers_lock` → assigns `self.peers = filtered`.
4. Returns `get_peers_safe()` (a locked copy). On any exception: logs error, returns previous cached value unchanged.

**`deregister()`:** Sets `_running = False` (stops heartbeat loop) → opens fresh `PeerClient` → sends `DEREGISTER {port}` (fire-and-forget, no response read). On exception: logs warning noting TTL will expire the entry instead. Always closes client in `finally`.

**`get_peers_safe()`:** Returns `self.peers.copy()` under `_peers_lock`. Use this from any thread needing a stable snapshot.

---

### `file_service.py` — `FileService`

Server-side handler for file listing and file transfer requests, plus the client-side logic for querying remote peers' file lists. The most complex service — it manages the approval gate between the network thread and the CLI thread.

```python
FileService()   # creates SHARED_DIR if it doesn't exist; logs its resolved path
```

**Module-level constant:**

| Constant           | Value   | Description                                                                                                                                                                            |
| ------------------ | ------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `APPROVAL_TIMEOUT` | `300.0` | Seconds `handle_file_request()` blocks on `result_event.wait()`. Must be less than `TRANSFER_TIMEOUT` (360 s) so the requesting peer's socket is still open when the decision is made. |

**`self.approval_queue: queue.Queue`:** FIFO queue through which `handle_file_request()` (network thread) passes requests to the CLI thread for user decision. The CLI thread must always call `result_event.set()` regardless of the decision, or the handler thread will hang until `APPROVAL_TIMEOUT` fires.

**Local helpers:**

| Method                    | Description                                                                                                                                                         |
| ------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `get_files()`             | `SHARED_DIR.iterdir()` → filters to `is_file()` → returns `[{"filename": name, "size": bytes}, ...]`                                                                |
| `_safe_path(filename)`    | `(SHARED_DIR / filename).resolve()` — raises `ValueError` if result doesn't start with `SHARED_DIR.resolve()`. Blocks `../../etc/passwd` style traversal.           |
| `_compute_checksum(path)` | SHA-256 over full file in `CHUNK_SIZE` iterations using `iter(lambda: f.read(CHUNK_SIZE), b"")`. Must stay byte-identical to `DownloadService._compute_checksum()`. |

**`list_remote_files(host, port)`:** Client-side. `PeerClient` → `LIST_FILES` → expects `FILE_LIST` → returns `files` list. Returns `[]` on any error so a single unreachable peer doesn't abort a broader sweep.

**`handle_list_files(conn, message)`:** Server-side. Reads `message["_requester_ip"]` for logging. Calls `get_files()` and sends `{"type": "FILE_LIST", "files": [...]}`. Exceptions during send are caught and logged.

**`handle_file_request(conn, message)`:** Server-side approval gate. Runs on a network thread. Four validation checks before any blocking occurs:

```
1. filename in message?           → ERROR "Filename missing"
2. filename in get_files()?       → REJECTED "File not shared"
3. _safe_path() doesn't raise?    → REJECTED "Invalid file path"
4. file_path.exists()?            → REJECTED "File not found on disk"
```

Then places a request dict on `self.approval_queue` and blocks on `result_event.wait(APPROVAL_TIMEOUT)`:

```python
{
    "filename":     str,              # requested file name
    "requester":    str,              # from message["_requester_ip"]
    "file_path":    Path,             # resolved absolute path
    "result_event": threading.Event,  # CLI thread calls .set() when decided
    "result_box":   list,             # result_box[0] = "approved" | "rejected"
}
```

After unblocking:

- `wait()` returned `False` (timed out) → `REJECTED "Approval timed out"`
- `result_box[0] != "approved"` → `REJECTED "Rejected by user"`
- Approved → `_compute_checksum()` + `file_path.stat().st_size` → `send APPROVED` → `_send_file()`

**`_send_file(conn, file_path)`:** Reads `file_path` in `CHUNK_SIZE` blocks, `base64.b64encode(chunk).decode("utf-8")` each one, sends `make_chunk(chunk_index, encoded)` with sequential zero-based index. Logs progress at DEBUG with percentage. Sends `make_done(file_path.name)` after the last chunk. Exceptions are caught and logged; the connection is closed by `PeerServer._handle_client()` after this method returns.

---

### `download_service.py` — `DownloadService`

Client-side engine for requesting and receiving files from peers. Handles the full protocol from connection to verified file on disk.

```python
DownloadService()   # creates DOWNLOADS_DIR if it doesn't exist
```

**`format_bytes(n)`:** Module-level helper. Converts byte count to human-readable string (`B`, `KB`, `MB`, `GB`, `TB`). Identical copy exists in `file_service.py` — both must stay in sync.

**`download_file(host, port, filename)`:**

Opens `PeerClient(host, port, timeout=TRANSFER_TIMEOUT)` (360 s — must exceed server-side `APPROVAL_TIMEOUT` of 300 s so the socket stays alive during user approval).

Steps:

1. `client.connect()`
2. `client.send(make_request_file(filename))`
3. `client.receive()` — branches:
   - `REJECTED` → log reason, return
   - Not `APPROVED` → log unexpected type, return
   - `APPROVED` → extract `size` and `checksum`
4. Missing `checksum` → abort immediately (cannot verify integrity)
5. `_receive_file(client, filename, expected_size, expected_checksum)`
6. `client.close()` always in `finally`

`ConnectionError` is caught separately from generic `Exception` and emits a firewall hint.

**`_receive_file(client, filename, expected_size, expected_checksum)`:**

Accumulates chunks into `received_chunks: dict[int, bytes]` (index → bytes). Tracks `total_received` and `last_chunk_index` for progress and error context.

On `FILE_CHUNK`: validates `index` and `data` presence → `base64.b64decode(raw)` → stores in dict. Logs every 10 chunks (and chunk 0) with percentage if `expected_size` is known. `ConnectionError` from `client.receive()` (covers both `socket.timeout` and disconnect) catches the specific case with last-chunk-index context and returns.

On `TRANSFER_DONE`:

1. Non-empty check on `received_chunks`
2. Gap check: `set(range(len(received_chunks))) == set(received_chunks.keys())`
3. Write `sorted(received_chunks)` → `DOWNLOADS_DIR / filename` in index order
4. **Size verification:** `output_path.stat().st_size == expected_size` → delete + return on mismatch
5. **Checksum verification:** `_compute_checksum(output_path) == expected_checksum` → delete + return on mismatch
6. Log `✓ Download complete and verified`

Outer `except Exception` at the loop level calls `output_path.unlink(missing_ok=True)` to prevent partial files on disk.

**`_compute_checksum(file_path)`:** SHA-256 over file in `CHUNK_SIZE` iterations. Must stay byte-identical to `FileService._compute_checksum()`.

---

### `node.py` — `P2PNode`

Top-level orchestrator. Composes all subsystems into one controllable object. Contains no file I/O, socket, or protocol logic — only lifecycle and coordination.

```python
P2PNode(port: int, tracker_host: str, tracker_port: int)
```

**Module-level constant:**

| Constant                | Value  | Description                                                                           |
| ----------------------- | ------ | ------------------------------------------------------------------------------------- |
| `AUTO_REFRESH_INTERVAL` | `60.0` | Seconds between automatic peer list and file cache refreshes in `_auto_refresh_loop`. |

**Key attributes:**

| Attribute                  | Type                       | Description                                                                                                                        |
| -------------------------- | -------------------------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| `self.running`             | `bool`                     | `False` until `start()`, `False` again after `stop()`. Controls `_auto_refresh_loop`.                                              |
| `self._remote_files_cache` | `dict[str, dict]`          | Keys: `"host:port"`. Values: `{"peer_index": int, "host": str, "port": int, "files": list}`. Cleared and rebuilt on every refresh. |
| `self._remote_files_lock`  | `threading.Lock`           | Guards all reads/writes to `_remote_files_cache`.                                                                                  |
| `self._refresh_thread`     | `threading.Thread \| None` | Daemon thread running `_auto_refresh_loop()`. `None` until `start()`.                                                              |

**`start()` — 7-step boot sequence (order is strict):**

```
1. _register_handlers()              → handlers wired before server opens
2. server.start()                    → node is reachable
3. discovery_service.register()      → node is discoverable + heartbeat started
4. time.sleep(0.5)                   → let server finish binding (first-peer race condition)
5. discovery_service.refresh_peers() → peer list populated; self filtered out
6. self.running = True
7. _refresh_remote_files()           → file cache populated concurrently
8. spawn "NodeAutoRefresh" daemon    → periodic refresh started
```

The 0.5 s sleep at step 4 is intentional: if this is the first node to start and another node is already online, the sleep ensures both sides are in a fully bound state before the peer exchange.

**`stop()`:** `self.running = False` → `discovery_service.deregister()` → `server.stop()`.

**`_register_handlers()`:** Registers exactly two handlers on `self.server`:

| `"type"` value | Handler                            |
| -------------- | ---------------------------------- |
| `LIST_FILES`   | `file_service.handle_list_files`   |
| `REQUEST_FILE` | `file_service.handle_file_request` |

**`_refresh_remote_files()`:** Gets `get_peers_safe()` snapshot → clears `_remote_files_cache` under lock → spawns one daemon thread per peer targeting `_fetch_peer_files(idx, peer)` → joins all with `timeout=15`. Clears before repopulating so departed peers don't leave stale entries.

**`_fetch_peer_files(idx, peer)`:** Calls `file_service.list_remote_files(host, port)` → writes `{"peer_index": idx, "host": host, "port": port, "files": files}` to `_remote_files_cache[f"{host}:{port}"]` under lock. Exceptions are caught per-peer and logged as warnings.

**`_auto_refresh_loop()`:** While `self.running`: sleep 60 s → check `self.running` → `discovery_service.refresh_peers()` + `_refresh_remote_files()`. Exceptions caught and logged; loop continues.

**`refresh_peers()`** _(public)_: Calls `discovery_service.refresh_peers()` then `_refresh_remote_files()`. Returns the updated peer list. Used by CLI `peers` command.

**`get_peers_display()`:** Returns `[{"index": int, "host": str, "port": int, "id": "host:port"}, ...]` from `get_peers_safe()`. Indices are positional — they can shift between calls if the peer list changes.

**`get_remote_files_display()`:** Returns all cached files flattened: `[{"filename", "size", "from_peer", "host", "port"}, ...]`. `from_peer` matches `index` in `get_peers_display()` — the two methods are designed to be used together.

**`download(peer_index, filename)`:** Resolves `peer_index` via `get_peer_for_download()` → logs error with valid range if out of bounds → delegates to `download_service.download_file(host, port, filename)`.

> **Signature:** `download(peer_index: int, filename: str)` — not `(host, port, filename)`. The CLI always uses peer indices, not raw addresses.

---

## Message Protocol

All messages are JSON dicts with a `"type"` field, framed with a 4-byte length header.

### Discovery (Node ↔ Tracker)

```json
// Node announces itself
{"type": "REGISTER", "port": 9000}
{"type": "ACK"}

// Node keeps registration alive (every 30 s)
{"type": "HEARTBEAT", "port": 9000}
{"type": "ACK"}

// Node requests peer list
{"type": "GET_PEERS"}
{"type": "PEER_LIST", "peers": [{"host": "1.2.3.4", "port": 9000}, ...]}

// Node removes itself
{"type": "DEREGISTER", "port": 9000}
{"type": "ACK"}
```

### File Listing (Node ↔ Node)

```json
{"type": "LIST_FILES"}
{"type": "FILE_LIST", "files": [{"filename": "doc.pdf", "size": 204800}, ...]}
```

### File Transfer (Node ↔ Node)

```json
// Requester initiates
{"type": "REQUEST_FILE", "filename": "doc.pdf"}

// Server responds (after user approval)
{"type": "APPROVED", "filename": "doc.pdf", "size": 204800, "checksum": "abc123..."}
{"type": "REJECTED", "filename": "doc.pdf", "reason": "Rejected by user"}

// Server streams data
{"type": "FILE_CHUNK", "index": 0, "data": "<base64>"}
{"type": "FILE_CHUNK", "index": 1, "data": "<base64>"}
{"type": "TRANSFER_DONE", "filename": "doc.pdf"}
```

### Error

```json
{ "type": "ERROR", "reason": "Description" }
```

---

## End-to-End Flows

### 1. Node Startup

```
P2PNode.start()
│
├─► _register_handlers()
│    ├── server.register_handler("LIST_FILES",   file_service.handle_list_files)
│    └── server.register_handler("REQUEST_FILE", file_service.handle_file_request)
│
├─► server.start()
│    └── daemon "PeerServer": bind(0.0.0.0:port) → listen(50) → accept loop
│    [Node is now reachable]
│
├─► discovery_service.register()
│    ├── PeerClient → Tracker: REGISTER {port} → ACK
│    └── spawn daemon "DiscoveryHeartbeat": HEARTBEAT every 30 s
│    [Node is now in tracker registry]
│
├─► time.sleep(0.5)   ← first-peer binding race guard
│
├─► discovery_service.refresh_peers()
│    ├── PeerClient → Tracker: GET_PEERS → PEER_LIST
│    └── filters out self (local_ip:port)
│    [Node knows who else is online]
│
├─► self.running = True
│
├─► _refresh_remote_files()
│    └── per-peer daemon threads → list_remote_files() → _remote_files_cache
│    [File cache populated]
│
└─► spawn daemon "NodeAutoRefresh": refresh every 60 s
```

### 2. Heartbeat (background, every 30 s)

```
DiscoveryService._heartbeat_loop()
│
└─► PeerClient → Tracker: HEARTBEAT {port} → ACK
     └── Tracker: self.peers[(ip, port)] = time.time()   ← TTL refreshed
```

### 3. Tracker TTL Expiry (background, every 30 s on Tracker)

```
Tracker._reap_dead_peers()
│
└─► for each (ip, port) where now - last_seen > 90 s:
     └── del self.peers[(ip, port)]
          └── log "Expired peer: ip:port"
```

### 4. Listing Remote Files (CLI: `remote` command)

```
P2PNode.get_remote_files_display()
└─► returns _remote_files_cache (in-memory, no network call)
     [Cache built by _refresh_remote_files() at startup and every 60 s]

--- OR manual refresh ---

P2PNode.refresh_peers()
├─► discovery_service.refresh_peers()    → updated peer list
└─► _refresh_remote_files()
     └── per peer: file_service.list_remote_files(host, port)
          └─► PeerClient → LIST_FILES → FILE_LIST → cached
```

### 5. Downloading a File

```
P2PNode.download(peer_index=0, filename="doc.pdf")
│
├─► get_peer_for_download(0)  →  {"host": "192.168.1.6", "port": 9001}
└─► download_service.download_file("192.168.1.6", 9001, "doc.pdf")
     │
     ├─► PeerClient.connect(host, port, timeout=360s)
     ├─► send: REQUEST_FILE {filename}
     ├─► receive: APPROVED {size, checksum}   ← or REJECTED → abort
     └─► _receive_file(client, filename, size, checksum)
          │
          ├─► loop: receive FILE_CHUNK {index, data}
          │    └── base64-decode → received_chunks[index]
          │
          └─► receive TRANSFER_DONE
               ├── gap check: set(range(N)) == set(keys())
               ├── write sorted chunks → DOWNLOADS_DIR/doc.pdf
               ├── size check: stat().st_size == expected_size
               └── checksum: sha256(file) == expected_checksum
                    ├── match    → ✓ log success
                    └── mismatch → unlink + log error
```

### 6. Serving a File Request

```
Incoming: REQUEST_FILE {filename: "doc.pdf"}
└─► PeerServer._handle_client()
     ├── conn.settimeout(CONNECT_TIMEOUT=15s)  ← initial read
     ├── recv_message(conn)
     ├── inject _requester_ip
     ├── conn.settimeout(APPROVAL_TIMEOUT=300s)  ← extended for approval wait
     └── file_service.handle_file_request(conn, message)
          │
          ├── filename present?             → ERROR if not
          ├── filename in get_files()?      → REJECTED if not
          ├── _safe_path() safe?            → REJECTED if traversal
          ├── file_path.exists()?           → REJECTED if not
          │
          ├── approval_queue.put({filename, requester, event, result_box})
          ├── result_event.wait(300s)
          │         │
          │         ▼ CLI thread
          │   prompts user → sets result_box[0] → event.set()
          │         │
          ├── wait returned False?          → REJECTED "Approval timed out"
          ├── result_box[0] != "approved"?  → REJECTED "Rejected by user"
          └── approved:
               ├── _compute_checksum()
               ├── send: APPROVED {size, checksum}
               └── _send_file(conn, file_path)
                    ├── read CHUNK_SIZE blocks → base64 encode → FILE_CHUNK {index, data}
                    └── TRANSFER_DONE
```

### 7. Node Shutdown

```
P2PNode.stop()
│
├── self.running = False          ← stops _auto_refresh_loop
│
├─► discovery_service.deregister()
│    ├── self._running = False    ← stops _heartbeat_loop
│    └── PeerClient → Tracker: DEREGISTER {port}
│         └── on failure: warn "will expire via TTL"
│
└─► server.stop()
     ├── self.running = False     ← stops accept loop
     ├── server_socket.shutdown(SHUT_RDWR)
     ├── server_socket.close()
     └── pool.shutdown(wait=False)
```

---

## Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                     Node A (Requester)                              │
│                                                                     │
│  P2PNode.download(peer_index, filename)                             │
│   └─ DownloadService.download_file()                                │
│        └─ PeerClient ─────────────── TCP ──────────────────────────►│
│                          REQUEST_FILE {filename}                    │
│◄───────────────────────────────────────────────────────────────────  │
│                          APPROVED {size, checksum}                  │
│◄───────────────────────────────────────────────────────────────────  │
│                          FILE_CHUNK {index, data} × N               │
│◄───────────────────────────────────────────────────────────────────  │
│                          TRANSFER_DONE                              │
│  gap check → write → size check → sha256 verify                     │
│  → DOWNLOADS_DIR/filename                                           │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│                     Node B (Sender)                                 │
│                                                                     │
│  PeerServer._handle_client()                                        │
│   └─ conn.settimeout(APPROVAL_TIMEOUT=300s)                         │
│   └─ FileService.handle_file_request()                              │
│        ├─ validate (4 checks)                                       │
│        ├─ approval_queue.put(...)                                    │
│        ├─ result_event.wait(300s)  ←── CLI thread reads + prompts   │
│        ├─ _compute_checksum()                                       │
│        └─ _send_file()  →  CHUNK_SIZE reads → base64 → FILE_CHUNK  │
│            from SHARED_DIR/filename                                 │
└─────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────┐
│                  Tracker Server                       │
│                                                       │
│  self.peers: dict[(ip,port) → last_seen_ts]          │
│                                                       │
│  REGISTER / HEARTBEAT  →  upsert timestamp           │
│  DEREGISTER            →  delete key                 │
│  GET_PEERS             →  snapshot keys              │
│  _reap_dead_peers()    →  delete expired (TTL 90s)   │
│                                                       │
│  (never involved in file transfers)                  │
└──────────────────────────────────────────────────────┘
```

---

## Directory Layout

```
project_root/
│
├── tracker.py                  # Run standalone first — blocking process
│
├── utils/
│   ├── config.py               # All constants; auto-creates directories on import
│   └── logger.py               # Colour logger; module-level state
│
├── network/
│   ├── protocol.py             # Wire format: 4-byte header + JSON; message constructors
│   ├── client.py               # PeerClient — outgoing TCP; ConnectionError normalisation
│   └── server.py               # PeerServer — incoming TCP; handler dispatch; 2-phase timeout
│
├── services/
│   ├── discovery_service.py    # LAN IP detection; REGISTER + heartbeat + GET_PEERS
│   ├── file_service.py         # LIST_FILES + REQUEST_FILE approval gate + streaming
│   └── download_service.py     # REQUEST_FILE client + chunk assembly + size+sha256 verify
│
├── node.py                     # P2PNode — orchestrates all above; remote file cache
│
├── shared_files/               # ← put files here to share them (auto-created)
├── downloads/                  # ← received files land here (auto-created)
└── .tmp_transfers/             # ← reserved for partial downloads (auto-created)
```

---

## Configuration Reference

All values live in `utils/config.py`. To customise, edit that file — all modules read from it at import time.

| What to change      | Constant           | Notes                                                                                                        |
| ------------------- | ------------------ | ------------------------------------------------------------------------------------------------------------ |
| Node listen port    | `DEFAULT_PORT`     | Must not equal `TRACKER_PORT`                                                                                |
| Tracker port        | `TRACKER_PORT`     | Must match the port `tracker.py` is started with                                                             |
| Chunk size          | `CHUNK_SIZE`       | Both sender and receiver use this; changing it breaks checksum compatibility with running peers              |
| Download directory  | `DOWNLOADS_DIR`    | Reassign to any `Path`; auto-created on import                                                               |
| Shared directory    | `SHARED_DIR`       | Only files directly inside this folder are ever offered to peers                                             |
| Transfer timeout    | `TRANSFER_TIMEOUT` | Must exceed `FileService.APPROVAL_TIMEOUT` (300 s). Current value: 360 s.                                    |
| Socket idle timeout | `SOCKET_TIMEOUT`   | General-purpose timeout; individual timeouts in server.py and discovery_service.py override this per-context |

---

## Timeout Reference

All timeouts in one place, ordered from shortest to longest:

| Timeout                          | Value     | Where set                                  | Governs                                                             |
| -------------------------------- | --------- | ------------------------------------------ | ------------------------------------------------------------------- |
| `get_lan_ip()` UDP socket        | `2.0 s`   | `discovery_service.get_lan_ip()`           | LAN IP detection at startup                                         |
| Tracker server socket            | `1.0 s`   | `Tracker.start()`                          | `accept()` polling interval for clean shutdown                      |
| PeerServer server socket         | `1.0 s`   | `PeerServer._run()`                        | `accept()` polling interval for clean shutdown                      |
| Tracker client connections       | `10.0 s`  | `Tracker._handle_client()`                 | Time allowed to read one message per connection                     |
| Discovery/heartbeat PeerClient   | `10.0 s`  | `DiscoveryService._send_register()` et al. | All tracker interactions                                            |
| `list_remote_files()` PeerClient | `10.0 s`  | `FileService.list_remote_files()`          | Listing files from a remote peer                                    |
| `_fetch_peer_files()` join       | `15.0 s`  | `P2PNode._refresh_remote_files()`          | Wall-clock limit per per-peer fetch thread                          |
| `PeerServer.CONNECT_TIMEOUT`     | `15.0 s`  | `PeerServer._handle_client()`              | Initial socket timeout on each accepted connection                  |
| `PeerClient` default             | `30.0 s`  | `PeerClient.__init__()`                    | Default for callers that don't specify                              |
| `SOCKET_TIMEOUT`                 | `30.0 s`  | `utils/config.py`                          | Reference constant; individual contexts override as needed          |
| `AUTO_REFRESH_INTERVAL`          | `60.0 s`  | `P2PNode._auto_refresh_loop()`             | Between automatic peer+cache refreshes                              |
| `Tracker.PEER_TTL`               | `90.0 s`  | `tracker.py`                               | How long a peer can be silent before the reaper evicts it           |
| `FileService.APPROVAL_TIMEOUT`   | `300.0 s` | `file_service.py`                          | `result_event.wait()` — user approval window                        |
| `PeerServer.APPROVAL_TIMEOUT`    | `300.0 s` | `server.py`                                | Socket timeout re-applied before `REQUEST_FILE` dispatch            |
| `TRANSFER_TIMEOUT`               | `360.0 s` | `utils/config.py`                          | `PeerClient` timeout during file downloads; must exceed 300 s above |

---

## Error Handling Strategy

| Layer              | Strategy                                                                                                                                              |
| ------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| `protocol.py`      | `ValueError` for bad messages/oversized payloads; `ConnectionError` for socket failures                                                               |
| `PeerClient`       | Normalises all socket errors to `ConnectionError`; marks `connected = False` before re-raising                                                        |
| `PeerServer`       | Per-connection isolation: handler exceptions are caught and logged; accept loop continues                                                             |
| `Tracker`          | Per-connection exception handling; accept loop continues on error; reaper exceptions silently swallowed                                               |
| `DiscoveryService` | All methods catch exceptions individually; logs and returns gracefully; shutdown never raises                                                         |
| `FileService`      | Validates each precondition before touching state; sends typed rejection messages; approval timeout auto-rejects                                      |
| `DownloadService`  | Deletes partially written files on any error; `ConnectionError` and generic `Exception` caught separately; logs specific failure reason at each stage |
| `P2PNode`          | Per-peer fetch failures are isolated; auto-refresh errors logged as warnings; loop continues                                                          |

**The golden rule:** a failure in one peer connection must never crash the server or affect other connections. Every handler runs in an isolated thread worker.

---

## Security Considerations

**Path traversal prevention:** `FileService._safe_path()` resolves the full path and checks it begins with `SHARED_DIR.resolve()`. A filename like `../../etc/passwd` is caught and rejected before any disk access, with a `REJECTED "Invalid file path"` response to the requester.

**Explicit approval gate:** No file is ever sent without the local user typing an explicit confirmation at the CLI. The network handler thread blocks on `result_event.wait(APPROVAL_TIMEOUT)`. Auto-rejection after 300 s prevents the handler from waiting forever on an unattended terminal.

**Dual integrity verification:** The sender computes SHA-256 before transfer and includes it in `APPROVED`. The receiver re-computes SHA-256 after writing to disk. Additionally, the receiver checks the exact byte size against `expected_size`. Both must match or the file is deleted immediately.

**Message size cap:** `recv_message()` rejects any payload over **100 MB** to prevent a malicious peer from exhausting receiver memory with a crafted oversized message. The send side also enforces this cap before writing.

**Peer TTL expiry:** The Tracker automatically removes peers that stop sending heartbeats within 90 s. This prevents stale entries from persisting indefinitely if a node crashes without deregistering.

**No authentication or encryption:** All communication is plaintext TCP. There is no peer identity verification — any node that knows the tracker address can register. For production use, mutual TLS and peer identity verification should be added.
