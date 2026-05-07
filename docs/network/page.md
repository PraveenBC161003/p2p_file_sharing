# Network

Low-level networking layer of the application. Three files with clearly separated responsibilities: `client.py` for outbound TCP connections, `server.py` for inbound connections, and `protocol.py` for the wire format shared between both.

---

## `client.py` — `PeerClient`

Manages a single outbound TCP connection to a remote peer. Handles connection lifecycle, serialisation delegation, and state tracking. All higher-level services (`DiscoveryService`, `DownloadService`, `FileService`) use this as their transport.

### Constructor

```python
PeerClient(host: str, port: int = 5000, timeout: float = 30)
```

| Parameter | Type    | Default | Description                                                                                                                                                                    |
| --------- | ------- | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `host`    | `str`   | —       | IP address or hostname of the remote peer                                                                                                                                      |
| `port`    | `int`   | `5000`  | Port to connect to                                                                                                                                                             |
| `timeout` | `float` | `30.0`  | Socket-level timeout applied to all send/receive operations on this connection. Callers that need long waits (e.g. file transfers) pass `TRANSFER_TIMEOUT` (360 s) explicitly. |

> **Timeout default change:** The old default was `10.0 s`. It is now `30.0 s` to accommodate slower peers on congested LANs while still being short enough to surface genuine failures promptly. Callers always override this for specific use cases.

`self.sock` is initialised to `None` and `self.connected` to `False`. No network activity happens at construction time.

### Methods

#### `connect()`

Opens a TCP socket (`AF_INET`, `SOCK_STREAM`), applies `self.timeout` via `sock.settimeout()`, and calls `sock.connect((host, port))`. Sets `self.connected = True` on success.

If already connected, logs a warning and returns early — it never opens a second socket over an existing one.

All failures are caught and re-raised as `ConnectionError` with distinct messages per failure mode:

| Exception caught         | `ConnectionError` message raised            |
| ------------------------ | ------------------------------------------- |
| `socket.timeout`         | `"Connection timeout to {host}:{port}"`     |
| `ConnectionRefusedError` | `"Connection refused by {host}:{port}"`     |
| `OSError`                | `"Failed to connect to {host}:{port}: {e}"` |

This normalisation means all callers only need to catch `ConnectionError` regardless of the underlying OS error.

#### `send(message: dict)`

Calls `_ensure_connected()`, then delegates to `protocol.send_message(self.sock, message)`. Logs the message `type` at DEBUG level on success.

On failure, sets `self.connected = False` before re-raising as `ConnectionError`. This marks the socket as dead so subsequent calls to `_ensure_connected()` will fail fast rather than attempting to write to a broken socket.

| Exception caught | Action                                                                 |
| ---------------- | ---------------------------------------------------------------------- |
| `socket.timeout` | `self.connected = False` → raise `ConnectionError("Send timeout")`     |
| Any other        | `self.connected = False` → raise `ConnectionError("Send failed: ...")` |

#### `receive() -> dict`

Calls `_ensure_connected()`, then delegates to `protocol.recv_message(self.sock)`. Mirrors `send()` exactly in error handling — marks connection dead and raises `ConnectionError` on any failure.

| Exception caught | Action                                                                         |
| ---------------- | ------------------------------------------------------------------------------ |
| `socket.timeout` | `self.connected = False` → raise `ConnectionError("Receive timeout from ...")` |
| Any other        | `self.connected = False` → raise `ConnectionError("Receive failed: ...")`      |

> Both `send()` and `receive()` convert `socket.timeout` into `ConnectionError`. This means callers — including `DownloadService._receive_file()` — only need a single `except ConnectionError` branch to handle both timeouts and disconnections.

#### `send_and_receive(message: dict) -> dict`

Convenience wrapper: calls `send(message)` then `receive()` and returns the result. Used by `DiscoveryService` for all tracker interactions (register, heartbeat, get peers).

#### `close()`

Calls `sock.close()` inside a `try/except` (close is best-effort and never raises to the caller). Sets `self.connected = False`. Safe to call multiple times or before `connect()` — `if self.sock` guards against a `None` socket.

#### `_ensure_connected()` _(private)_

Guard called at the top of `send()` and `receive()`. Checks both `self.connected` and `self.sock is not None`. Raises `RuntimeError` if either condition fails, preventing silent use of a closed or never-opened socket.

### State Machine

```
[created]
  self.sock = None
  self.connected = False
      │
      ▼ connect()
[connected]
  self.connected = True
      │
      ├── send() / receive() raises  →  self.connected = False
      │                                  [dead — _ensure_connected will reject further calls]
      │
      └── close()  →  self.connected = False
                      [closed — safe to reconstruct]
```

---

## `protocol.py`

Defines the wire format for all peer-to-peer communication and provides message constructor helpers. Every send and receive in the system goes through here — `PeerClient`, `PeerServer`, and `FileService` all import from this module.

### Module-Level Constants

| Constant         | Value  | Description                                                          |
| ---------------- | ------ | -------------------------------------------------------------------- |
| `_LENGTH_FORMAT` | `">I"` | `struct` format string: big-endian (`>`) unsigned int (`I`)          |
| `_LENGTH_SIZE`   | `4`    | Byte size of the packed header, derived from `struct.calcsize(">I")` |

Both are private (underscore-prefixed) and used only inside this module.

### Wire Format

Messages are framed with a **4-byte big-endian unsigned integer header** encoding the payload length, followed by the UTF-8 JSON body.

```
┌──────────────────────┬──────────────────────────────────┐
│  4-byte header       │  N-byte JSON payload              │
│  struct.pack(">I", N)│  { "type": "...", ... }           │
└──────────────────────┴──────────────────────────────────┘
```

Length-prefixed framing guarantees the receiver knows exactly how many bytes to read, avoiding partial reads across TCP packet boundaries and eliminating the need for delimiter scanning.

> **Size limit change:** The old implementation rejected payloads over **10 MB**. Both `send_message()` and `recv_message()` now enforce a **100 MB** cap. The increase accommodates large `FILE_LIST` responses from peers with many shared files while still bounding memory allocation from malformed messages.

### Functions

#### `send_message(sock, message: dict)`

1. Serialises `message` to a UTF-8 JSON byte string. Raises `ValueError` on serialisation failure.
2. Checks the payload is ≤ 100 MB. Raises `ValueError("Message payload too large (>100MB)")` if not.
3. Packs `len(payload)` as a 4-byte big-endian header using `struct.pack(_LENGTH_FORMAT, ...)`.
4. Calls `sock.sendall(header + payload)`. `sendall` loops internally until the full buffer is written, handling OS-level partial writes transparently.
5. Logs `type` and byte count at DEBUG level.

Exception mapping (all re-raised as `ConnectionError`):

| Exception caught  | `ConnectionError` message         |
| ----------------- | --------------------------------- |
| `socket.timeout`  | `"Send timeout"`                  |
| `BrokenPipeError` | `"Connection lost (broken pipe)"` |
| Any other         | `"Failed to send message: {e}"`   |

> **`BrokenPipeError` handling is new.** The old version did not handle this explicitly. It arises when the remote end closes the connection before the local side finishes writing (e.g. the peer rejects a request and closes immediately). Without this catch it would propagate as an unhandled `OSError`.

#### `recv_message(sock) -> dict`

1. Calls `recv_exact(sock, _LENGTH_SIZE)` to read exactly 4 header bytes. Raises `ConnectionError` on failure.
2. Unpacks payload length with `struct.unpack(_LENGTH_FORMAT, raw_header)[0]`. Raises `ValueError` on malformed header.
3. Rejects `payload_length > 100 MB`. Raises `ValueError`.
4. Rejects `payload_length == 0`. Raises `ValueError("Empty message payload")`. This is **new** — the old implementation would have attempted a zero-byte read.
5. Calls `recv_exact(sock, payload_length)` for the body.
6. Decodes UTF-8 and parses JSON. Raises `ValueError` on invalid JSON.
7. Logs `type` and byte count at DEBUG level.

#### `recv_exact(sock, size: int) -> bytes` _(private)_

Accumulates exactly `size` bytes from the socket by looping `sock.recv(size - len(data))` until the buffer is full. Each call to `recv()` may return fewer bytes than requested — this loop is the correct way to read a fixed-size payload from a stream socket.

Exception handling per iteration:

| Condition            | Raised as                                                          |
| -------------------- | ------------------------------------------------------------------ |
| `socket.timeout`     | `ConnectionError(f"Timeout receiving data (got N/size bytes)")`    |
| Other `Exception`    | `ConnectionError(f"Error receiving data: {e}")`                    |
| `chunk == b""` (EOF) | `ConnectionError(f"Connection closed by peer (got N/size bytes)")` |

The progress context `(got N/size bytes)` in all error messages helps diagnose partial reads during debugging.

### Message Constructors

All outgoing messages are built via these helpers. Direct inline dict construction is avoided so the wire format stays consistent and field changes only need to happen in one place.

| Constructor                               | `type` field    | Additional fields               |
| ----------------------------------------- | --------------- | ------------------------------- |
| `make_list_files()`                       | `LIST_FILES`    | —                               |
| `make_file_list(files)`                   | `FILE_LIST`     | `files`                         |
| `make_request_file(filename)`             | `REQUEST_FILE`  | `filename`                      |
| `make_approved(filename, size, checksum)` | `APPROVED`      | `filename`, `size`, `checksum`  |
| `make_rejected(filename, reason)`         | `REJECTED`      | `filename`, `reason`            |
| `make_chunk(index, data)`                 | `FILE_CHUNK`    | `index`, `data` (base64 string) |
| `make_done(filename)`                     | `TRANSFER_DONE` | `filename`                      |

> `make_chunk` expects `data` to already be a base64-encoded string. Raw bytes cannot travel through a JSON payload — encoding is the caller's responsibility (`FileService._send_file` handles this before calling `make_chunk`).

---

## `server.py` — `PeerServer`

Listens for inbound TCP connections and dispatches each message to a registered handler based on its `type` field. Runs the accept loop on a background daemon thread and handles each client connection in a `ThreadPoolExecutor` worker.

### Module-Level Constants

| Constant           | Type    | Default | Description                                                                                                                                                                                                                             |
| ------------------ | ------- | ------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `RECV_TIMEOUT`     | `float` | `30.0`  | General socket timeout for data reads. Referenced in comments; the actual socket timeout applied per-connection is `CONNECT_TIMEOUT` initially, then `APPROVAL_TIMEOUT` for `REQUEST_FILE`.                                             |
| `CONNECT_TIMEOUT`  | `float` | `15.0`  | Timeout applied to each new connection's socket while waiting for the first message. Long enough for slow peers on a congested LAN.                                                                                                     |
| `APPROVAL_TIMEOUT` | `float` | `300.0` | Timeout applied to `REQUEST_FILE` connections after the initial message is received. The connection socket is re-timed with this value so the handler can wait for user approval without being killed by the shorter `CONNECT_TIMEOUT`. |

> **These are server-side connection timeouts**, distinct from `FileService.APPROVAL_TIMEOUT` (which governs `result_event.wait()`). They must be at least as large as the application-level approval timeout to avoid the socket closing while the handler is still waiting for user input.

### Constructor

```python
PeerServer(host: str = "0.0.0.0", port: int = 5000, max_workers: int = 20)
```

| Parameter     | Type  | Default     | Description                                                                                                                                                                               |
| ------------- | ----- | ----------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `host`        | `str` | `"0.0.0.0"` | Interface to bind on. Always use `"0.0.0.0"` to accept on all interfaces (LAN, WiFi, loopback). Binding on `"127.0.0.1"` would make the server invisible to other devices on the network. |
| `port`        | `int` | `5000`      | Port to listen on                                                                                                                                                                         |
| `max_workers` | `int` | `20`        | Max concurrent client connections in the `ThreadPoolExecutor`. Previously `10`; doubled to handle more simultaneous peers.                                                                |

**Initialised attributes:**

| Attribute            | Type                      | Description                                                 |
| -------------------- | ------------------------- | ----------------------------------------------------------- |
| `self.host`          | `str`                     | Bound interface                                             |
| `self.port`          | `int`                     | Bound port                                                  |
| `self.handlers`      | `Dict[str, Handler]`      | Maps message type strings to handler callables              |
| `self.running`       | `bool`                    | Lifecycle flag for the accept loop; `False` until `start()` |
| `self.server_socket` | `Optional[socket.socket]` | `None` until `_run()` creates it                            |
| `self.pool`          | `ThreadPoolExecutor`      | Worker pool; created at construction with `max_workers`     |

### Methods

#### `register_handler(msg_type: str, handler: Handler)`

Inserts `handler` into `self.handlers[msg_type]`. Logs the registration at DEBUG level. Call this before `start()`. The `Handler` type alias is `Callable[[socket.socket, dict], None]`.

```python
server.register_handler("REQUEST_FILE", file_service.handle_file_request)
server.register_handler("LIST_FILES",   file_service.handle_list_files)
```

#### `start()`

Sets `self.running = True`, spawns `_run` as a **named daemon thread** (`name="PeerServer"`), and returns immediately — the caller is not blocked. Daemon threads are automatically killed when the main process exits.

#### `_run()` _(private, daemon thread)_

Creates and configures the server socket:

1. Sets `SO_REUSEADDR` — allows immediate rebind after restart, bypassing the OS `TIME_WAIT` state.
2. Attempts `SO_REUSEPORT` — allows multiple processes to share the same port (useful in testing). Silently passes on `AttributeError` because Windows does not expose this option.
3. Sets a `1.0 s` accept timeout so the loop can poll `self.running` and exit cleanly without blocking indefinitely on `accept()`.
4. Calls `bind((self.host, self.port))`. On `OSError`, logs a "check for port conflict" hint, sets `self.running = False`, and returns — the caller sees the server never started via logs.
5. Calls `listen(50)`.

Accept loop:

- `socket.timeout` → `continue` (normal; re-checks `self.running`)
- Successful `accept()` → logs the connection, submits `_handle_client(conn, addr)` to the pool
- Other exceptions → logged only if `self.running` is still `True` (avoids noise during `stop()`)

> **`SO_REUSEPORT` is new.** The old version only set `SO_REUSEADDR`. The addition improves test ergonomics (multiple test processes sharing a port) and is a no-op on production deployments.

#### `_handle_client(conn, addr)` _(private, pool worker)_

Handles one peer connection end-to-end. Each connection carries **exactly one request** — after the handler returns the socket is closed. The exception is `REQUEST_FILE`, whose handler holds the socket open for the duration of the file stream (this is intentional and documented in the method's docstring).

**Step-by-step:**

1. Sets `conn.settimeout(CONNECT_TIMEOUT)` (15 s) — time allowed to receive the first message.
2. Calls `recv_message(conn)`. On `socket.timeout` → logs warning, returns. On other exception → logs warning, returns. Both paths fall through to `finally`.
3. Validates the message is a `dict` and has a non-empty `type` field. Logs and returns on either failure.
4. Injects `message["_requester_ip"] = ip` so handlers can display the requester's address without direct socket access.
5. Looks up `self.handlers.get(msg_type)`. If no handler is registered, logs a warning and returns.
6. **`REQUEST_FILE` special case:** re-calls `conn.settimeout(APPROVAL_TIMEOUT)` (300 s) before dispatching. This extends the socket lifetime to match the application-level approval wait. Any other message type retains the `CONNECT_TIMEOUT` socket timeout.
7. Calls `handler(conn, message)` inside a `try/except Exception` — a crashing handler is logged but does not kill other connections or the server.
8. `finally`: calls `conn.close()` (suppressing exceptions) and logs the connection as closed at DEBUG level.

> **Per-connection timeout differentiation is new.** The old version set one timeout globally and never adjusted it per message type. The two-phase approach (`CONNECT_TIMEOUT` for setup, `APPROVAL_TIMEOUT` for `REQUEST_FILE`) prevents the socket from being killed while a user is deciding whether to approve a transfer.

#### `stop()`

1. Sets `self.running = False` — signals `_run()` to exit on its next 1 s poll.
2. Calls `server_socket.shutdown(SHUT_RDWR)` — forcibly unblocks any `accept()` in progress.
3. Calls `server_socket.close()`. Steps 2 and 3 each wrapped in independent `try/except` so a failure in `shutdown()` does not prevent `close()`.
4. Calls `pool.shutdown(wait=False)` — releases worker threads without waiting for in-flight handlers to finish.

### Handler Contract

A handler receives the raw socket and the fully-parsed, injected message dict. It is responsible for writing any response back over `conn` using `protocol.send_message()`. The server never sends automatic responses.

```python
def my_handler(conn: socket.socket, message: dict):
    # message always contains at least:
    #   { "type": str, "_requester_ip": str }
    # "_requester_ip" is injected by _handle_client, not sent by the peer.
    send_message(conn, make_approved(...))
```

### Threading Model

```
Main thread
  └── start()
        └── daemon thread "PeerServer": _run()
              ├── socket: SO_REUSEADDR + SO_REUSEPORT, settimeout(1.0)
              ├── bind → listen(50)
              └── accept loop
                    ├── pool worker: _handle_client(conn_1, addr_1)
                    │     ├── conn.settimeout(CONNECT_TIMEOUT=15s)
                    │     ├── recv_message()
                    │     ├── inject _requester_ip
                    │     ├── [if REQUEST_FILE] conn.settimeout(APPROVAL_TIMEOUT=300s)
                    │     ├── handler(conn, message)
                    │     └── conn.close()  ← always, via finally
                    │
                    ├── pool worker: _handle_client(conn_2, addr_2)
                    └── ...  (up to max_workers=20 concurrent)
```

Each client connection is isolated in its own worker. A crash or slow handler in one worker does not affect others. The `max_workers` cap prevents unbounded thread growth under high connection rates.

### Socket Timeout Lifecycle Per Connection

```
accept() returns conn
    │
    ├── conn.settimeout(CONNECT_TIMEOUT=15s)
    │         └── recv_message()    ← timeout here = "slow peer"
    │
    ├── msg_type == "REQUEST_FILE"?
    │     └── conn.settimeout(APPROVAL_TIMEOUT=300s)
    │               └── handler blocks on result_event.wait(300s)
    │                   └── _send_file() streams chunks
    │
    └── conn.close()  ← finally block
```
