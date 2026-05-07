# Network

Low-level networking layer of the application. Three files with clearly separated responsibilities: `client.py` for outbound TCP connections, `server.py` for inbound connections, and `protocol.py` for the wire format shared between both.

---

## `client.py` — `PeerClient`

Manages a single outbound TCP connection to a remote peer. Handles connection lifecycle, serialization delegation, and state tracking. All higher-level services (`DiscoveryService`, `DownloadService`, `FileService`) use this as their transport.

### Constructor

```python
PeerClient(host: str, port: int = 5000, timeout: float = 10)
```

| Parameter | Default | Description                               |
| --------- | ------- | ----------------------------------------- |
| `host`    | —       | IP address or hostname of the remote peer |
| `port`    | `5000`  | Port to connect to                        |
| `timeout` | `10.0s` | Socket-level timeout for all operations   |

The socket (`self.sock`) and connection flag (`self.connected`) are initialized to `None` and `False` respectively. No network activity happens at construction time.

### Methods

#### `connect()`

Opens a TCP connection to `host:port`. Sets a socket timeout to prevent indefinitely blocking calls. If already connected, logs a warning and returns early — it will never open a second socket on top of an existing one.

Raises `ConnectionError` on any failure (DNS resolution, connection refused, timeout). The raised error wraps the original exception with a clean message for upstream callers.

#### `send(message: dict)`

Serializes `message` to JSON bytes via `protocol.send_message()` and writes it to the socket. If sending fails, marks the connection as dead (`self.connected = False`) before re-raising so that subsequent calls don't attempt to use a broken socket.

#### `receive() -> dict`

Reads and deserializes the next message from the socket via `protocol.recv_message()`. Mirrors `send()` in its error handling — marks the connection dead on failure.

#### `send_and_receive(message: dict) -> dict`

Convenience wrapper for the request-response pattern. Calls `send()` then immediately `receive()` and returns the result. Used by `DiscoveryService` for all tracker interactions.

#### `close()`

Closes the underlying socket and sets `self.connected = False`. Silently swallows any exception from `sock.close()` — close is best-effort. Safe to call multiple times or even before `connect()`.

#### `_ensure_connected()` _(internal)_

Guard called at the top of `send()` and `receive()`. Raises `RuntimeError` if the socket is not ready, preventing silent failures from out-of-order usage (e.g. sending before connecting).

### State Machine

```
[created] ──connect()──> [connected] ──close()──> [closed]
              |                  |
         ConnectionError    send()/receive()
                           raises → [disconnected]
```

---

## `protocol.py`

Defines the wire format for all peer-to-peer communication and provides message constructor helpers. Every send and receive in the system goes through here — `PeerClient`, `PeerServer`, and `FileService` all import from this module.

### Wire Format

Messages are framed with a 4-byte big-endian unsigned integer header that encodes the payload length, followed by the UTF-8 JSON payload.

```
┌────────────────────┬──────────────────────────────────┐
│  4-byte header     │  N-byte JSON payload              │
│  (payload length)  │  { "type": "...", ... }           │
└────────────────────┴──────────────────────────────────┘
```

This length-prefixed framing ensures the receiver knows exactly how many bytes to read, avoiding partial reads across TCP packet boundaries.

### Functions

#### `send_message(sock, message: dict)`

Serializes `message` to JSON, packs the byte length into a 4-byte big-endian header, then calls `sock.sendall()` with the header and payload concatenated. `sendall` guarantees the entire buffer is written even if the OS sends it in multiple pieces.

Raises `ValueError` if the message cannot be serialized to JSON.

#### `recv_message(sock) -> dict`

Reads exactly 4 bytes for the header, unpacks the payload length, then reads exactly that many bytes for the body. Rejects payloads over **10 MB** to prevent memory exhaustion from malformed or malicious messages. Deserializes the body back to a dict.

Raises `ConnectionError` if the remote end closes the connection mid-read. Raises `ValueError` on an oversized payload or invalid JSON.

#### `recv_exact(sock, size: int) -> bytes` _(internal)_

Loop-reads from the socket until exactly `size` bytes have been accumulated. Necessary because a single `sock.recv()` call is not guaranteed to return the requested number of bytes on a stream socket.

### Message Constructors

All outgoing messages are built through these helpers. Direct inline dict construction is intentionally avoided so that the wire format stays consistent between sender and receiver, and any field changes only need to happen in one place.

| Constructor                               | Message type    | Key fields                      |
| ----------------------------------------- | --------------- | ------------------------------- |
| `make_list_files()`                       | `LIST_FILES`    | —                               |
| `make_file_list(files)`                   | `FILE_LIST`     | `files`                         |
| `make_request_file(filename)`             | `REQUEST_FILE`  | `filename`                      |
| `make_approved(filename, size, checksum)` | `APPROVED`      | `filename`, `size`, `checksum`  |
| `make_rejected(filename, reason)`         | `REJECTED`      | `filename`, `reason`            |
| `make_chunk(index, data)`                 | `FILE_CHUNK`    | `index`, `data` (base64 string) |
| `make_done(filename)`                     | `TRANSFER_DONE` | `filename`                      |

> `make_chunk` expects `data` to already be a base64-encoded string. Raw bytes cannot travel through a JSON payload — encoding is the caller's responsibility (`FileService._send_file` handles this).

---

## `server.py` — `PeerServer`

Listens for inbound TCP connections and dispatches each message to a registered handler based on its `type` field. Runs on a background daemon thread, handles each client connection in a thread pool worker.

### Constructor

```python
PeerServer(host: str = "0.0.0.0", port: int = 5000, max_workers: int = 10)
```

| Parameter     | Default     | Description                                                            |
| ------------- | ----------- | ---------------------------------------------------------------------- |
| `host`        | `"0.0.0.0"` | Interface to bind to — `0.0.0.0` accepts connections on all interfaces |
| `port`        | `5000`      | Port to listen on                                                      |
| `max_workers` | `10`        | Max concurrent client connections via `ThreadPoolExecutor`             |

Initializes an empty `self.handlers` dict and a `ThreadPoolExecutor`. No socket is created until `start()` is called.

### Methods

#### `register_handler(msg_type: str, handler: Handler)`

Registers a callable for a given message type string. The handler signature is `(conn: socket.socket, message: dict) -> None`. Called before `start()` to wire up all message types.

```python
server.register_handler("REQUEST_FILE", file_service.handle_file_request)
server.register_handler("LIST_FILES",   file_service.handle_list_files)
```

#### `start()`

Spawns the `_run` loop on a **daemon thread** (automatically killed when the main process exits) and returns immediately. The server is non-blocking from the caller's perspective.

#### `stop()`

Sets `self.running = False` to signal the accept loop to exit, then shuts down and closes the server socket, and calls `pool.shutdown(wait=False)` to release worker threads without waiting for in-flight handlers to finish.

#### `_run()` _(internal)_

The main accept loop. Creates and binds the server socket with `SO_REUSEADDR` (allows immediate rebind after a restart). Sets a `1.0s` accept timeout so the loop can check `self.running` regularly rather than blocking indefinitely. Each accepted connection is submitted to the thread pool as `_handle_client`.

#### `_handle_client(conn, addr)` _(internal)_

Runs per-connection in a pool worker. Loops reading messages from the connection via `protocol.recv_message()` until the connection closes or `self.running` is false. For each message:

1. Validates it is a `dict` and has a `type` field.
2. Injects `_requester_ip` (the peer's IP address) into the message dict so handlers can display it without needing direct socket access.
3. Looks up the handler in `self.handlers` and calls it.
4. Catches and logs handler exceptions without killing the connection — one bad message doesn't terminate the session.

Closes `conn` in a `finally` block regardless of how the loop exits.

### Handler Contract

A handler receives the raw socket and the fully parsed message dict. It is responsible for writing any response back over `conn` using `protocol.send_message()`. The server does not send any automatic responses.

```python
def my_handler(conn: socket.socket, message: dict):
    # message always contains at least: { "type": str, "_requester_ip": str }
    send_message(conn, make_approved(...))
```

### Threading Model

```
Main thread
  └── start()
        └── daemon thread: _run()  (accept loop)
              ├── pool worker: _handle_client(conn_1, addr_1)
              ├── pool worker: _handle_client(conn_2, addr_2)
              └── ...  (up to max_workers concurrent)
```

Each client connection is isolated in its own worker. A crash or slow handler in one worker does not affect others. The `max_workers` cap prevents unbounded thread growth under heavy load.
