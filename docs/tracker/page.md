# Tracker — Component Documentation

## Overview

`Tracker` is the central coordination server in the P2P network. It does not transfer files itself — its sole responsibility is **peer registry management**: accepting registrations from nodes as they come online, serving peer lists on request, and removing nodes when they go offline.

Every `P2PNode` contacts the Tracker at startup and shutdown via the `DiscoveryService`.

**Module:** `tracker.py`  
**Logger name:** `Tracker`  
**Entry point:** `__main__` block at the bottom of the file

---

## Architecture

```
Tracker (TCP Server)
│
├── Accepts connections on (host, port)
├── Spawns a daemon thread per connection
│
└── _handle_client()
     ├── REGISTER     → adds (ip, port) to self.peers
     ├── GET_PEERS    → returns full peer list
     ├── DEREGISTER   → removes (ip, port) from self.peers
     └── Unknown type → returns ERROR response
```

Each client connection is short-lived: one message in, one message out, then the socket closes. The Tracker is **stateless per connection** and **stateful across connections** via `self.peers`.

---

## Class: `Tracker`

### Constructor

```python
Tracker(host: str = "0.0.0.0", port: int = 5002)
```

| Parameter | Type  | Default     | Description                                                                 |
| --------- | ----- | ----------- | --------------------------------------------------------------------------- |
| `host`    | `str` | `"0.0.0.0"` | Interface to bind to. `0.0.0.0` listens on all available network interfaces |
| `port`    | `int` | `5002`      | Port to accept peer connections on                                          |

**Initialised attributes:**

| Attribute            | Type                         | Description                                                 |
| -------------------- | ---------------------------- | ----------------------------------------------------------- |
| `self.host`          | `str`                        | Bound interface                                             |
| `self.port`          | `int`                        | Bound port                                                  |
| `self.peers`         | `set` of `(ip, port)` tuples | In-memory registry of all currently active peers            |
| `self.running`       | `bool`                       | Lifecycle flag; controls the accept loop                    |
| `self.server_socket` | `socket \| None`             | The raw TCP server socket; `None` until `start()` is called |

---

### Methods

#### `start() → None`

Brings the Tracker online. This is a **blocking call** — it enters an accept loop and does not return until `stop()` is called or the process is interrupted.

**Steps:**

1. Sets `self.running = True`.
2. Creates a TCP socket (`AF_INET`, `SOCK_STREAM`).
3. Sets `SO_REUSEADDR` — allows immediate restart without waiting for OS socket timeout.
4. Binds to `(self.host, self.port)` and starts listening with a backlog of **50** queued connections.
5. Enters accept loop:
   - On each incoming connection, spawns a **daemon thread** targeting `_handle_client()`.
   - Daemon threads are automatically killed when the main process exits — no manual cleanup needed.
   - Exceptions during `accept()` are logged and the loop continues.

```python
tracker = Tracker(host="0.0.0.0", port=5002)
tracker.start()
# [Tracker] Tracker running on 0.0.0.0:5002
```

---

#### `stop() → None`

Gracefully shuts down the Tracker.

1. Sets `self.running = False` — signals the accept loop to exit on its next iteration.
2. Closes `self.server_socket` — causes any blocked `accept()` call to raise an exception and unblock the loop.

```python
tracker.stop()
# [Tracker] Tracker stopped
```

---

#### `_handle_client(conn, addr) → None` _(private)_

Handles one complete request/response cycle for a single peer connection. Runs inside a dedicated daemon thread.

| Parameter | Type     | Description                         |
| --------- | -------- | ----------------------------------- |
| `conn`    | `socket` | The connected client socket         |
| `addr`    | `tuple`  | `(ip, port)` of the connecting peer |

The client's IP is extracted from `addr[0]`. The port used for peer identity is read from the message payload (not the ephemeral connection port).

**Message handling:**

| Message Type              | Required Fields | Behaviour                                                        | Response                                      |
| ------------------------- | --------------- | ---------------------------------------------------------------- | --------------------------------------------- |
| `REGISTER`                | `port`          | Adds `(ip, port)` to `self.peers`. Logs the registration.        | `{"type": "ACK"}`                             |
| `REGISTER` (missing port) | —               | Does not modify peer set                                         | `{"type": "ERROR", "reason": "Missing port"}` |
| `GET_PEERS`               | —               | Serialises `self.peers` into a list of `{"host", "port"}` dicts  | `{"type": "PEER_LIST", "peers": [...]}`       |
| `DEREGISTER`              | `port`          | Removes `(ip, port)` from `self.peers` if present. Logs removal. | `{"type": "ACK"}`                             |
| _(anything else)_         | —               | No state change                                                  | `{"type": "ERROR", "reason": "Unknown type"}` |

The `finally` block guarantees `conn.close()` is always called, even if an exception occurs mid-handling.

---

#### Message Flow Examples

**Node coming online:**

```
Peer  ──►  {"type": "REGISTER", "port": 9000}   ──►  Tracker
Peer  ◄──  {"type": "ACK"}                       ◄──  Tracker
           self.peers = {("192.168.1.5", 9000)}
```

**Node requesting peer list:**

```
Peer  ──►  {"type": "GET_PEERS"}                         ──►  Tracker
Peer  ◄──  {"type": "PEER_LIST",                         ◄──  Tracker
             "peers": [{"host": "192.168.1.5", "port": 9000}, ...]}
```

**Node going offline:**

```
Peer  ──►  {"type": "DEREGISTER", "port": 9000}  ──►  Tracker
Peer  ◄──  {"type": "ACK"}                        ◄──  Tracker
           self.peers = {}
```

---

## Lifecycle Diagram

```
Tracker()
    │
    ▼
start()
    ├── Create + bind TCP socket
    ├── Listen (backlog = 50)
    └── Accept loop
         │
         ├── connection arrives
         │    └── spawn daemon thread → _handle_client()
         │         ├── recv_message()
         │         ├── route by msg_type
         │         ├── mutate self.peers (if REGISTER / DEREGISTER)
         │         ├── send_message()
         │         └── conn.close()  ← always, via finally
         │
         └── loop continues...
              │
              ▼  (on stop() or KeyboardInterrupt)
           stop()
              ├── self.running = False
              └── server_socket.close()
```

---

## Concurrency Model

Each peer connection is handled in its own **daemon thread**. This means:

- The Tracker can serve multiple peers simultaneously without one slow client blocking others.
- `self.peers` is a `set` shared across all threads. In CPython, simple `add()` and `remove()` operations on a `set` are protected by the GIL and are effectively atomic for single operations, which is sufficient for this use case.
- Daemon threads require no explicit join — they are cleaned up automatically when the main thread exits.

---

## Design Notes

**Port distinction** — The port stored in `self.peers` is the peer's _server port_ (from the message payload), not the _ephemeral connection port_ (from `addr[1]`). This is intentional: the ephemeral port is meaningless for routing; the server port is what other peers need to connect to.

**No persistence** — `self.peers` is an in-memory `set`. If the Tracker restarts, all peer registrations are lost. Peers must re-register on their next startup.

**No heartbeat / TTL** — The Tracker does not actively check whether registered peers are still alive. Stale entries remain until the peer explicitly sends `DEREGISTER`. A future improvement could add periodic health checks or TTL-based expiry.

**`DEREGISTER` is lenient** — If the `(ip, port)` pair is not found in the set, the Tracker still responds with `ACK` rather than an error, making the operation idempotent.

---

## Dependencies

| Import                          | Role                                           |
| ------------------------------- | ---------------------------------------------- |
| `socket`                        | TCP server socket (stdlib)                     |
| `threading`                     | Per-connection daemon threads (stdlib)         |
| `utils.logger.get_logger`       | Structured logger                              |
| `network.protocol.recv_message` | Deserialises an incoming message from a socket |
| `network.protocol.send_message` | Serialises and sends a message over a socket   |

---

## Running the Tracker

The module includes a `__main__` block for running the Tracker as a standalone process:

```python
if __name__ == "__main__":
    tracker = Tracker()
    try:
        tracker.start()       # blocking — runs until interrupted
    except KeyboardInterrupt:
        tracker.stop()        # graceful shutdown on Ctrl+C
```

**Default configuration:** binds to all interfaces on port `5002`. To customise:

```python
tracker = Tracker(host="127.0.0.1", port=6000)
```

Run directly:

```bash
python tracker.py
# [Tracker] Tracker running on 0.0.0.0:5002
```
