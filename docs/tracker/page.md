# Tracker — Component Documentation

## Overview

`Tracker` is the central coordination server in the P2P network. It does not transfer files itself — its sole responsibility is **peer registry management**: accepting registrations from nodes as they come online, serving peer lists on request, removing nodes when they go offline, and expiring nodes that silently disappear.

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
├── Server socket timeout = 1.0 s  ← allows clean shutdown polling
├── Spawns a daemon thread per connection → _handle_client()
└── Spawns a background daemon thread on start() → _reap_dead_peers()

_handle_client()
     ├── REGISTER     → upserts (ip, port) → time.time() into self.peers
     ├── GET_PEERS    → returns snapshot of self.peers as list of {host, port}
     ├── DEREGISTER   → deletes (ip, port) from self.peers
     ├── HEARTBEAT    → refreshes timestamp for (ip, port); auto-registers if missing
     └── Unknown type → returns ERROR response

_reap_dead_peers()  [background daemon, interval = PEER_TTL / 3]
     └── deletes every (ip, port) whose last_seen timestamp is older than PEER_TTL
```

Each client connection is short-lived: one message in, one message out, socket closes. The Tracker is **stateless per connection** and **stateful across connections** via `self.peers`.

---

## Module-Level Constant

| Constant   | Type    | Default | Description                                                                                                                       |
| ---------- | ------- | ------- | --------------------------------------------------------------------------------------------------------------------------------- |
| `PEER_TTL` | `float` | `90.0`  | Seconds a peer registration stays valid without a heartbeat. Peers should re-register every ~30 s to stay well within the window. |

---

## Class: `Tracker`

### Constructor

```python
Tracker(host: str = "0.0.0.0", port: int = 5002)
```

| Parameter | Type  | Default     | Description                                                                   |
| --------- | ----- | ----------- | ----------------------------------------------------------------------------- |
| `host`    | `str` | `"0.0.0.0"` | Interface to bind to. `"0.0.0.0"` listens on all available network interfaces |
| `port`    | `int` | `5002`      | Port to accept peer connections on                                            |

**Initialised attributes:**

| Attribute            | Type                           | Description                                                                                                           |
| -------------------- | ------------------------------ | --------------------------------------------------------------------------------------------------------------------- |
| `self.host`          | `str`                          | Bound interface                                                                                                       |
| `self.port`          | `int`                          | Bound port                                                                                                            |
| `self.peers`         | `dict[tuple[str, int], float]` | Registry mapping `(ip, port)` → `last_seen` Unix timestamp. Previously a `set`; now a `dict` to support TTL tracking. |
| `self.running`       | `bool`                         | Lifecycle flag; controls both the accept loop and the reaper loop                                                     |
| `self.server_socket` | `socket \| None`               | The raw TCP server socket; `None` until `start()` is called                                                           |
| `self._peers_lock`   | `threading.Lock`               | Mutex protecting all reads and writes to `self.peers` across concurrent handler threads                               |

> **Data structure change:** `self.peers` was a `set` of `(ip, port)` tuples in the previous version. It is now a `dict` mapping `(ip, port) → float` (Unix timestamp). Every REGISTER and HEARTBEAT message updates the timestamp; the reaper uses it to evict stale entries. All access to `self.peers` must be done inside `with self._peers_lock`.

---

### Methods

#### `start() → None`

Brings the Tracker online. This is a **blocking call** — it enters an accept loop and does not return until `stop()` is called or the process is interrupted.

**Steps:**

1. Sets `self.running = True`.
2. Starts the `_reap_dead_peers` background thread as a daemon.
3. Creates a TCP socket (`AF_INET`, `SOCK_STREAM`).
4. Sets `SO_REUSEADDR` — allows immediate restart without waiting for OS socket timeout.
5. Sets a `1.0 s` timeout on the server socket. This is intentional: `accept()` will raise `socket.timeout` every second, allowing the loop to re-check `self.running` and exit cleanly when `stop()` is called. `socket.timeout` exceptions are caught and silently continued.
6. Attempts `bind((self.host, self.port))`. On `OSError` (e.g. port already in use), logs the error, sets `self.running = False`, and returns early without entering the accept loop.
7. Calls `listen(50)` and logs a success line that includes the configured `PEER_TTL`.
8. Enters accept loop: on each connection spawns a **daemon thread** targeting `_handle_client()`. Daemon threads are automatically killed when the main process exits.

```python
tracker = Tracker(host="0.0.0.0", port=5002)
tracker.start()
# [Tracker] Tracker running on 0.0.0.0:5002 (peer TTL=90.0s)
```

---

#### `stop() → None`

Gracefully shuts down the Tracker.

1. Sets `self.running = False` — signals both the accept loop and the reaper loop to exit on their next iteration.
2. Calls `server_socket.shutdown(SHUT_RDWR)` — interrupts any blocked `accept()` immediately.
3. Calls `server_socket.close()`.
4. Both shutdown steps are wrapped in individual `try/except` blocks so that a failure in `shutdown()` does not prevent `close()` from running.

```python
tracker.stop()
# [Tracker] Tracker stopped
```

> **Shutdown change:** The previous version called only `server_socket.close()`. The current version calls `shutdown(SHUT_RDWR)` first to forcibly unblock `accept()`, then `close()`. This matters because the 1.0 s socket timeout means the loop would otherwise wait up to one second before noticing `self.running = False`.

---

#### `_reap_dead_peers() → None` _(private, daemon thread)_

Runs in a background daemon thread started by `start()`. Periodically evicts peers whose TTL has expired.

**Behaviour:**

- Loops while `self.running` is `True`.
- Sleeps `PEER_TTL / 3` seconds between passes (i.e. every 30 s with the default 90 s TTL). This gives three chances to catch a peer within one TTL window.
- On each wake, acquires `_peers_lock` and collects every key whose `now - last_seen > PEER_TTL`.
- Deletes each expired key and logs an `[INFO]` line per eviction.

```
# Timeline example (PEER_TTL = 90 s, reaper interval = 30 s)
t=0   Peer A registers  → self.peers[("10.0.0.1", 9000)] = 0.0
t=30  reaper wakes      → age = 30 s < 90 s  → kept
t=60  reaper wakes      → age = 60 s < 90 s  → kept
t=90  reaper wakes      → age = 90 s = TTL   → evicted
```

---

#### `_handle_client(conn, addr) → None` _(private, per-connection daemon thread)_

Handles one complete request/response cycle for a single peer connection.

| Parameter | Type            | Description                         |
| --------- | --------------- | ----------------------------------- |
| `conn`    | `socket.socket` | The connected client socket         |
| `addr`    | `tuple`         | `(ip, port)` of the connecting peer |

Sets a `10.0 s` timeout on `conn` before reading — avoids hanging indefinitely on a slow or misbehaving client. The client's IP is extracted from `addr[0]`; the peer's _server port_ is always read from the message payload (never from `addr[1]`, which is an ephemeral port).

The `finally` block guarantees `conn.close()` is always called, even if an exception occurs mid-handling. Both `conn.close()` exceptions are silently suppressed.

**Message handling:**

| Message Type              | Required Fields | Behaviour                                                                                                                                                | Response                                      |
| ------------------------- | --------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------- |
| `REGISTER`                | `port`          | Acquires lock → upserts `(ip, port) → time.time()` into `self.peers`. Logs registration with total peer count.                                           | `{"type": "ACK"}`                             |
| `REGISTER` (missing port) | —               | Logs a warning; does not modify peer set.                                                                                                                | `{"type": "ERROR", "reason": "Missing port"}` |
| `GET_PEERS`               | —               | Acquires lock → snapshots `self.peers` keys into a list of `{"host", "port"}` dicts. Logs count at DEBUG level.                                          | `{"type": "PEER_LIST", "peers": [...]}`       |
| `DEREGISTER`              | `port`          | Acquires lock → deletes `(ip, port)` if present and logs with remaining count. If not found, logs a warning. Either way, responds with `ACK`.            | `{"type": "ACK"}`                             |
| `HEARTBEAT`               | `port`          | Acquires lock → updates `self.peers[(ip, port)] = time.time()` if present (logs DEBUG). If the key is absent, inserts it (auto-registration, logs INFO). | `{"type": "ACK"}`                             |
| _(anything else)_         | —               | Logs a warning with the unknown type; no state change.                                                                                                   | `{"type": "ERROR", "reason": "Unknown type"}` |

> **New message type — `HEARTBEAT`:** Not present in the previous version. A lightweight keep-alive that refreshes a peer's TTL without the overhead of a full re-register. If the peer was not already in `self.peers` (e.g. the Tracker restarted), the heartbeat silently auto-registers it, making the handler fault-tolerant.

---

### Message Flow Examples

**Node coming online:**

```
Peer  ──►  {"type": "REGISTER", "port": 9000}   ──►  Tracker
Peer  ◄──  {"type": "ACK"}                       ◄──  Tracker
           self.peers = {("192.168.1.5", 9000): <timestamp>}
```

**Node sending a heartbeat:**

```
Peer  ──►  {"type": "HEARTBEAT", "port": 9000}   ──►  Tracker
Peer  ◄──  {"type": "ACK"}                        ◄──  Tracker
           self.peers[("192.168.1.5", 9000)] = <refreshed timestamp>
```

**Node requesting peer list:**

```
Peer  ──►  {"type": "GET_PEERS"}                                   ──►  Tracker
Peer  ◄──  {"type": "PEER_LIST",                                   ◄──  Tracker
             "peers": [{"host": "192.168.1.5", "port": 9000}, ...]}
```

**Node going offline:**

```
Peer  ──►  {"type": "DEREGISTER", "port": 9000}  ──►  Tracker
Peer  ◄──  {"type": "ACK"}                        ◄──  Tracker
           self.peers = {}
```

**Peer silently disappears (no DEREGISTER):**

```
t=0    Peer registers     → self.peers[("10.0.0.1", 9000)] = 0.0
t=90   Reaper fires       → now - 0.0 > 90.0 → evicted
       self.peers = {}
```

---

## Lifecycle Diagram

```
Tracker()
    │
    ▼
start()
    ├── self.running = True
    ├── Spawn daemon thread → _reap_dead_peers()
    │         └── while running: sleep(TTL/3) → evict expired peers
    │
    ├── Create TCP socket, SO_REUSEADDR, settimeout(1.0)
    ├── bind() → on OSError: log + return early
    ├── listen(backlog=50)
    └── Accept loop
          │
          ├── socket.timeout  → continue (re-check self.running)
          │
          ├── connection arrives
          │    └── spawn daemon thread → _handle_client(conn, addr)
          │         ├── conn.settimeout(10.0)
          │         ├── recv_message()
          │         ├── acquire _peers_lock
          │         ├── route by msg_type → mutate self.peers
          │         ├── release _peers_lock
          │         ├── send_message()
          │         └── conn.close()  ← always, via finally
          │
          └── loop continues...
               │
               ▼  (on stop() or KeyboardInterrupt)
            stop()
               ├── self.running = False
               ├── server_socket.shutdown(SHUT_RDWR)
               └── server_socket.close()
```

---

## Concurrency Model

The Tracker runs three categories of concurrent execution:

| Thread          | Count         | Target                | Daemon | Purpose                               |
| --------------- | ------------- | --------------------- | ------ | ------------------------------------- |
| Main thread     | 1             | `start()` accept loop | No     | Accepts incoming connections          |
| Client handlers | 1 per request | `_handle_client()`    | Yes    | Serves one request/response cycle     |
| Reaper          | 1             | `_reap_dead_peers()`  | Yes    | Periodically evicts TTL-expired peers |

`self.peers` is shared across all threads. All reads and writes are protected by `self._peers_lock` (`threading.Lock()`). The lock scope is kept deliberately narrow — only the dict mutation and snapshot operations are inside `with self._peers_lock` blocks — to minimise contention.

> **Concurrency change vs previous version:** The old implementation relied on CPython's GIL to protect `set.add()` / `set.remove()` operations. The new implementation uses an explicit `threading.Lock` because dict mutation is more complex (upsert, snapshot, delete-if-present) and because the reaper thread creates a third concurrent writer that the GIL alone cannot sequence safely.

---

## Design Notes

**`self.peers` is now a `dict`, not a `set`** — The switch from `set[tuple]` to `dict[tuple, float]` is the most significant structural change. It enables TTL tracking with zero additional data structures and makes `HEARTBEAT` a natural O(1) timestamp update.

**Port distinction** — The port stored in `self.peers` is always the peer's _server port_ (from the message payload), not the _ephemeral connection port_ (`addr[1]`). The ephemeral port is meaningless for routing; the server port is what other peers need to connect to.

**`DEREGISTER` is lenient** — If `(ip, port)` is not found in the dict, the Tracker logs a warning but still responds with `ACK`, making the operation idempotent. This handles the case where a peer calls `DEREGISTER` after having already been reaped by the TTL expiry.

**`HEARTBEAT` auto-registers** — If a `HEARTBEAT` arrives for an unknown peer (e.g. Tracker restarted mid-session), the handler inserts the entry rather than returning an error. This prevents peers from needing out-of-band error recovery for a transient Tracker restart.

**No persistence** — `self.peers` is in-memory only. A Tracker restart clears all registrations. Peers must re-register on their next startup or heartbeat cycle.

**Bind failure is non-fatal to the caller** — `start()` returns early on `OSError` rather than raising, because it is typically called in a thread or a `try/except KeyboardInterrupt` block. The caller should check logs or wrap `start()` to detect this condition.

---

## Dependencies

| Import                          | Role                                            |
| ------------------------------- | ----------------------------------------------- |
| `socket`                        | TCP server socket (stdlib)                      |
| `threading`                     | Per-connection daemon threads + `Lock` (stdlib) |
| `time`                          | `time.time()` for TTL timestamps (stdlib)       |
| `utils.logger.get_logger`       | Structured logger                               |
| `network.protocol.recv_message` | Deserialises an incoming message from a socket  |
| `network.protocol.send_message` | Serialises and sends a message over a socket    |

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

**Default configuration:** binds to all interfaces on port `5002` with a `90 s` peer TTL. To customise:

```python
tracker = Tracker(host="127.0.0.1", port=6000)
```

Run directly:

```bash
python tracker.py
# [Tracker] Tracker running on 0.0.0.0:5002 (peer TTL=90.0s)
```
