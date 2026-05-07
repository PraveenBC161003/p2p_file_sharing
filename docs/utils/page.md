# Utils

Internal utilities used across the project. This module contains two files: `config.py` for project-wide constants and path setup, and `logger.py` for structured terminal logging.

---

## `config.py`

Centralizes all configuration constants — directory paths, network settings, transfer parameters, and protocol message tokens. All other modules should import from here rather than hardcoding values.

Paths are resolved at **import time**: `BASE_DIR` is anchored two levels above `config.py` itself (`Path(__file__).resolve().parent.parent`). All three working directories are created immediately via `Path.mkdir(parents=True, exist_ok=True)` as a side-effect of importing the module — no explicit setup call is required.

### Directory Paths

| Constant        | Resolved Path                    | Purpose                                        |
| --------------- | -------------------------------- | ---------------------------------------------- |
| `BASE_DIR`      | `<project_root>/`                | Anchor for all relative paths                  |
| `DOWNLOADS_DIR` | `<project_root>/downloads/`      | Files received from peers                      |
| `SHARED_DIR`    | `<project_root>/shared_files/`   | Files exposed for others to download           |
| `TEMP_DIR`      | `<project_root>/.tmp_transfers/` | Partial/in-progress downloads (resume support) |

All three directories are auto-created on first import if they don't already exist.

### Network Settings

| Constant         | Type    | Default | Description                                   |
| ---------------- | ------- | ------- | --------------------------------------------- |
| `DEFAULT_PORT`   | `int`   | `5000`  | Port peers listen on for incoming connections |
| `TRACKER_PORT`   | `int`   | `5002`  | Port the tracker/discovery server listens on  |
| `BACKLOG`        | `int`   | `5`     | Max queued incoming connections per socket    |
| `SOCKET_TIMEOUT` | `float` | `30.0`  | Idle timeout (seconds) for socket operations  |

### Transfer Settings

| Constant             | Type    | Default           | Description                                                                                                                                                                                             |
| -------------------- | ------- | ----------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `CHUNK_SIZE`         | `int`   | `524288` (512 KB) | Size of each file chunk sent over the wire                                                                                                                                                              |
| `TRANSFER_TIMEOUT`   | `float` | `360.0`           | Max wait (seconds) between consecutive chunks. Intentionally set above the server-side `APPROVAL_TIMEOUT` (300 s) to prevent the client from timing out while the server is still in the approval phase |
| `CHECKSUM_ALGORITHM` | `str`   | `"sha256"`        | Algorithm used to verify file integrity                                                                                                                                                                 |

> **Implementation note — `TRANSFER_TIMEOUT` vs docs:** The live value is `360.0 s`, not `60.0 s` as stated in older documentation. The increase exists so that `TRANSFER_TIMEOUT` reliably exceeds the server-side `APPROVAL_TIMEOUT` (300 s); a client that timed out during the approval window would abort a transfer the server was about to approve.

### Protocol Message Tokens

Plain string constants used as message-type identifiers in the peer-to-peer protocol. They are compared directly with `==` in protocol handlers — no enum machinery, no integer codes.

**Discovery (Tracker)**

| Constant         | Wire Value     | Direction      | Meaning                              |
| ---------------- | -------------- | -------------- | ------------------------------------ |
| `MSG_REGISTER`   | `"REGISTER"`   | peer → tracker | Peer announces itself to the tracker |
| `MSG_DEREGISTER` | `"DEREGISTER"` | peer → tracker | Peer removes itself from the tracker |
| `MSG_GET_PEERS`  | `"GET_PEERS"`  | peer → tracker | Request the current active peer list |
| `MSG_PEER_LIST`  | `"PEER_LIST"`  | tracker → peer | Tracker's response with active peers |

**File Listing**

| Constant         | Wire Value     | Direction   | Meaning                                    |
| ---------------- | -------------- | ----------- | ------------------------------------------ |
| `MSG_LIST_FILES` | `"LIST_FILES"` | peer → peer | Ask a remote peer what files it is sharing |
| `MSG_FILE_LIST`  | `"FILE_LIST"`  | peer → peer | Remote peer's response with its file list  |

**Transfer Handshake**

| Constant             | Wire Value         | Direction   | Meaning                                    |
| -------------------- | ------------------ | ----------- | ------------------------------------------ |
| `MSG_REQUEST_FILE`   | `"REQUEST_FILE"`   | peer → peer | Request a specific file from a remote peer |
| `MSG_APPROVED`       | `"APPROVED"`       | peer → peer | Remote peer accepts the transfer request   |
| `MSG_REJECTED`       | `"REJECTED"`       | peer → peer | Remote peer denies the transfer request    |
| `MSG_TRANSFER_START` | `"TRANSFER_START"` | peer → peer | Signals that file data is about to begin   |
| `MSG_TRANSFER_DONE`  | `"TRANSFER_DONE"`  | peer → peer | Signals that all file data has been sent   |

**Chunk Transfer**

| Constant         | Wire Value     | Direction   | Meaning                     |
| ---------------- | -------------- | ----------- | --------------------------- |
| `MSG_FILE_CHUNK` | `"FILE_CHUNK"` | peer → peer | A single chunk of file data |

**Generic**

| Constant    | Wire Value | Meaning                        |
| ----------- | ---------- | ------------------------------ |
| `MSG_ACK`   | `"ACK"`    | General-purpose acknowledgment |
| `MSG_ERROR` | `"ERROR"`  | General-purpose error response |

---

## `logger.py`

A lightweight, color-aware logger for structured terminal output. Instances are named (typically after the calling module), and every line is prefixed with a timestamp, the logger name, and a colored severity tag.

### How Color Detection Works

Color support is determined **once at module load** by calling `supports_color()`, which checks whether `sys.stdout` has an `isatty` attribute and whether `sys.stdout.isatty()` returns `True`. The result is stored in the module-level boolean `_COLOR_ENABLED`. When `False` (e.g. output is redirected to a file or a pipe), all ANSI escape codes are stripped — the `_color()` helper returns the raw text unchanged.

### Output Format

```
[HH:MM:SS] [<name>] [<LEVEL>] <message>
```

Each component is independently colorized:

| Component    | Color   | ANSI Code  |
| ------------ | ------- | ---------- |
| `[HH:MM:SS]` | Dim     | `\033[2m`  |
| `[name]`     | Blue    | `\033[34m` |
| `[INFO]`     | Cyan    | `\033[36m` |
| `[OK]`       | Green   | `\033[32m` |
| `[WARN]`     | Yellow  | `\033[33m` |
| `[ERROR]`    | Red     | `\033[31m` |
| `[DEBUG]`    | Magenta | `\033[35m` |

### Module-Level State

| Variable         | Type   | Default                      | Description                                                  |
| ---------------- | ------ | ---------------------------- | ------------------------------------------------------------ |
| `_COLOR_ENABLED` | `bool` | Result of `supports_color()` | Controls ANSI escape emission; set once at import time       |
| `_DEBUG`         | `bool` | `False`                      | Gates `debug()` output; flipped globally by `enable_debug()` |

> Both variables are module-globals. Calling `enable_debug()` affects **all** `Logger` instances in the process because they all read `_DEBUG` from the module namespace at call time — there is no per-instance debug flag.

### Usage

```python
from utils.logger import get_logger

log = get_logger("peer")

log.info("Connecting to tracker...")
log.success("Registered successfully")
log.warn("Peer response was slow")    # also callable as log.warning(...)
log.error("Connection refused")
log.debug("Raw packet: ...")          # only prints if enable_debug() was called
```

### Enabling Debug Mode

Debug messages are suppressed by default. Call `enable_debug()` once at startup — typically when a `--debug` CLI flag is parsed — to activate them globally.

```python
from utils.logger import enable_debug

enable_debug()
```

### API Reference

#### `supports_color() -> bool`

Checks whether the current `stdout` is an interactive TTY. Returns `False` if `stdout` has no `isatty` attribute (e.g. in some embedded environments) or if `isatty()` returns `False`.

#### `enable_debug()`

Sets the module-level `_DEBUG` flag to `True`. Affects all `Logger` instances immediately, including ones already created.

#### `get_logger(name: str) -> Logger`

Factory function. Returns a new `Logger` instance tagged with `name`. Recommended over instantiating `Logger` directly.

#### `Logger(name: str)`

Internal class. Holds only the instance `name`; all other state (`_DEBUG`, `_COLOR_ENABLED`) is read from the module at call time.

#### `Logger.info(msg: str)`

Prints a `[INFO]` line. Use for normal operational events.

#### `Logger.success(msg: str)`

Prints an `[OK]` line. Use to confirm that an operation completed successfully.

#### `Logger.warn(msg: str)` / `Logger.warning(msg: str)`

Both names are available (`warning` is an alias for `warn`). Prints a `[WARN]` line for non-fatal issues or degraded conditions.

#### `Logger.error(msg: str)`

Prints an `[ERROR]` line. Use for failures that require attention.

#### `Logger.debug(msg: str)`

Prints a `[DEBUG]` line **only** when `_DEBUG` is `True` (i.e. after `enable_debug()` has been called). Silently suppressed otherwise — no performance-sensitive guard needed at call sites.
