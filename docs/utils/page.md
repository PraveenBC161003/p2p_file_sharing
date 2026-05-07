# Utils

Internal utilities used across the project. This module contains two files: `config.py` for project-wide constants and path setup, and `logger.py` for structured terminal logging.

---

## `config.py`

Centralizes all configuration constants — directory paths, network settings, transfer parameters, and protocol message tokens. All other modules should import from here rather than hardcoding values.

### Directory Paths

Paths are resolved relative to the project root (`BASE_DIR`), two levels up from `config.py` itself. All three directories are created automatically on startup if they don't exist.

| Constant        | Path                             | Purpose                                        |
| --------------- | -------------------------------- | ---------------------------------------------- |
| `BASE_DIR`      | `<project_root>/`                | Anchor for all relative paths                  |
| `DOWNLOADS_DIR` | `<project_root>/downloads/`      | Files received from peers                      |
| `SHARED_DIR`    | `<project_root>/shared_files/`   | Files exposed for others to download           |
| `TEMP_DIR`      | `<project_root>/.tmp_transfers/` | Partial/in-progress downloads (resume support) |

### Network Settings

| Constant         | Default | Description                                  |
| ---------------- | ------- | -------------------------------------------- |
| `DEFAULT_PORT`   | `5000`  | Port peers listen on for connections         |
| `TRACKER_PORT`   | `5002`  | Port the tracker/discovery server listens on |
| `BACKLOG`        | `5`     | Max queued incoming connections per socket   |
| `SOCKET_TIMEOUT` | `30.0s` | Idle timeout for socket operations           |

### Transfer Settings

| Constant             | Default           | Description                                |
| -------------------- | ----------------- | ------------------------------------------ |
| `CHUNK_SIZE`         | `524288` (512 KB) | Size of each file chunk sent over the wire |
| `TRANSFER_TIMEOUT`   | `60.0s`           | Max wait time between consecutive chunks   |
| `CHECKSUM_ALGORITHM` | `"sha256"`        | Algorithm used to verify file integrity    |

### Protocol Message Tokens

String constants used as message type identifiers in the peer-to-peer protocol. Groupped by their role in the communication flow.

**Discovery (Tracker)**

| Constant         | Value          | Meaning                              |
| ---------------- | -------------- | ------------------------------------ |
| `MSG_REGISTER`   | `"REGISTER"`   | Peer announces itself to the tracker |
| `MSG_DEREGISTER` | `"DEREGISTER"` | Peer removes itself from the tracker |
| `MSG_GET_PEERS`  | `"GET_PEERS"`  | Request the current peer list        |
| `MSG_PEER_LIST`  | `"PEER_LIST"`  | Tracker's response with active peers |

**File Listing**

| Constant         | Value          | Meaning                                   |
| ---------------- | -------------- | ----------------------------------------- |
| `MSG_LIST_FILES` | `"LIST_FILES"` | Ask a peer what files it's sharing        |
| `MSG_FILE_LIST`  | `"FILE_LIST"`  | Peer's response with its shared file list |

**Transfer Handshake**

| Constant             | Value              | Meaning                             |
| -------------------- | ------------------ | ----------------------------------- |
| `MSG_REQUEST_FILE`   | `"REQUEST_FILE"`   | Request a specific file from a peer |
| `MSG_APPROVED`       | `"APPROVED"`       | Peer accepts the transfer request   |
| `MSG_REJECTED`       | `"REJECTED"`       | Peer denies the transfer request    |
| `MSG_TRANSFER_START` | `"TRANSFER_START"` | Signals the beginning of file data  |
| `MSG_TRANSFER_DONE`  | `"TRANSFER_DONE"`  | Signals the end of file data        |

**Chunk Transfer**

| Constant         | Value          | Meaning                     |
| ---------------- | -------------- | --------------------------- |
| `MSG_FILE_CHUNK` | `"FILE_CHUNK"` | A single chunk of file data |

**Generic**

| Constant    | Value     | Meaning                |
| ----------- | --------- | ---------------------- |
| `MSG_ACK`   | `"ACK"`   | General acknowledgment |
| `MSG_ERROR` | `"ERROR"` | General error response |

---

## `logger.py`

A lightweight, color-aware logger for structured terminal output. Each logger instance is named (typically after the module using it), and every line is prefixed with a timestamp, the logger name, and a colored severity level.

### Output Format

```
[HH:MM:SS] [<name>] [<LEVEL>] <message>
```

Each part is independently colored when the terminal supports it:

| Part         | Color   |
| ------------ | ------- |
| `[HH:MM:SS]` | Dim     |
| `[name]`     | Blue    |
| `[INFO]`     | Cyan    |
| `[OK]`       | Green   |
| `[WARN]`     | Yellow  |
| `[ERROR]`    | Red     |
| `[DEBUG]`    | Magenta |

Color output is enabled only when `stdout` is an interactive TTY. When piped or redirected (e.g. to a file), all ANSI codes are stripped automatically.

### Usage

```python
from utils.logger import get_logger

log = get_logger("peer")

log.info("Connecting to tracker...")
log.success("Registered successfully")
log.warn("Peer response was slow")
log.error("Connection refused")
log.debug("Raw packet: ...")   # only prints if debug mode is on
```

### Enabling Debug Mode

Debug-level messages are suppressed by default. Call `enable_debug()` once at startup (e.g. when a `--debug` CLI flag is passed) to activate them globally across all logger instances.

```python
from utils.logger import enable_debug

enable_debug()
```

### API Reference

#### `get_logger(name: str) -> Logger`

Factory function. Returns a `Logger` instance tagged with the given name. Recommended over instantiating `Logger` directly.

#### `Logger.info(msg: str)`

General informational messages about normal operations.

#### `Logger.success(msg: str)`

Confirmation that an operation completed successfully.

#### `Logger.warn(msg: str)`

Non-fatal issues or degraded conditions worth noting.

#### `Logger.error(msg: str)`

Failures that need attention.

#### `Logger.debug(msg: str)`

Verbose diagnostic output. Only printed when `enable_debug()` has been called.
