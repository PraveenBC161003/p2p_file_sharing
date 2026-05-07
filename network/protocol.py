import json
import struct
import socket
from typing import Dict

from utils.logger import get_logger

log = get_logger("Protocol")

_LENGTH_FORMAT = ">I"
_LENGTH_SIZE = struct.calcsize(_LENGTH_FORMAT)


def send_message(sock: socket.socket, message: Dict):
    """Send a message over socket with error handling."""
    try:
        payload = json.dumps(message).encode("utf-8")
    except Exception as e:
        raise ValueError(f"Invalid message format: {e}")

    if len(payload) > 100 * 1024 * 1024:
        raise ValueError("Message payload too large (>100MB)")

    header = struct.pack(_LENGTH_FORMAT, len(payload))
    
    try:
        sock.sendall(header + payload)
        log.debug(f"Sent: {message.get('type', '?')} ({len(payload)} bytes)")
    except socket.timeout:
        raise ConnectionError("Send timeout")
    except BrokenPipeError:
        raise ConnectionError("Connection lost (broken pipe)")
    except Exception as e:
        raise ConnectionError(f"Failed to send message: {e}")


def recv_exact(sock: socket.socket, size: int) -> bytes:
    """Receive exactly 'size' bytes from socket."""
    data = b""
    while len(data) < size:
        try:
            chunk = sock.recv(size - len(data))
        except socket.timeout:
            raise ConnectionError(f"Timeout receiving data (got {len(data)}/{size} bytes)")
        except Exception as e:
            raise ConnectionError(f"Error receiving data: {e}")
        
        if not chunk:
            raise ConnectionError(f"Connection closed by peer (got {len(data)}/{size} bytes)")
        data += chunk
    return data


def recv_message(sock: socket.socket) -> Dict:
    """Receive a message from socket with error handling."""
    try:
        raw_header = recv_exact(sock, _LENGTH_SIZE)
    except ConnectionError as e:
        raise ConnectionError(f"Failed to receive message header: {e}")
    
    try:
        payload_length = struct.unpack(_LENGTH_FORMAT, raw_header)[0]
    except Exception as e:
        raise ValueError(f"Invalid message header: {e}")

    # Reject payloads over 100 MB to prevent memory exhaustion attacks
    if payload_length > 100 * 1024 * 1024:
        raise ValueError(f"Message too large: {payload_length} bytes (max 100MB)")
    
    if payload_length == 0:
        raise ValueError("Empty message payload")

    try:
        raw_payload = recv_exact(sock, payload_length)
    except ConnectionError as e:
        raise ConnectionError(f"Failed to receive message payload: {e}")

    try:
        message = json.loads(raw_payload.decode("utf-8"))
    except Exception as e:
        raise ValueError(f"Invalid JSON received: {e}")

    log.debug(f"Received: {message.get('type', '?')} ({payload_length} bytes)")
    return message


# ── Message constructors ──────────────────────────────────────────────────────
# All outgoing messages are built through these helpers to keep the wire format
# consistent between sender and receiver. Never construct dicts inline.

def make_list_files():
    return {"type": "LIST_FILES"}

def make_file_list(files):
    return {"type": "FILE_LIST", "files": files}

def make_request_file(filename):
    return {"type": "REQUEST_FILE", "filename": filename}

def make_approved(filename, size, checksum):
    # checksum lets the downloader verify the file was not corrupted in transit
    return {
        "type":     "APPROVED",
        "filename": filename,
        "size":     size,
        "checksum": checksum
    }

def make_rejected(filename, reason):
    return {
        "type":     "REJECTED",
        "filename": filename,
        "reason":   reason
    }

def make_chunk(index, data):
    # data must be a base64-encoded string — raw bytes cannot travel through JSON
    return {
        "type":  "FILE_CHUNK",
        "index": index,
        "data":  data
    }

def make_done(filename):
    return {"type": "TRANSFER_DONE", "filename": filename}