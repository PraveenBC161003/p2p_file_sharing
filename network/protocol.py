import json
import struct
import socket
from typing import Dict

from utils.logger import get_logger

log = get_logger("Protocol")

_LENGTH_FORMAT = ">I"
_LENGTH_SIZE = struct.calcsize(_LENGTH_FORMAT)


def send_message(sock: socket.socket, message: Dict):
    try:
        payload = json.dumps(message).encode("utf-8")
    except Exception as e:
        raise ValueError(f"Invalid message format: {e}")

    header = struct.pack(_LENGTH_FORMAT, len(payload))
    sock.sendall(header + payload)
    log.debug(f"Sent: {message.get('type', '?')}")


def recv_exact(sock: socket.socket, size: int) -> bytes:
    data = b""
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise ConnectionError("Connection closed by peer")
        data += chunk
    return data


def recv_message(sock: socket.socket) -> Dict:
    raw_header = recv_exact(sock, _LENGTH_SIZE)
    payload_length = struct.unpack(_LENGTH_FORMAT, raw_header)[0]

    # Reject payloads over 10 MB to prevent memory exhaustion attacks
    if payload_length > 10 * 1024 * 1024:
        raise ValueError("Message too large")

    raw_payload = recv_exact(sock, payload_length)

    try:
        message = json.loads(raw_payload.decode("utf-8"))
    except Exception as e:
        raise ValueError(f"Invalid JSON received: {e}")

    log.debug(f"Received: {message.get('type', '?')}")
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