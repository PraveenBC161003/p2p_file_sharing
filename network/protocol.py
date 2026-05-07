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
    try:
        raw_header = recv_exact(sock, _LENGTH_SIZE)
    except ConnectionError as e:
        raise ConnectionError(f"Failed to receive message header: {e}")

    try:
        payload_length = struct.unpack(_LENGTH_FORMAT, raw_header)[0]
    except Exception as e:
        raise ValueError(f"Invalid message header: {e}")

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


def make_list_files():
    return {"type": "LIST_FILES"}

def make_file_list(files):
    return {"type": "FILE_LIST", "files": files}

def make_request_file(filename):
    return {"type": "REQUEST_FILE", "filename": filename}

def make_approved(filename, size, checksum):
    return {"type": "APPROVED", "filename": filename, "size": size, "checksum": checksum}

def make_rejected(filename, reason):
    return {"type": "REJECTED", "filename": filename, "reason": reason}

def make_chunk(index, data):
    return {"type": "FILE_CHUNK", "index": index, "data": data}

def make_done(filename):
    return {"type": "TRANSFER_DONE", "filename": filename}