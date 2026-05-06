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
        payload = json.dumps(message).encode("utf-8") # Python dict -> JSON string -> bytes: Required for network transmission {"type": "REQUEST_FILE", "filename": "a.txt"}
    except Exception as e:
        raise ValueError(f"Invalid message format: {e}") # Prevents sending invalid or corrupted messages

    header = struct.pack(_LENGTH_FORMAT, len(payload)) # Packs payload size into 4 bytes
    sock.sendall(header + payload) # This is crucial because sockets are a continuous stream—they don’t preserve message boundaries. "next message is N bytes long"

    log.debug(f"Sent: {message.get('type', '?')}") # Prints the message type, Helps trace network activity during debugging


def recv_exact(sock: socket.socket, size: int) -> bytes:
    data = b"" # Empty byte string to accumulate incoming data

    while len(data) < size: # Keep receiving until we get the full message [bytes == required size]
        chunk = sock.recv(size - len(data)) # Request only remaining bytes needed

        if not chunk: # If recv() returns empty (b"") -> connection is closed. This prevents infinite loops and Incomplete data
            raise ConnectionError("Connection closed by peer")

        data += chunk # Adds newly received bytes to buffer.

    return data # Once len(data) == size, return full message


def recv_message(sock: socket.socket) -> Dict: # Reads data from socket, Returns a structured Python dict
    # Read header
    raw_header = recv_exact(sock, _LENGTH_SIZE) # _LENGTH_SIZE = 4, Reads exactly 4 bytes.

    payload_length = struct.unpack(_LENGTH_FORMAT, raw_header)[0] # Converts bytes into integer, This is to know "Next message is 42 bytes long"

    # Safety check (10 MB max) - Prevents memory attacks and malicious payloads
    if payload_length > 10 * 1024 * 1024: 
        raise ValueError("Message too large")

    # Reads exactly payload_length bytes 
    raw_payload = recv_exact(sock, payload_length)

    try:
        message = json.loads(raw_payload.decode("utf-8"))
    except Exception as e: 
        raise ValueError(f"Invalid JSON received: {e}")

    log.debug(f"Received: {message.get('type', '?')}")
    return message

def make_list_files():
    return {"type": "LIST_FILES"} # Request message - What files do you have?

def make_file_list(files):
    return {"type": "FILE_LIST", "files": files} # Response message - Sent as a reply to LIST_FILES

def make_request_file(filename):
    return {
        "type": "REQUEST_FILE",
        "filename": filename
    } # Request message to download a specific file. "Give me this exact file"

def make_approved(filename, size, checksum):
    return {
        "type": "APPROVED",
        "filename": filename,
        "size": size,
        "checksum": checksum
    }

def make_rejected(filename, reason):
    return {
        "type": "REJECTED",
        "filename": filename,
        "reason": reason
    }

def make_chunk(index, data):
    return {
        "type": "FILE_CHUNK",
        "index": index,
        "data": data
    }

def make_done(filename):
    return {
        "type": "TRANSFER_DONE",
        "filename": filename
    }