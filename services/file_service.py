import hashlib
import base64
import queue
import threading
from pathlib import Path

from utils.logger import get_logger
from utils.config import SHARED_DIR, CHUNK_SIZE
from network.client import PeerClient
from network.protocol import (
    send_message,
    make_approved,
    make_rejected,
    make_chunk,
    make_done,
    make_list_files,
)

log = get_logger("FileService")


def format_bytes(bytes_size: int) -> str:
    """Convert bytes to human-readable format."""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes_size < 1024:
            return f"{bytes_size:.1f} {unit}"
        bytes_size /= 1024
    return f"{bytes_size:.1f} TB"


class FileService:
    def __init__(self):
        SHARED_DIR.mkdir(exist_ok=True)
        log.info(f"Shared directory: {SHARED_DIR.resolve()}")

        # Incoming file requests waiting for user approval sit here.
        # The background handler thread puts items in; the CLI thread reads them.
        self.approval_queue = queue.Queue()

    # ── Local helpers ─────────────────────────────────────────────────────────

    def get_files(self) -> list[dict]:
        """Return metadata for every file in the local shared directory."""
        files = []
        for file in SHARED_DIR.iterdir():
            if file.is_file():
                files.append({
                    "filename": file.name,
                    "size":     file.stat().st_size,
                })
        return files

    def _safe_path(self, filename: str) -> Path:
        """Resolve filename inside SHARED_DIR, blocking directory traversal."""
        file_path = (SHARED_DIR / filename).resolve()
        if not str(file_path).startswith(str(SHARED_DIR.resolve())):
            raise ValueError("Path traversal attempt blocked")
        return file_path

    def _compute_checksum(self, file_path: Path) -> str:
        """SHA-256 over the full file read in CHUNK_SIZE pieces."""
        h = hashlib.sha256()
        with file_path.open("rb") as f:
            for chunk in iter(lambda: f.read(CHUNK_SIZE), b""):
                h.update(chunk)
        return h.hexdigest()

    # ── Client-side: remote listing ───────────────────────────────────────────

    def list_remote_files(self, host: str, port: int) -> list[dict]:
        """
        Open a connection to a peer, send LIST_FILES, and return the file list.

        This is the client-side counterpart to handle_list_files().  Both ends
        of the LIST_FILES protocol now live in the same service class, which
        keeps the symmetric request/response logic together and makes it easy to
        extend (e.g. add filtering) without touching multiple files.

        Returns an empty list on any network or protocol error so that the
        caller (node.list_peer_files) can continue with the remaining peers.
        """
        client = PeerClient(host, port, timeout=10)
        try:
            client.connect()
            client.send(make_list_files())

            response = client.receive()

            if response.get("type") != "FILE_LIST":
                log.warning(
                    f"Unexpected response from {host}:{port} "
                    f"(expected FILE_LIST, got {response.get('type')})"
                )
                return []

            files = response.get("files", [])
            log.debug(f"Listed {len(files)} file(s) from {host}:{port}")
            return files

        except Exception as e:
            # A single unreachable peer must not abort the whole listing.
            log.warning(f"Could not list files from {host}:{port} — {e}")
            return []

        finally:
            client.close()

    # ── Server-side: respond to incoming LIST_FILES ───────────────────────────

    def handle_list_files(self, conn, message):
        """Respond to an incoming LIST_FILES request with a FILE_LIST message."""
        log.info(f"Peer requesting file list")
        try:
            send_message(conn, {
                "type":  "FILE_LIST",
                "files": self.get_files(),
            })
            log.debug("File list sent")
        except Exception as e:
            log.error(f"Failed to send file list: {e}")

    # ── Server-side: respond to incoming REQUEST_FILE ─────────────────────────

    def handle_file_request(self, conn, message):
        """Handle incoming file request with user approval."""
        filename  = message.get("filename")
        requester = message.get("_requester_ip", "unknown")

        if not filename:
            log.warning("File request with missing filename")
            send_message(conn, {"type": "ERROR", "reason": "Filename missing"})
            return

        log.info(f"Peer {requester} requesting file: {filename}")

        # Step 1: File must exist in the shared folder.
        available_files = [f["filename"] for f in self.get_files()]
        if filename not in available_files:
            log.warning(f"Blocked request (not shared): {filename}")
            send_message(conn, make_rejected(filename, "File not shared"))
            return

        # Step 2: Resolve path safely — blocks traversal attempts.
        try:
            file_path = self._safe_path(filename)
        except ValueError:
            log.warning(f"Blocked path traversal: {filename}")
            send_message(conn, make_rejected(filename, "Invalid file path"))
            return

        # Step 3: Confirm file still exists on disk.
        if not file_path.exists():
            log.warning(f"File missing on disk: {filename}")
            send_message(conn, make_rejected(filename, "File not found"))
            return

        # Step 4: Push to CLI thread for manual approval.
        result_event = threading.Event()
        result_box   = [None]

        self.approval_queue.put({
            "filename":     filename,
            "requester":    requester,
            "file_path":    file_path,
            "result_event": result_event,
            "result_box":   result_box,
        })

        # Block until the CLI thread sets the event after user input.
        result_event.wait()

        if result_box[0] == "approved":
            log.info(f"Sending file: {filename} to {requester}")
            checksum = self._compute_checksum(file_path)
            file_size = file_path.stat().st_size
            send_message(conn, make_approved(filename, file_size, checksum))
            self._send_file(conn, file_path)
        else:
            log.info(f"Rejected: {filename} requested by {requester}")
            send_message(conn, make_rejected(filename, "Rejected by user"))

    def _send_file(self, conn, file_path: Path):
        """Send file to peer in chunks with proper error handling."""
        file_size = file_path.stat().st_size
        log.info(f"Starting transfer of {file_path.name} ({format_bytes(file_size)})")

        try:
            with file_path.open("rb") as f:
                chunk_index = 0
                bytes_sent = 0
                
                while True:
                    chunk = f.read(CHUNK_SIZE)
                    if not chunk:
                        break
                    
                    encoded = base64.b64encode(chunk).decode()
                    try:
                        send_message(conn, make_chunk(chunk_index, encoded))
                        bytes_sent += len(chunk)
                        progress = (bytes_sent / file_size * 100) if file_size > 0 else 0
                        log.debug(f"Sent chunk {chunk_index} ({format_bytes(len(chunk))}) [{progress:.1f}%]")
                        chunk_index += 1
                    except Exception as e:
                        log.error(f"Failed to send chunk {chunk_index}: {e}")
                        return

            send_message(conn, make_done(file_path.name))
            log.success(f"✓ File transfer complete: {file_path.name}")
            
        except Exception as e:
            log.error(f"File send error: {e}")