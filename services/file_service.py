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

# How long to wait for a user to approve/reject before auto-rejecting.
# 5 minutes should be more than enough for a human to respond.
APPROVAL_TIMEOUT = 300.0

def format_bytes(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"

class FileService:

    def __init__(self):
        SHARED_DIR.mkdir(parents=True, exist_ok=True)
        log.info(f"Shared directory: {SHARED_DIR.resolve()}")

        # Incoming file requests waiting for user approval sit here.
        # The background server thread puts items in; the CLI thread reads them.
        self.approval_queue: queue.Queue = queue.Queue()

    # Local helpers

    def get_files(self) -> list[dict]:
        files = []
        for path in SHARED_DIR.iterdir():
            if path.is_file():
                files.append({
                    "filename": path.name,
                    "size":     path.stat().st_size,
                })
        return files

    def _safe_path(self, filename: str) -> Path:
        resolved = (SHARED_DIR / filename).resolve()
        if not str(resolved).startswith(str(SHARED_DIR.resolve())):
            raise ValueError(f"Path traversal attempt blocked: {filename!r}")
        return resolved

    def _compute_checksum(self, file_path: Path) -> str:
        """SHA-256 over the full file, read in CHUNK_SIZE pieces."""
        h = hashlib.sha256()
        with file_path.open("rb") as f:
            for chunk in iter(lambda: f.read(CHUNK_SIZE), b""):
                h.update(chunk)
        return h.hexdigest()

    # Client-side: list files on a remote peer 

    def list_remote_files(self, host: str, port: int) -> list[dict]:
        client = PeerClient(host, port, timeout=10)
        try:
            client.connect()
            client.send(make_list_files())
            response = client.receive()

            if response.get("type") != "FILE_LIST":
                log.warn(
                    f"Unexpected LIST_FILES response from {host}:{port}: "
                    f"{response.get('type')}"
                )
                return []

            files = response.get("files", [])
            log.debug(f"Listed {len(files)} file(s) from {host}:{port}")
            return files

        except Exception as e:
            log.warn(f"Could not list files from {host}:{port}: {e}")
            return []

        finally:
            client.close()

    # Server-side: respond to LIST_FILES 

    def handle_list_files(self, conn, message: dict):
        """Send our local file list to the requesting peer."""
        requester = message.get("_requester_ip", "unknown")
        log.info(f"File list requested by {requester}")
        try:
            send_message(conn, {"type": "FILE_LIST", "files": self.get_files()})
            log.debug(f"File list sent to {requester}")
        except Exception as e:
            log.error(f"Failed to send file list to {requester}: {e}")

    # Server-side: respond to REQUEST_FILE

    def handle_file_request(self, conn, message: dict):
        """Handle an incoming file request: check availability, ask user, transfer."""
        filename  = message.get("filename")
        requester = message.get("_requester_ip", "unknown")

        if not filename:
            log.warn(f"REQUEST_FILE missing filename from {requester}")
            send_message(conn, {"type": "ERROR", "reason": "Filename missing"})
            return

        log.info(f"File request from {requester}: {filename!r}")

        # 1. File must be in the shared folder
        available = [f["filename"] for f in self.get_files()]
        if filename not in available:
            log.warn(f"Not shared — rejecting request for {filename!r} from {requester}")
            send_message(conn, make_rejected(filename, "File not shared"))
            return

        # 2. Resolve path safely 
        try:
            file_path = self._safe_path(filename)
        except ValueError as e:
            log.warn(f"Path traversal blocked from {requester}: {e}")
            send_message(conn, make_rejected(filename, "Invalid file path"))
            return

        # 3. File must still exist on disk 
        if not file_path.exists():
            log.warn(f"File missing on disk: {file_path}")
            send_message(conn, make_rejected(filename, "File not found on disk"))
            return

        # 4. Ask the CLI user for approval
        result_event = threading.Event()
        result_box   = [None]

        self.approval_queue.put({
            "filename":     filename,
            "requester":    requester,
            "file_path":    file_path,
            "result_event": result_event,
            "result_box":   result_box,
        })

        log.info(
            f"Waiting for user approval (timeout={APPROVAL_TIMEOUT}s) "
            f"for {filename!r} requested by {requester}"
        )

        # Block until the CLI thread sets the event.
        # APPROVAL_TIMEOUT prevents the handler thread from waiting forever
        # if the CLI is unattended or the user never responds.
        approved = result_event.wait(timeout=APPROVAL_TIMEOUT)

        if not approved:
            # Timed out waiting for the user — auto-reject to unblock the peer
            log.warn(
                f"Approval timed out after {APPROVAL_TIMEOUT}s "
                f"for {filename!r} — auto-rejecting"
            )
            send_message(conn, make_rejected(filename, "Approval timed out"))
            return

        # 5. Send or reject based on user decision 
        if result_box[0] == "approved":
            log.info(f"Approved — sending {filename!r} to {requester}")
            checksum  = self._compute_checksum(file_path)
            file_size = file_path.stat().st_size
            send_message(conn, make_approved(filename, file_size, checksum))
            self._send_file(conn, file_path)
        else:
            log.info(f"Rejected by user — {filename!r} will not be sent to {requester}")
            send_message(conn, make_rejected(filename, "Rejected by user"))

    # File streaming 

    def _send_file(self, conn, file_path: Path):
        file_size = file_path.stat().st_size
        log.info(f"Starting transfer: {file_path.name} ({format_bytes(file_size)})")

        try:
            with file_path.open("rb") as f:
                chunk_index = 0
                bytes_sent  = 0

                while True:
                    chunk = f.read(CHUNK_SIZE)
                    if not chunk:
                        break

                    encoded = base64.b64encode(chunk).decode("utf-8")
                    send_message(conn, make_chunk(chunk_index, encoded))

                    bytes_sent += len(chunk)
                    progress    = (bytes_sent / file_size * 100) if file_size else 0
                    log.debug(
                        f"Sent chunk {chunk_index} "
                        f"({format_bytes(len(chunk))}) [{progress:.1f}%]"
                    )
                    chunk_index += 1

            send_message(conn, make_done(file_path.name))
            log.success(f"✓ Transfer complete: {file_path.name} ({chunk_index} chunks)")

        except Exception as e:
            log.error(f"Error during file transfer ({file_path.name}): {e}")
            # The connection will be closed by the server after this handler returns.