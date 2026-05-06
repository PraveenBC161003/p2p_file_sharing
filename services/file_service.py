import hashlib
import base64
import queue
import threading
from pathlib import Path

from utils.logger import get_logger
from utils.config import SHARED_DIR, CHUNK_SIZE
from network.protocol import send_message, make_approved, make_rejected, make_chunk, make_done

log = get_logger("FileService")


class FileService:
    def __init__(self):
        SHARED_DIR.mkdir(exist_ok=True)
        log.info(f"Shared directory: {SHARED_DIR.resolve()}")

        # Incoming file requests waiting for user approval sit here.
        # The background handler thread puts items in; the CLI thread reads them.
        self.approval_queue = queue.Queue()

    def get_files(self):
        files = []
        for file in SHARED_DIR.iterdir():
            if file.is_file():
                files.append({
                    "filename": file.name,
                    "size":     file.stat().st_size
                })
        return files

    def handle_list_files(self, conn, message):
        log.info("Sending file list")
        send_message(conn, {
            "type":  "FILE_LIST",
            "files": self.get_files()
        })

    def _safe_path(self, filename: str) -> Path:
        # Prevent directory traversal attacks (e.g. filename = "../../etc/passwd")
        file_path = (SHARED_DIR / filename).resolve()
        if not str(file_path).startswith(str(SHARED_DIR.resolve())):
            raise ValueError("Path traversal attempt blocked")
        return file_path

    def _compute_checksum(self, file_path: Path) -> str:
        # sha256 over the full file in CHUNK_SIZE pieces to avoid memory overload
        h = hashlib.sha256()
        with file_path.open("rb") as f:
            for chunk in iter(lambda: f.read(CHUNK_SIZE), b""):
                h.update(chunk)
        return h.hexdigest()

    def handle_file_request(self, conn, message):
        filename  = message.get("filename")
        # Peer IP attached by the server so we can show it in the approval prompt
        requester = message.get("_requester_ip", "unknown")

        if not filename:
            send_message(conn, {"type": "ERROR", "reason": "Filename missing"})
            return

        # Step 1: File must exist in the shared folder
        available_files = [f["filename"] for f in self.get_files()]
        if filename not in available_files:
            log.warning(f"Blocked request (not shared): {filename}")
            send_message(conn, make_rejected(filename, "File not shared"))
            return

        # Step 2: Resolve path safely — blocks traversal attempts
        try:
            file_path = self._safe_path(filename)
        except ValueError:
            log.warning(f"Blocked path traversal: {filename}")
            send_message(conn, make_rejected(filename, "Invalid file path"))
            return

        # Step 3: Confirm file still exists on disk
        if not file_path.exists():
            log.warning(f"File missing on disk: {filename}")
            send_message(conn, make_rejected(filename, "File not found"))
            return

        # Step 4: Push request to CLI thread for manual approval.
        # result_event lets this thread sleep until the user types yes/no.
        # result_box carries the decision back (list so inner scope can write to it).
        result_event = threading.Event()
        result_box   = [None]

        self.approval_queue.put({
            "filename":     filename,
            "requester":    requester,
            "file_path":    file_path,
            "result_event": result_event,
            "result_box":   result_box
        })

        # Block this handler thread here until CLI thread calls result_event.set()
        result_event.wait()

        if result_box[0] == "approved":
            log.info(f"Sending file: {filename} to {requester}")
            checksum = self._compute_checksum(file_path)
            send_message(conn, make_approved(filename, file_path.stat().st_size, checksum))
            self._send_file(conn, file_path)
        else:
            log.info(f"Rejected: {filename} requested by {requester}")
            send_message(conn, make_rejected(filename, "Rejected by user"))

    def _send_file(self, conn, file_path: Path):
        with file_path.open("rb") as f:
            chunk_index = 0
            while True:
                chunk = f.read(CHUNK_SIZE)
                if not chunk:
                    break
                # base64 encodes binary into ASCII so it can travel inside JSON
                encoded = base64.b64encode(chunk).decode()
                send_message(conn, make_chunk(chunk_index, encoded))
                log.debug(f"Sent chunk {chunk_index}")
                chunk_index += 1

        send_message(conn, make_done(file_path.name))
        log.success(f"File sent: {file_path.name}")