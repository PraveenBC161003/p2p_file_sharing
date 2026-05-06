import hashlib
import base64
from pathlib import Path

from utils.logger import get_logger
from utils.config import SHARED_DIR, CHUNK_SIZE
from network.protocol import send_message, make_approved, make_rejected, make_chunk, make_done

log = get_logger("FileService")


class FileService:
    def __init__(self):
        SHARED_DIR.mkdir(exist_ok=True)
        log.info(f"Shared directory: {SHARED_DIR.resolve()}")

    def get_files(self):
        files = []
        for file in SHARED_DIR.iterdir():
            if file.is_file():
                files.append({
                    "filename": file.name,
                    "size": file.stat().st_size
                })
        return files

    def handle_list_files(self, conn, message):
        log.info("Sending file list")
        send_message(conn, {
            "type": "FILE_LIST",
            "files": self.get_files()
        })

    def _safe_path(self, filename: str) -> Path:
        file_path = (SHARED_DIR / filename).resolve()
        if not str(file_path).startswith(str(SHARED_DIR.resolve())):
            raise ValueError("Path traversal attempt blocked")
        return file_path

    def _compute_checksum(self, file_path: Path) -> str:
        # Reads file in chunks to avoid loading entire file into memory
        h = hashlib.sha256()
        with file_path.open("rb") as f:
            for chunk in iter(lambda: f.read(CHUNK_SIZE), b""):
                h.update(chunk)
        return h.hexdigest()

    def handle_file_request(self, conn, message):
        filename = message.get("filename")

        if not filename:
            send_message(conn, {"type": "ERROR", "reason": "Filename missing"})
            return

        # Step 1: Validate filename is in shared list
        available_files = [f["filename"] for f in self.get_files()]

        if filename not in available_files:
            log.warning(f"Blocked request (not shared): {filename}")
            send_message(conn, make_rejected(filename, "File not shared"))
            return

        # Step 2: Safe path resolution — prevents directory traversal attacks
        try:
            file_path = self._safe_path(filename)
        except ValueError:
            log.warning(f"Blocked path traversal: {filename}")
            send_message(conn, make_rejected(filename, "Invalid file path"))
            return

        # Step 3: Final existence check
        if not file_path.exists():
            log.warning(f"File missing on disk: {filename}")
            send_message(conn, make_rejected(filename, "File not found"))
            return

        log.info(f"Approved file transfer: {filename}")

        # Step 4: Send approval with checksum so receiver can verify integrity
        checksum = self._compute_checksum(file_path)
        send_message(conn, make_approved(filename, file_path.stat().st_size, checksum))

        # Step 5: Send file chunks
        self._send_file(conn, file_path)

    def _send_file(self, conn, file_path: Path):
        with file_path.open("rb") as f:
            chunk_index = 0
            while True:
                chunk = f.read(CHUNK_SIZE)
                if not chunk:
                    break

                # base64 encode binary chunk into a safe ASCII string for JSON transport
                encoded = base64.b64encode(chunk).decode()
                send_message(conn, make_chunk(chunk_index, encoded))

                log.debug(f"Sent chunk {chunk_index}")
                chunk_index += 1

        send_message(conn, make_done(file_path.name))
        log.success(f"File sent: {file_path.name}")