import base64
from pathlib import Path

from utils.logger import get_logger
from utils.config import SHARED_DIR, CHUNK_SIZE
from network.protocol import send_message

log = get_logger("FileService")

class FileService:
    def __init__(self): # Ensures shared folder exists and Logs its absolute. This defines node's public file surface
        SHARED_DIR.mkdir(exist_ok=True)
        log.info(f"Shared directory: {SHARED_DIR.resolve()}")

    def get_files(self): # Scans the shared directory and Build a list of available files [ {"filename": "a.txt", size: 1024} ]
        # Used for file listing and Validation before sending. This is the File catalog API
        files = []

        for file in SHARED_DIR.iterdir():
            if file.is_file():
                files.append({
                    "filename": file.name,
                    "size": file.stat().st_size
                })

        return files

    def handle_list_files(self, conn, message): # Responds to "LIST_FILES" request. Sends available list to peer
        log.info("Sending file list")

        # Enables file discovery across peers.
        send_message(conn, {
            "type": "FILE_LIST",
            "files": self.get_files()
        })

    def _safe_path(self, filename: str) -> Path:
        # Without this, The user could access system files, A massive security risk.
        # Resolve full path
        file_path = (SHARED_DIR / filename).resolve()

        # Ensure it stays within shared directory
        if not str(file_path).startswith(str(SHARED_DIR.resolve())):
            raise ValueError("Path traversal attempt blocked")

        return file_path

    def handle_file_request(self, conn, message):
        filename = message.get("filename")
        # Extracts requested filename

        if not filename: # If Missing -> Send error
            send_message(conn, {
                "type": "ERROR",
                "reason": "Filename missing"
            })
            return

        # Step 1: Validate filename is in shared list
        available_files = [f["filename"] for f in self.get_files()] 

        if filename not in available_files: # Even if file exists on disk -> not necessarily shared
            log.warn(f"Blocked request (not shared): {filename}")

            send_message(conn, {
                "type": "REJECTED",
                "reason": "File not shared"
            })
            return

        # Step 2: Safe path resolution
        try:
            file_path = self._safe_path(filename) # Prevents directory escapes attacks
        except ValueError:
            log.warn(f"Blocked path traversal: {filename}")

            send_message(conn, {
                "type": "REJECTED",
                "reason": "Invalid file path"
            })
            return

        # Step 3: Final existence check
        if not file_path.exists():
            log.warn(f"File missing: {filename}")

            send_message(conn, {
                "type": "REJECTED",
                "reason": "File not found"
            })
            return

        log.info(f"Approved file transfer: {filename}")

        # Step 4: Send approval
        send_message(conn, {
            "type": "APPROVED",
            "filename": filename,
            "size": file_path.stat().st_size
        })

        # Step 5: Send file
        self._send_file(conn, file_path) # Delegates the actual transfer

    def _send_file(self, conn, file_path: Path): # Exact opposite of _receive_file()

        with file_path.open("rb") as f: # Binary read mode
            chunk_index = 0

            while True:
                chunk = f.read(CHUNK_SIZE) # Reads small piece of file -> Prevents memory overload

                if not chunk:
                    break # End of file

                encoded = base64.b64encode(chunk).decode() # Converts binary to safe string -> Required for network transmission

                send_message(conn, {
                    "type": "FILE_CHUNK", # -> Protocol identifier
                    "index": chunk_index, # -> Chunk order
                    "data": encoded # -> actual content
                })

                log.debug(f"Sent chunk {chunk_index}")
                chunk_index += 1 # This tracks the chunk sequence

        send_message(conn, {
            "type": "TRANSFER_DONE",
            "filename": file_path.name
        })

        log.success(f"File sent: {file_path.name}")