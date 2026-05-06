import hashlib
import base64
from pathlib import Path

from utils.logger import get_logger
from utils.config import DOWNLOADS_DIR, CHUNK_SIZE

from network.client import PeerClient
from network.protocol import make_request_file

log = get_logger("DownloadService")


class DownloadService:
    def __init__(self):
        DOWNLOADS_DIR.mkdir(exist_ok=True)

    def download_file(self, host: str, port: int, filename: str):
        client = PeerClient(host, port)

        try:
            client.connect()

            # Step 1: Send download request to peer
            client.send(make_request_file(filename))

            # Step 2: Wait for approval or rejection
            response = client.receive()

            if response["type"] == "REJECTED":
                # Peer explicitly refused — log the reason and stop
                log.warning(f"Download rejected: {response.get('reason')}")
                return

            if response["type"] != "APPROVED":
                # Received something unexpected instead of APPROVED/REJECTED — abort
                log.error(f"Unexpected response type: {response.get('type')}")
                return

            # Extract metadata from approval message
            expected_size     = response.get("size")
            expected_checksum = response.get("checksum")

            if not expected_checksum:
                # Sender did not provide a checksum — cannot verify integrity after download
                log.error("APPROVED message missing checksum — aborting for safety")
                return

            log.info(f"Download approved: {filename} ({expected_size} bytes)")

            # Step 3: Receive chunks and write to disk
            self._receive_file(client, filename, expected_checksum)

        finally:
            client.close()

    def _receive_file(self, client: PeerClient, filename: str, expected_checksum: str):
        output_path = DOWNLOADS_DIR / filename

        # Tracks received chunks by index to detect gaps or out-of-order delivery
        received_chunks: dict[int, bytes] = {}

        while True:
            message = client.receive()
            msg_type = message.get("type")

            if msg_type == "FILE_CHUNK":
                index = message.get("index")
                raw   = message.get("data")

                if index is None or raw is None:
                    # Malformed chunk — skip and keep going rather than crashing
                    log.warning("Received malformed FILE_CHUNK (missing index or data), skipping")
                    continue

                # Decode base64 back to original binary bytes
                chunk_bytes = base64.b64decode(raw)
                received_chunks[index] = chunk_bytes
                log.debug(f"Received chunk {index}")

            elif msg_type == "TRANSFER_DONE":
                # All chunks received — now reassemble in correct order
                if not received_chunks:
                    log.error("TRANSFER_DONE received but no chunks were collected")
                    return

                # Sort by chunk index to reconstruct the file in the original order
                ordered_chunks = [received_chunks[i] for i in sorted(received_chunks)]

                # Verify there are no gaps in chunk sequence
                expected_indices = set(range(len(received_chunks)))
                actual_indices   = set(received_chunks.keys())

                if expected_indices != actual_indices:
                    missing = expected_indices - actual_indices
                    log.error(f"Missing chunks: {missing} — file may be corrupt")
                    return

                # Write assembled file to disk
                with output_path.open("wb") as f:
                    for chunk in ordered_chunks:
                        f.write(chunk)

                # Verify checksum against what the sender promised in APPROVED
                actual_checksum = self._compute_checksum(output_path)

                if actual_checksum != expected_checksum:
                    log.error(
                        f"Checksum mismatch for '{filename}' — "
                        f"expected {expected_checksum}, got {actual_checksum}. "
                        f"Deleting corrupt file."
                    )
                    output_path.unlink(missing_ok=True)
                    return

                log.success(f"Download complete and verified: {filename}")
                break

            else:
                # Unknown message type mid-transfer — log but keep going
                log.warning(f"Unexpected message during transfer: {msg_type}")

    def _compute_checksum(self, file_path: Path) -> str:
        # Must match the algorithm used in FileService._compute_checksum (sha256)
        h = hashlib.sha256()
        with file_path.open("rb") as f:
            for chunk in iter(lambda: f.read(CHUNK_SIZE), b""):
                h.update(chunk)
        return h.hexdigest()