import hashlib
import base64
import socket
from pathlib import Path

from utils.logger import get_logger
from utils.config import DOWNLOADS_DIR, CHUNK_SIZE, TRANSFER_TIMEOUT

from network.client import PeerClient
from network.protocol import make_request_file

log = get_logger("DownloadService")


class DownloadService:
    def __init__(self):
        DOWNLOADS_DIR.mkdir(exist_ok=True)

    def download_file(self, host: str, port: int, filename: str):
        """Download file from peer with proper error handling and timeouts."""
        client = PeerClient(host, port, timeout=TRANSFER_TIMEOUT)

        try:
            log.info(f"Connecting to {host}:{port} to download {filename}")
            client.connect()

            # Step 1: Send download request to peer
            log.debug("Sending file request...")
            client.send(make_request_file(filename))

            # Step 2: Wait for approval or rejection
            log.debug("Waiting for peer approval...")
            response = client.receive()

            if response["type"] == "REJECTED":
                # Peer explicitly refused — log the reason and stop
                reason = response.get("reason", "unknown")
                log.warning(f"Download rejected by peer: {reason}")
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

            log.info(f"Download approved: {filename} ({format_bytes(expected_size)})")

            # Step 3: Receive chunks and write to disk
            self._receive_file(client, filename, expected_size, expected_checksum)

        except socket.timeout:
            log.error(f"Connection timeout while downloading {filename}")
        except ConnectionError as e:
            log.error(f"Connection error: {e}")
        except Exception as e:
            log.error(f"Download failed: {e}")
        finally:
            client.close()

    def _receive_file(self, client: PeerClient, filename: str, expected_size: int, expected_checksum: str):
        """Receive file in chunks with proper error handling."""
        output_path = DOWNLOADS_DIR / filename

        # Tracks received chunks by index to detect gaps or out-of-order delivery
        received_chunks: dict[int, bytes] = {}
        total_received = 0
        last_chunk_index = -1

        try:
            while True:
                try:
                    message = client.receive()
                except socket.timeout:
                    log.error(f"Timeout waiting for next chunk (last received: {last_chunk_index})")
                    return

                msg_type = message.get("type")

                if msg_type == "FILE_CHUNK":
                    index = message.get("index")
                    raw   = message.get("data")

                    if index is None or raw is None:
                        # Malformed chunk — skip and keep going rather than crashing
                        log.warning("Received malformed FILE_CHUNK (missing index or data), skipping")
                        continue

                    try:
                        # Decode base64 back to original binary bytes
                        chunk_bytes = base64.b64decode(raw)
                        received_chunks[index] = chunk_bytes
                        total_received += len(chunk_bytes)
                        last_chunk_index = index
                        log.debug(f"Received chunk {index} ({format_bytes(len(chunk_bytes))})")
                    except Exception as e:
                        log.warning(f"Failed to decode chunk {index}: {e}")
                        continue

                elif msg_type == "TRANSFER_DONE":
                    log.info("Transfer complete, reassembling file...")
                    # All chunks received — now reassemble in correct order
                    if not received_chunks:
                        log.error("TRANSFER_DONE received but no chunks were collected")
                        return

                    # Sort by chunk index to reconstruct the file in the original order
                    try:
                        ordered_chunks = [received_chunks[i] for i in sorted(received_chunks)]
                    except KeyError as e:
                        log.error(f"Gap in chunk sequence: chunk {e} missing")
                        return

                    # Verify there are no gaps in chunk sequence
                    expected_indices = set(range(len(received_chunks)))
                    actual_indices   = set(received_chunks.keys())

                    if expected_indices != actual_indices:
                        missing = expected_indices - actual_indices
                        log.error(f"Missing chunks: {missing} — file may be corrupt")
                        return

                    log.info(f"Received {len(received_chunks)} chunks, writing to disk...")

                    # Write assembled file to disk
                    with output_path.open("wb") as f:
                        for chunk in ordered_chunks:
                            f.write(chunk)

                    log.info(f"File written: {output_path}")

                    # Verify file size matches expected
                    actual_size = output_path.stat().st_size
                    if actual_size != expected_size:
                        log.error(
                            f"Size mismatch for '{filename}' — "
                            f"expected {format_bytes(expected_size)}, got {format_bytes(actual_size)}. "
                            f"Deleting file."
                        )
                        output_path.unlink(missing_ok=True)
                        return

                    # Verify checksum against what the sender promised in APPROVED
                    log.info("Verifying file integrity...")
                    actual_checksum = self._compute_checksum(output_path)

                    if actual_checksum != expected_checksum:
                        log.error(
                            f"Checksum mismatch for '{filename}' — "
                            f"expected {expected_checksum[:16]}..., got {actual_checksum[:16]}... "
                            f"Deleting corrupt file."
                        )
                        output_path.unlink(missing_ok=True)
                        return

                    log.success(f"✓ Download complete and verified: {filename}")
                    break

                else:
                    # Unknown message type mid-transfer — log but keep going
                    log.warning(f"Unexpected message during transfer: {msg_type}")

        except Exception as e:
            log.error(f"Error during transfer: {e}")
            # Clean up partial file
            output_path.unlink(missing_ok=True)

    def _compute_checksum(self, file_path: Path) -> str:
        """Compute SHA-256 checksum of file."""
        # Must match the algorithm used in FileService._compute_checksum (sha256)
        h = hashlib.sha256()
        with file_path.open("rb") as f:
            for chunk in iter(lambda: f.read(CHUNK_SIZE), b""):
                h.update(chunk)
        return h.hexdigest()


def format_bytes(bytes_size: int) -> str:
    """Convert bytes to human-readable format."""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes_size < 1024:
            return f"{bytes_size:.1f} {unit}"
        bytes_size /= 1024
    return f"{bytes_size:.1f} TB"