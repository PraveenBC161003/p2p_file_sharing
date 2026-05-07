import hashlib
import base64
from pathlib import Path

from utils.logger import get_logger
from utils.config import DOWNLOADS_DIR, CHUNK_SIZE, TRANSFER_TIMEOUT

from network.client import PeerClient
from network.protocol import make_request_file

log = get_logger("DownloadService")


def format_bytes(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


class DownloadService:

    def __init__(self):
        DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)

    def download_file(self, host: str, port: int, filename: str):
        """Download a file from a remote peer with full error handling."""
        client = PeerClient(host, port, timeout=TRANSFER_TIMEOUT)

        try:
            log.info(f"Connecting to {host}:{port} to download {filename!r}")
            client.connect()

            # Step 1: Send download request
            log.debug("Sending REQUEST_FILE...")
            client.send(make_request_file(filename))

            # Step 2: Wait for approval or rejection
            # The remote peer's CLI user may take time to approve — use the
            # full TRANSFER_TIMEOUT here so we don't bail out prematurely.
            log.debug("Waiting for peer to approve/reject...")
            response = client.receive()

            if response.get("type") == "REJECTED":
                reason = response.get("reason", "unknown")
                log.warn(f"Download rejected by {host}:{port}: {reason}")
                return

            if response.get("type") != "APPROVED":
                log.error(
                    f"Expected APPROVED/REJECTED from {host}:{port}, "
                    f"got: {response.get('type')!r}"
                )
                return

            # Step 3: Parse approval metadata
            expected_size     = response.get("size")
            expected_checksum = response.get("checksum")

            if not expected_checksum:
                # Cannot verify integrity without a checksum — abort.
                log.error("APPROVED message is missing 'checksum' — aborting for safety")
                return

            log.info(
                f"Download approved: {filename!r} "
                f"({format_bytes(expected_size) if expected_size else 'unknown size'})"
            )

            # Step 4: Receive chunks and reassemble
            self._receive_file(client, filename, expected_size, expected_checksum)

        except ConnectionError as e:
            log.error(
                f"Connection error with {host}:{port}: {e}. "
                "Check that the peer's firewall allows inbound TCP on that port."
            )
        except Exception as e:
            log.error(f"Download failed: {e}")
        finally:
            client.close()

    def _receive_file(
        self,
        client: PeerClient,
        filename: str,
        expected_size: int,
        expected_checksum: str,
    ):
        """Receive chunked file data, reassemble, verify size and checksum."""
        output_path = DOWNLOADS_DIR / filename

        received_chunks: dict[int, bytes] = {}
        total_received  = 0
        last_chunk_index = -1

        try:
            while True:
                try:
                    message = client.receive()
                except ConnectionError as e:
                    # PeerClient.receive() converts socket.timeout → ConnectionError,
                    # so this single branch handles both timeouts and disconnects.
                    log.error(
                        f"Connection error waiting for chunk "
                        f"(last chunk index: {last_chunk_index}): {e}"
                    )
                    return

                msg_type = message.get("type")

                if msg_type == "FILE_CHUNK":
                    index = message.get("index")
                    raw   = message.get("data")

                    if index is None or raw is None:
                        log.warn("Malformed FILE_CHUNK (missing index or data) — skipping")
                        continue

                    try:
                        chunk_bytes = base64.b64decode(raw)
                        received_chunks[index] = chunk_bytes
                        total_received   += len(chunk_bytes)
                        last_chunk_index  = index

                        # Progress log every 10 chunks to avoid log spam on large files
                        if index % 10 == 0 or index == 0:
                            progress = (
                                f" [{total_received / expected_size * 100:.1f}%]"
                                if expected_size else ""
                            )
                            log.debug(
                                f"Chunk {index}: "
                                f"{format_bytes(len(chunk_bytes))} received"
                                f"{progress}"
                            )
                    except Exception as e:
                        log.warn(f"Failed to decode chunk {index}: {e}")
                        continue

                elif msg_type == "TRANSFER_DONE":
                    log.info(
                        f"Transfer complete — {len(received_chunks)} chunk(s) received, "
                        f"reassembling..."
                    )

                    if not received_chunks:
                        log.error("TRANSFER_DONE received but no chunks collected")
                        return

                    # Verify there are no gaps in the chunk sequence
                    expected_indices = set(range(len(received_chunks)))
                    actual_indices   = set(received_chunks.keys())

                    if expected_indices != actual_indices:
                        missing = sorted(expected_indices - actual_indices)
                        log.error(f"Chunk sequence has gaps — missing: {missing}")
                        return

                    # Write assembled file in original order
                    with output_path.open("wb") as f:
                        for i in sorted(received_chunks):
                            f.write(received_chunks[i])

                    log.info(f"File written to: {output_path}")

                    # Verify size 
                    actual_size = output_path.stat().st_size
                    if expected_size is not None and actual_size != expected_size:
                        log.error(
                            f"Size mismatch for {filename!r}: "
                            f"expected {format_bytes(expected_size)}, "
                            f"got {format_bytes(actual_size)} — deleting"
                        )
                        output_path.unlink(missing_ok=True)
                        return

                    # Verify checksum 
                    log.info("Verifying integrity (SHA-256)...")
                    actual_checksum = self._compute_checksum(output_path)

                    if actual_checksum != expected_checksum:
                        log.error(
                            f"Checksum mismatch for {filename!r}: "
                            f"expected {expected_checksum[:16]}..., "
                            f"got {actual_checksum[:16]}... — deleting corrupt file"
                        )
                        output_path.unlink(missing_ok=True)
                        return

                    log.success(f"✓ Download complete and verified: {filename}")
                    break

                else:
                    log.warn(f"Unexpected message type during transfer: {msg_type!r}")

        except Exception as e:
            log.error(f"Unexpected error during file receive: {e}")
            output_path.unlink(missing_ok=True)

    def _compute_checksum(self, file_path: Path) -> str:
        """SHA-256 checksum — must match algorithm used in FileService."""
        h = hashlib.sha256()
        with file_path.open("rb") as f:
            for chunk in iter(lambda: f.read(CHUNK_SIZE), b""):
                h.update(chunk)
        return h.hexdigest()