from pathlib import Path

BASE_DIR:      Path = Path(__file__).resolve().parent.parent
DOWNLOADS_DIR: Path = BASE_DIR / "downloads"
SHARED_DIR:    Path = BASE_DIR / "shared_files"
TEMP_DIR:      Path = BASE_DIR / ".tmp_transfers"

for _d in (DOWNLOADS_DIR, SHARED_DIR, TEMP_DIR):
    _d.mkdir(parents=True, exist_ok=True)

DEFAULT_PORT:  int   = 5000
TRACKER_PORT:  int   = 5002
BACKLOG:       int   = 5
SOCKET_TIMEOUT: float = 30.0
CHUNK_SIZE:    int   = 512 * 1024
TRANSFER_TIMEOUT: float = 360.0  # must exceed server-side APPROVAL_TIMEOUT (300 s)
CHECKSUM_ALGORITHM: str = "sha256"

MSG_REGISTER    = "REGISTER"
MSG_DEREGISTER  = "DEREGISTER"
MSG_GET_PEERS   = "GET_PEERS"
MSG_PEER_LIST   = "PEER_LIST"
MSG_LIST_FILES  = "LIST_FILES"
MSG_FILE_LIST   = "FILE_LIST"
MSG_REQUEST_FILE   = "REQUEST_FILE"
MSG_APPROVED       = "APPROVED"
MSG_REJECTED       = "REJECTED"
MSG_TRANSFER_START = "TRANSFER_START"
MSG_TRANSFER_DONE  = "TRANSFER_DONE"
MSG_FILE_CHUNK     = "FILE_CHUNK"
MSG_ACK   = "ACK"
MSG_ERROR = "ERROR"