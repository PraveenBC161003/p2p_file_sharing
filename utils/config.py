from pathlib import Path

# Root directory of the project
BASE_DIR: Path = Path(__file__).resolve().parent.parent

# Directory where downloaded files will be stored
DOWNLOADS_DIR: Path = BASE_DIR / "downloads"

# Directory where user keeps files to share
SHARED_DIR: Path = BASE_DIR / "shared_files"

# Temporary directory for partial downloads (resume support)
TEMP_DIR: Path = BASE_DIR / ".tmp_transfers"


# Ensure all directories exist at runtime
for directory in (DOWNLOADS_DIR, SHARED_DIR, TEMP_DIR):
    directory.mkdir(parents=True, exist_ok=True)



DEFAULT_PORT: int = 5000
TRACKER_PORT: int = 5002

# Max pending connections
BACKLOG: int = 5

# Timeout for socket operations
SOCKET_TIMEOUT: float = 30.0


# Chunk size for file transfer (5`12` KB)
CHUNK_SIZE: int = 512 * 1024

# Timeout while waiting for next chunk
TRANSFER_TIMEOUT: float = 60.0

# Checksum algorithm
CHECKSUM_ALGORITHM: str = "sha256"

# Discovery
MSG_REGISTER    = "REGISTER"
MSG_DEREGISTER  = "DEREGISTER"
MSG_GET_PEERS   = "GET_PEERS"
MSG_PEER_LIST   = "PEER_LIST"

# File listing
MSG_LIST_FILES  = "LIST_FILES"
MSG_FILE_LIST   = "FILE_LIST"

# Transfer
MSG_REQUEST_FILE   = "REQUEST_FILE"
MSG_APPROVED       = "APPROVED"
MSG_REJECTED       = "REJECTED"
MSG_TRANSFER_START = "TRANSFER_START"
MSG_TRANSFER_DONE  = "TRANSFER_DONE"

# Chunk transfer
MSG_FILE_CHUNK     = "FILE_CHUNK"

# Generic responses
MSG_ACK   = "ACK"
MSG_ERROR = "ERROR"