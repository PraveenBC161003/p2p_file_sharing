import threading
import time

from utils.logger import get_logger

from network.server import PeerServer
from services.file_service import FileService
from services.download_service import DownloadService
from services.discovery_service import DiscoveryService

log = get_logger("Node")

# How often to automatically refresh the peer list and remote file cache (seconds).
# Keeps the node up-to-date as new peers join/leave the network.
AUTO_REFRESH_INTERVAL = 60.0
class P2PNode:

    def __init__(self, port: int, tracker_host: str, tracker_port: int):
        self.port = port

        # Core components 
        self.server = PeerServer(port=port)
        self.file_service = FileService()
        self.download_service = DownloadService()
        self.discovery_service = DiscoveryService(
            port=port,
            tracker_host=tracker_host,
            tracker_port=tracker_port,
        )

        self.running = False

        # Remote file cache
        self._remote_files_cache: dict[str, dict] = {}
        self._remote_files_lock = threading.Lock()

        # Background refresh thread
        self._refresh_thread: threading.Thread | None = None

    # Lifecycle

    def start(self):
        log.info("Starting P2P Node...")

        # 1. Register handlers before the server accepts connections
        self._register_handlers()

        # 2. Start listening for inbound peer connections
        self.server.start()

        # 3. Register with tracker (also starts the heartbeat loop)
        self.discovery_service.register()

        # 4. Fetch initial peer list
        # Give the server a moment to finish binding so that if we are
        # the first peer and another node is already running, they can
        # immediately connect back to us.
        time.sleep(0.5)
        peers = self.discovery_service.refresh_peers()
        log.info(f"Initial peer count: {len(peers)}")

        self.running = True

        # 5. Populate remote file cache from all known peers
        self._refresh_remote_files()

        # 6. Start background auto-refresh so the node stays current as
        #    more peers join the network over time.
        self._refresh_thread = threading.Thread(
            target=self._auto_refresh_loop,
            daemon=True,
            name="NodeAutoRefresh"
        )
        self._refresh_thread.start()

        log.success("Node started successfully")
        log.info(f"Listening on 0.0.0.0:{self.port}")
        log.info(f"LAN IP: {self.discovery_service.local_ip}")

    def stop(self):
        log.info("Stopping node...")
        self.running = False

        self.discovery_service.deregister()
        self.server.stop()

        log.success("Node stopped")

    # Handler registration 

    def _register_handlers(self):
        self.server.register_handler("LIST_FILES",    self.file_service.handle_list_files)
        self.server.register_handler("REQUEST_FILE",  self.file_service.handle_file_request)
        log.info("Handlers registered: LIST_FILES, REQUEST_FILE")

    # Peer / file refresh

    def refresh_peers(self) -> list[dict]:
        """Public method: re-fetch peer list from tracker and rebuild file cache."""
        log.info("Refreshing peer list...")
        peers = self.discovery_service.refresh_peers()
        log.info(f"Found {len(peers)} peer(s)")
        self._refresh_remote_files()
        return peers

    def _auto_refresh_loop(self):
        while self.running:
            time.sleep(AUTO_REFRESH_INTERVAL)
            if not self.running:
                break
            try:
                log.debug("Auto-refresh: updating peer list and remote file cache...")
                self.discovery_service.refresh_peers()
                self._refresh_remote_files()
            except Exception as e:
                log.warn(f"Auto-refresh error: {e}")

    def _refresh_remote_files(self):
        peers = self.discovery_service.get_peers_safe()

        if not peers:
            log.debug("No remote peers to list files from")
            return

        log.info(f"Fetching remote file lists from {len(peers)} peer(s)...")

        # Reset cache before re-populating
        with self._remote_files_lock:
            self._remote_files_cache.clear()

        threads = []
        for idx, peer in enumerate(peers):
            t = threading.Thread(
                target=self._fetch_peer_files,
                args=(idx, peer),
                daemon=True
            )
            threads.append(t)
            t.start()

        # Wait for all concurrent fetches (with a reasonable wall-clock limit)
        for t in threads:
            t.join(timeout=15)

        with self._remote_files_lock:
            total = sum(len(d["files"]) for d in self._remote_files_cache.values())
        log.info(f"Remote file cache ready: {total} file(s) across {len(peers)} peer(s)")

    def _fetch_peer_files(self, idx: int, peer: dict):
        """Fetch and cache the file list from a single peer (runs in a thread)."""
        host = peer.get("host")
        port = peer.get("port")
        peer_id = f"{host}:{port}"

        try:
            files = self.file_service.list_remote_files(host, port)
            with self._remote_files_lock:
                self._remote_files_cache[peer_id] = {
                    "peer_index": idx,
                    "host": host,
                    "port": port,
                    "files": files,
                }
            log.debug(f"Cached {len(files)} file(s) from peer [{idx}] {peer_id}")
        except Exception as e:
            log.warn(f"Failed to list files from peer [{idx}] {peer_id}: {e}")

    # Display helpers (used by CLI)

    def get_peers_display(self) -> list[dict]:
        """Return the current peer list with stable indices for the CLI."""
        peers = self.discovery_service.get_peers_safe()
        return [
            {
                "index": idx,
                "host":  p.get("host", "unknown"),
                "port":  p.get("port", 0),
                "id":    f"{p.get('host')}:{p.get('port')}",
            }
            for idx, p in enumerate(peers)
        ]

    def get_remote_files_display(self) -> list[dict]:
        """Return all cached remote files formatted for CLI display."""
        result = []
        with self._remote_files_lock:
            for peer_id, data in self._remote_files_cache.items():
                for file_info in data.get("files", []):
                    result.append({
                        "filename":  file_info.get("filename", "unknown"),
                        "size":      file_info.get("size", 0),
                        "from_peer": data.get("peer_index"),
                        "host":      data.get("host"),
                        "port":      data.get("port"),
                    })
        return result

    # Download 

    def get_peer_for_download(self, peer_index: int) -> dict | None:
        peers = self.get_peers_display()
        if 0 <= peer_index < len(peers):
            return peers[peer_index]
        return None

    def download(self, peer_index: int, filename: str):
        peer = self.get_peer_for_download(peer_index)
        if not peer:
            log.error(
                f"Invalid peer index: {peer_index}. "
                f"Run 'peers' to see valid indices (0 – {len(self.get_peers_display()) - 1})."
            )
            return

        host = peer["host"]
        port = peer["port"]
        log.info(f"Downloading '{filename}' from peer [{peer_index}] ({host}:{port})")
        self.download_service.download_file(host, port, filename)