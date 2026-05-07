import threading
from utils.logger import get_logger

from network.server import PeerServer
from services.file_service import FileService
from services.download_service import DownloadService
from services.discovery_service import DiscoveryService

log = get_logger("Node")

# use a class (P2PNode) to group all components (server, services, config) into one controllable object that represents a single running node. 
# This makes it easier to manage state, start/stop the system, and pass around dependencies.
class P2PNode:
    
    # __init__ => Bootstrap layer. It guarantees that every instance of your class starts in a fully configured, ready-to-run state.
    def __init__(self, port: int, tracker_host: str, tracker_port: int):
        # self -> The current object
        # port -> The port number this node will listen on (for incoming peer connections)
        # tracker_host -> The hostname/IP of the tracker server (for peer discovery)
        # tracker_port -> The port number of the tracker server
 
        self.port = port # node identity in the network

        # Core components
        self.server = PeerServer(port=port) # Listens to other peers, Accepts Connects, receives file requests
        self.file_service = FileService() # List available files, Read files, Provide metadata -> data layer
        self.download_service = DownloadService() # Request files from peers, Receive file chunks, Reassemble files -> Client side transfer engine
        self.discovery_service = DiscoveryService(
            port=port,
            tracker_host=tracker_host,
            tracker_port=tracker_port
        ) # Register this peer with tracker, Discover other peers, Maintain peer list -> P2P network graph

        self.running = False # Indicates whether the node is active, Used to control lifecycle and prevent actions when stopped
        
        # Thread-safe access to peer list and remote files
        self._peers_lock = threading.Lock()
        self._remote_files_cache = {}  # {peer_id: [files]}
        self._remote_files_lock = threading.Lock()


    def start(self): # This function is a runtime orchestrator - This transitions the node from configured to actively participating in the P2P network.
        log.info("Starting P2P Node...") # Signals system boot, Useful for debugging lifecycle issues -> Observability hook

        # Step 1: Register handlers BEFORE starting server
        self._register_handlers()

        # Step 2: Start server
        self.server.start() # Opens socket, Begins listening on self.port, Accepts incoming peer connections. At this point the nodes becomes visible and reachable to other peers

        # Step 3: Register with tracker
        self.discovery_service.register() # Sends node info to tracker, IP address, Port, Availability -> This makes node discoverable

        # Step 4: Fetch peer list
        peers = self.discovery_service.refresh_peers() # Requests updated peer list from tracker, This enables Outgoing connections and File requests

        log.info(f"Connected peers available: {len(peers)}") # Gives immediate visibility into network state, Useful for debugging connectivity and discovery issues

        self.running = True # Marks node as Active
        log.success("Node started successfully") # Final confirmation
        
        # Fetch remote files from all peers
        self._refresh_remote_files()


    def stop(self):

        log.info("Stopping node...") # This is a logging call, not just output Can include metadata like: Time, Module name, Thread

        # print() => Stopping node.. => basic output
        # log.info() => [2024-06-01 12:00:00] [Node] Stopping node... => production-grade observability tool

        self.running = False

        # Deregister from tracker
        self.discovery_service.deregister()

        # Stop server
        self.server.stop()

        log.success("Node stopped")

    def _register_handlers(self): # It creates mapping between message types and functions

        self.server.register_handler(
            "LIST_FILES", # file listing logic
            self.file_service.handle_list_files 
        )
        # Request Arrives -> "LIST_FILES"
        # Server finds handler
        # calls handle_list_files()
        # Returns list of files -> This enables file discovery across peers

        # URL -> Controller
        # Message Type -> Handler Function

        self.server.register_handler(
            "REQUEST_FILE", # file sending logic
            self.file_service.handle_file_request
        )

        # Request Arrives -> "REQUEST_FILE"
        # Server finds handler
        # calls handle_file_request()
        # Reads files + sends chunks -> This powers actual file transfer

        log.info("Handlers registered")
    
    def refresh_peers(self):
        """Fetch latest peer list from tracker and refresh remote files."""
        log.info("Refreshing peer list...")
        peers = self.discovery_service.refresh_peers()
        log.info(f"Found {len(peers)} peer(s)")
        self._refresh_remote_files()
        return peers
    
    def get_peers_display(self) -> list:
        """Return list of peers with formatted info for CLI display."""
        with self._peers_lock:
            peers = self.discovery_service.peers
            result = []
            for idx, peer in enumerate(peers):
                host = peer.get("host", "unknown")
                port = peer.get("port", 0)
                peer_id = f"{host}:{port}"
                result.append({
                    "index": idx,
                    "host": host,
                    "port": port,
                    "id": peer_id
                })
            return result
    
    def _refresh_remote_files(self):
        """Fetch file list from all discovered peers and cache them."""
        with self._peers_lock:
            peers = self.discovery_service.peers.copy()
        
        with self._remote_files_lock:
            self._remote_files_cache.clear()
        
        for idx, peer in enumerate(peers):
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
                        "files": files
                    }
                log.debug(f"Cached {len(files)} file(s) from {peer_id}")
            except Exception as e:
                log.warning(f"Failed to list files from {peer_id}: {e}")
    
    def get_remote_files_display(self) -> list:
        """Return all remote files formatted for CLI display."""
        result = []
        with self._remote_files_lock:
            for peer_id, data in self._remote_files_cache.items():
                for file_info in data.get("files", []):
                    result.append({
                        "filename": file_info.get("filename", "unknown"),
                        "size": file_info.get("size", 0),
                        "from_peer": data.get("peer_index"),
                        "host": data.get("host"),
                        "port": data.get("port")
                    })
        return result
    
    def get_peer_for_download(self, peer_index: int) -> dict:
        """Get peer info by index for download operation."""
        peers = self.get_peers_display()
        if 0 <= peer_index < len(peers):
            return peers[peer_index]
        return None

    def download(self, peer_index: int, filename: str):
        """Download file from a peer using peer index."""
        peer = self.get_peer_for_download(peer_index)
        if not peer:
            log.error(f"Invalid peer index: {peer_index}")
            return
        
        host = peer.get("host")
        port = peer.get("port")
        
        log.info(f"Downloading '{filename}' from peer {peer_index} ({host}:{port})")
        self.download_service.download_file(host, port, filename)