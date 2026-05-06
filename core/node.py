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

    def download(self, host: str, port: int, filename: str):
        # host -> IP/hostname of the target peer
        # port -> listening port of the target peer
        # filename -> file to fetch -> Defines clear contract for initiating a download

        log.info(f"Downloading '{filename}' from {host}:{port}")

        self.download_service.download_file(host, port, filename)
        # Opening a socket to the peer
        # Sending "REQUEST_FILE" message
        # Receiving file in chunks
        # Writing to disk
        # Handling retries / resume
        # Verifying integrity (checksum)

        # This Separation ensures: Node = orchestration, DownloadService = execution engine