import threading
from utils.logger import get_logger
from network.client import PeerClient

log = get_logger("DiscoveryService")

class DiscoveryService:
    def __init__(self, port: int, tracker_host: str, tracker_port: int):
        self.port = port
        self.tracker_host = tracker_host
        self.tracker_port = tracker_port

        self.peers = []  # List of peer info dicts: {"host": "...", "port": ...}
        self._peers_lock = threading.Lock()  # Ensure thread-safe access

    def register(self):
        """Register this node with the tracker."""
        client = PeerClient(self.tracker_host, self.tracker_port, timeout=10)

        try:
            client.connect()

            message = {
                "type": "REGISTER",
                "port": self.port,
            }

            response = client.send_and_receive(message)

            if response.get("type") == "ACK":
                log.success("Registered with tracker")
            else:
                log.warn("Registration failed")

        except Exception as e:
            log.error(f"Tracker not reachable: {e}")

        finally:
            client.close()

    def refresh_peers(self):
        """Fetch latest peer list from tracker and update local cache."""
        client = PeerClient(self.tracker_host, self.tracker_port, timeout=10)

        try:
            client.connect()

            message = {
                "type": "GET_PEERS"
            }

            response = client.send_and_receive(message)

            if response.get("type") == "PEER_LIST":
                peer_list = response.get("peers", [])
                
                # Thread-safe update
                with self._peers_lock:
                    self.peers = peer_list
                
                log.info(f"Peers updated: {len(self.peers)} found")

            else:
                log.warn("Unexpected tracker response")

        except Exception as e:
            log.error(f"Failed to fetch peers: {e}")

        finally:
            client.close()

        return self.peers

    def deregister(self):
        """Deregister this node from the tracker."""
        client = PeerClient(self.tracker_host, self.tracker_port, timeout=10)

        try:
            client.connect()

            message = {
                "type": "DEREGISTER",
                "port": self.port
            }

            client.send(message)
            log.info("Deregistered from tracker")

        except Exception:
            log.warn("Tracker unavailable during deregistration")

        finally:
            client.close()

    def get_peers_safe(self):
        """Get a thread-safe copy of the peer list."""
        with self._peers_lock:
            return self.peers.copy()