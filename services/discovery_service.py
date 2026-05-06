from utils.logger import get_logger
from network.client import PeerClient

log = get_logger("DiscoveryService")

class DiscoveryService:
    def __init__(self, port: int, tracker_host: str, tracker_port: int):
        self.port = port
        self.tracker_host = tracker_host
        self.tracker_port = tracker_port

        self.peers = []

    def register(self):
        client = PeerClient(self.tracker_host, self.tracker_port)

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
        client = PeerClient(self.tracker_host, self.tracker_port)

        try:
            client.connect()

            message = {
                "type": "GET_PEERS"
            }

            response = client.send_and_receive(message)

            if response.get("type") == "PEER_LIST":
                self.peers = response.get("peers", [])
                log.info(f"Peers updated: {len(self.peers)} found")

            else:
                log.warn("Unexpected tracker response")

        except Exception as e:
            log.error(f"Failed to fetch peers: {e}")

        finally:
            client.close()

        return self.peers

    def deregister(self):
        client = PeerClient(self.tracker_host, self.tracker_port)

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