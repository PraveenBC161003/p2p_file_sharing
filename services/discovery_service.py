import socket
import threading
import time

from utils.logger import get_logger
from network.client import PeerClient

log = get_logger("DiscoveryService")

# How often to send a heartbeat to the tracker (seconds).
# Must be less than the tracker's PEER_TTL (90 s).
HEARTBEAT_INTERVAL = 30.0


def get_lan_ip() -> str:
    """
    Determine the machine's LAN IP address by opening a UDP socket toward
    the tracker and reading the local interface that the OS selects.

    This does NOT send any packets — connect() on UDP just sets the routing
    information without a handshake.  Falls back to "127.0.0.1" if the
    machine has no default route (e.g. offline).
    """
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(2)
        # Any reachable address works; we only care about the local side.
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


class DiscoveryService:
    def __init__(self, port: int, tracker_host: str, tracker_port: int):
        self.port         = port
        self.tracker_host = tracker_host
        self.tracker_port = tracker_port

        # Detect the LAN IP once at startup so we can:
        #   1. Filter ourself out of the peer list returned by the tracker.
        #   2. Log what address we are reachable on.
        self.local_ip = get_lan_ip()
        log.info(f"Local LAN IP detected: {self.local_ip}  port: {self.port}")

        self.peers: list[dict] = []   # [{"host": "...", "port": ...}, ...]
        self._peers_lock = threading.Lock()

        self._heartbeat_thread: threading.Thread | None = None
        self._running = False

    # ── Registration ──────────────────────────────────────────────────────────

    def register(self):
        """Register this node with the tracker and start the heartbeat loop."""
        self._send_register()

        # Start background heartbeat so the tracker never expires us
        self._running = True
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            daemon=True,
            name="DiscoveryHeartbeat"
        )
        self._heartbeat_thread.start()

    def _send_register(self):
        """Send a single REGISTER message to the tracker."""
        client = PeerClient(self.tracker_host, self.tracker_port, timeout=10)
        try:
            client.connect()
            response = client.send_and_receive({"type": "REGISTER", "port": self.port})

            if response.get("type") == "ACK":
                log.success(f"Registered with tracker as {self.local_ip}:{self.port}")
            else:
                log.warn(f"Unexpected registration response: {response}")

        except Exception as e:
            log.error(f"Tracker registration failed: {e}")
        finally:
            client.close()

    # ── Heartbeat ─────────────────────────────────────────────────────────────

    def _heartbeat_loop(self):
        """Periodically send HEARTBEAT so the tracker doesn't expire us."""
        while self._running:
            time.sleep(HEARTBEAT_INTERVAL)
            if not self._running:
                break
            try:
                client = PeerClient(self.tracker_host, self.tracker_port, timeout=10)
                client.connect()
                response = client.send_and_receive({
                    "type": "HEARTBEAT",
                    "port": self.port
                })
                if response.get("type") == "ACK":
                    log.debug("Heartbeat acknowledged by tracker")
                else:
                    log.warn(f"Unexpected heartbeat response: {response}")
            except Exception as e:
                log.warn(f"Heartbeat failed (will retry in {HEARTBEAT_INTERVAL}s): {e}")
            finally:
                try:
                    client.close()
                except Exception:
                    pass

    # ── Peer discovery ────────────────────────────────────────────────────────

    def refresh_peers(self) -> list[dict]:
        """
        Fetch the latest peer list from the tracker, filter out this node,
        and update the local cache.
        """
        client = PeerClient(self.tracker_host, self.tracker_port, timeout=10)
        try:
            client.connect()
            response = client.send_and_receive({"type": "GET_PEERS"})

            if response.get("type") != "PEER_LIST":
                log.warn(f"Unexpected tracker response: {response.get('type')}")
                return self.peers

            all_peers: list[dict] = response.get("peers", [])

            # ── KEY FIX: remove self from peer list ──────────────────────────
            # The tracker includes us in its list. Without this filter, we
            # would try to download files from ourselves, which succeeds
            # locally (same machine) but makes no sense in real use, and
            # can cause phantom "peers" in the CLI output.
            filtered = [
                p for p in all_peers
                if not (p.get("host") == self.local_ip and p.get("port") == self.port)
            ]

            removed = len(all_peers) - len(filtered)
            if removed:
                log.debug(f"Filtered out {removed} self-entry/entries from peer list")

            with self._peers_lock:
                self.peers = filtered

            log.info(f"Peers refreshed: {len(self.peers)} remote peer(s) found")
            for p in self.peers:
                log.debug(f"  Peer: {p['host']}:{p['port']}")

        except Exception as e:
            log.error(f"Failed to fetch peers from tracker: {e}")
        finally:
            client.close()

        return self.get_peers_safe()

    # ── Deregistration ────────────────────────────────────────────────────────

    def deregister(self):
        """Stop heartbeat and remove this node from the tracker."""
        self._running = False

        client = PeerClient(self.tracker_host, self.tracker_port, timeout=10)
        try:
            client.connect()
            client.send({"type": "DEREGISTER", "port": self.port})
            log.info(f"Deregistered from tracker ({self.local_ip}:{self.port})")
        except Exception:
            log.warn("Tracker unavailable during deregistration — peer will expire via TTL")
        finally:
            client.close()

    # ── Thread-safe access ────────────────────────────────────────────────────

    def get_peers_safe(self) -> list[dict]:
        """Return a thread-safe snapshot of the current peer list."""
        with self._peers_lock:
            return self.peers.copy()