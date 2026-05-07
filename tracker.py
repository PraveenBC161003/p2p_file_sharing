import socket
import threading
import time

from utils.logger import get_logger
from network.protocol import recv_message, send_message

log = get_logger("Tracker")

# How long (seconds) a peer registration stays valid without a heartbeat.
# Peers should re-register before this window expires (e.g. every 30 s).
PEER_TTL = 90.0
class Tracker:
    def __init__(self, host="0.0.0.0", port=5002):
        self.host = host
        self.port = port

        # peers: { (ip, port) -> last_seen_timestamp }
        # Using a dict lets us update the timestamp on every REGISTER/heartbeat
        # and expire entries that haven't checked in within PEER_TTL seconds.
        self.peers: dict[tuple, float] = {}
        self.running = False
        self.server_socket = None
        self._peers_lock = threading.Lock()

    # Lifecycle 

    def start(self):
        self.running = True

        # Background thread that removes peers whose TTL has expired
        reaper = threading.Thread(target=self._reap_dead_peers, daemon=True)
        reaper.start()

        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.settimeout(1.0)   # allows clean shutdown check

        try:
            self.server_socket.bind((self.host, self.port))
        except OSError as e:
            log.error(f"Failed to bind {self.host}:{self.port} → {e}")
            self.running = False
            return

        self.server_socket.listen(50)
        log.success(f"Tracker running on {self.host}:{self.port} (peer TTL={PEER_TTL}s)")

        while self.running:
            try:
                conn, addr = self.server_socket.accept()
                threading.Thread(
                    target=self._handle_client,
                    args=(conn, addr),
                    daemon=True
                ).start()

            except socket.timeout:
                continue   # normal — just re-check self.running

            except Exception as e:
                if self.running:
                    log.error(f"Accept error: {e}")

    def stop(self):
        self.running = False
        if self.server_socket:
            try:
                self.server_socket.shutdown(socket.SHUT_RDWR)
            except Exception:
                pass
            try:
                self.server_socket.close()
            except Exception:
                pass
        log.info("Tracker stopped")

    # Reaper 

    def _reap_dead_peers(self):
        while self.running:
            time.sleep(PEER_TTL / 3)   # check three times per TTL window
            now = time.time()
            with self._peers_lock:
                dead = [k for k, ts in self.peers.items() if now - ts > PEER_TTL]
                for key in dead:
                    del self.peers[key]
                    log.info(f"Expired peer: {key[0]}:{key[1]}")

    def _handle_client(self, conn: socket.socket, addr):
        ip = addr[0]

        try:
            conn.settimeout(10.0)
            message = recv_message(conn)
            msg_type = message.get("type")

            # REGISTER 
            if msg_type == "REGISTER":
                port = message.get("port")

                if not port:
                    log.warn(f"Register failed — missing port from {ip}")
                    send_message(conn, {"type": "ERROR", "reason": "Missing port"})
                    return

                with self._peers_lock:
                    self.peers[(ip, port)] = time.time()

                total = len(self.peers)
                log.success(f"Registered: {ip}:{port}  (total peers: {total})")
                send_message(conn, {"type": "ACK"})

            # GET_PEERS
            elif msg_type == "GET_PEERS":
                with self._peers_lock:
                    peer_list = [
                        {"host": p[0], "port": p[1]}
                        for p in self.peers
                    ]

                log.debug(f"Peer list sent to {ip}: {len(peer_list)} peer(s)")
                send_message(conn, {
                    "type":  "PEER_LIST",
                    "peers": peer_list,
                })

            # DEREGISTER
            elif msg_type == "DEREGISTER":
                port = message.get("port")

                with self._peers_lock:
                    key = (ip, port)
                    if key in self.peers:
                        del self.peers[key]
                        log.info(f"Deregistered: {ip}:{port}  (total peers: {len(self.peers)})")
                    else:
                        log.warn(f"Deregister — peer not found: {ip}:{port}")

                send_message(conn, {"type": "ACK"})

            # HEARTBEAT
            elif msg_type == "HEARTBEAT":
                port = message.get("port")

                with self._peers_lock:
                    key = (ip, port)
                    if key in self.peers:
                        self.peers[key] = time.time()
                        log.debug(f"Heartbeat: {ip}:{port}")
                    else:
                        # Peer wasn't registered yet — register it now
                        self.peers[key] = time.time()
                        log.info(f"Heartbeat auto-registered: {ip}:{port}")

                send_message(conn, {"type": "ACK"})

            else:
                log.warn(f"Unknown message type from {ip}: {msg_type}")
                send_message(conn, {"type": "ERROR", "reason": "Unknown type"})

        except Exception as e:
            log.warn(f"Client error {addr}: {e}")

        finally:
            try:
                conn.close()
            except Exception:
                pass


if __name__ == "__main__":
    tracker = Tracker()
    try:
        tracker.start()
    except KeyboardInterrupt:
        tracker.stop()