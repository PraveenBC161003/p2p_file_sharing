import socket
import threading

from utils.logger import get_logger
from network.protocol import recv_message, send_message

log = get_logger("Tracker")


class Tracker:
    def __init__(self, host="0.0.0.0", port=5002):
        self.host = host
        self.port = port

        self.peers = set()  # {(ip, port)}
        self.running = False
        self.server_socket = None

    def start(self):
        self.running = True

        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(50)

        log.success(f"Tracker running on {self.host}:{self.port}")

        while self.running:
            try:
                conn, addr = self.server_socket.accept()
                threading.Thread(
                    target=self._handle_client,
                    args=(conn, addr),
                    daemon=True
                ).start()

            except Exception as e:
                log.error(f"Accept error: {e}")

    def _handle_client(self, conn, addr):
        ip = addr[0]

        try:
            message = recv_message(conn)
            msg_type = message.get("type")

            # ───────── REGISTER ─────────
            if msg_type == "REGISTER":
                port = message.get("port")

                if port:
                    self.peers.add((ip, port))
                    log.info(f"Registered: {ip}:{port}")

                    send_message(conn, {"type": "ACK"})
                else:
                    send_message(conn, {"type": "ERROR", "reason": "Missing port"})

            # ───────── GET PEERS ─────────
            elif msg_type == "GET_PEERS":
                peer_list = [
                    {"host": p[0], "port": p[1]}
                    for p in self.peers
                ]

                send_message(conn, {
                    "type": "PEER_LIST",
                    "peers": peer_list
                })

            # ───────── DEREGISTER ─────────
            elif msg_type == "DEREGISTER":
                port = message.get("port")

                if (ip, port) in self.peers:
                    self.peers.remove((ip, port))
                    log.info(f"Deregistered: {ip}:{port}")

                send_message(conn, {"type": "ACK"})

            else:
                send_message(conn, {"type": "ERROR", "reason": "Unknown type"})

        except Exception as e:
            log.warn(f"Client error {addr}: {e}")

        finally:
            conn.close()

    def stop(self):
        self.running = False
        if self.server_socket:
            self.server_socket.close()
        log.info("Tracker stopped")


if __name__ == "__main__":
    tracker = Tracker()
    try:
        tracker.start()
    except KeyboardInterrupt:
        tracker.stop()
