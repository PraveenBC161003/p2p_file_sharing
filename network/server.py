import socket
import threading
from typing import Callable, Dict, Optional
from concurrent.futures import ThreadPoolExecutor

from utils.logger import get_logger
from network.protocol import recv_message

log = get_logger("Server")

Handler = Callable[[socket.socket, dict], None]

RECV_TIMEOUT = 30.0

CONNECT_TIMEOUT = 15.0

APPROVAL_TIMEOUT = 300.0


class PeerServer:
    def __init__(self, host: str = "0.0.0.0", port: int = 5000, max_workers: int = 20):
        self.host = host
        self.port = port
        self.handlers: Dict[str, Handler] = {}
        self.running = False
        self.server_socket: Optional[socket.socket] = None
        self.pool = ThreadPoolExecutor(max_workers=max_workers)

    def register_handler(self, msg_type: str, handler: Handler):
        self.handlers[msg_type] = handler
        log.debug(f"Handler registered: {msg_type}")

    def start(self):
        self.running = True
        thread = threading.Thread(target=self._run, daemon=True, name="PeerServer")
        thread.start()
        log.success(f"Server starting on {self.host}:{self.port}")

    def _run(self):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        try:
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except AttributeError:
            pass

        self.server_socket.settimeout(1.0)

        try:
            self.server_socket.bind((self.host, self.port))
        except OSError as e:
            log.error(f"Failed to bind {self.host}:{self.port} → {e}")
            log.error("Check: is another process already using this port?")
            self.running = False
            return

        self.server_socket.listen(50)
        log.info(f"Listening for connections on 0.0.0.0:{self.port}")

        while self.running:
            try:
                conn, addr = self.server_socket.accept()
                log.info(f"New connection from {addr[0]}:{addr[1]}")
                self.pool.submit(self._handle_client, conn, addr)

            except socket.timeout:
                continue  

            except Exception as e:
                if self.running:
                    log.error(f"Accept error: {e}")

    def _handle_client(self, conn: socket.socket, addr):
        ip, port = addr
        conn.settimeout(CONNECT_TIMEOUT)

        try:
            try:
                message = recv_message(conn)
            except socket.timeout:
                log.warn(f"Timeout waiting for first message from {ip}:{port}")
                return
            except Exception as e:
                log.warn(f"Failed to receive message from {ip}:{port}: {e}")
                return

            if not isinstance(message, dict):
                log.warn(f"Non-dict message from {ip}:{port} — ignoring")
                return

            msg_type = message.get("type")
            if not msg_type:
                log.warn(f"Message without 'type' from {ip}:{port} — ignoring")
                return
            message["_requester_ip"] = ip

            handler = self.handlers.get(msg_type)
            if not handler:
                log.warn(f"No handler registered for message type: '{msg_type}'")
                return

            log.debug(f"Dispatching {msg_type} from {ip}:{port}")

            if msg_type == "REQUEST_FILE":
                conn.settimeout(APPROVAL_TIMEOUT)

            try:
                handler(conn, message)
            except Exception as e:
                log.error(f"Handler error ({msg_type}) from {ip}:{port}: {e}")

        finally:
            try:
                conn.close()
            except Exception:
                pass
            log.debug(f"Connection closed: {ip}:{port}")

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

        self.pool.shutdown(wait=False)
        log.info("Server stopped")