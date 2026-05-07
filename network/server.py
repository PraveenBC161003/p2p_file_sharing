import socket
import threading
from typing import Callable, Dict, Optional
from concurrent.futures import ThreadPoolExecutor

from utils.logger import get_logger
from network.protocol import recv_message

log = get_logger("Server")

Handler = Callable[[socket.socket, dict], None]

class PeerServer:
    def __init__(self, host: str = "0.0.0.0", port: int = 5000, max_workers: int = 10):
        self.host = host
        self.port = port
        self.handlers: Dict[str, Handler] = {}
        self.running = False
        self.server_socket: Optional[socket.socket] = None
        self.pool = ThreadPoolExecutor(max_workers=max_workers)

    def register_handler(self, msg_type: str, handler: Handler):
        self.handlers[msg_type] = handler
        log.debug(f"Handler registered for {msg_type}")

    def start(self):
        self.running = True
        thread = threading.Thread(target=self._run, daemon=True)
        thread.start()
        log.success(f"Server starting on {self.host}:{self.port}")

    def _run(self):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.settimeout(1.0)

        try:
            self.server_socket.bind((self.host, self.port))
        except OSError as e:
            log.error(f"Failed to bind {self.host}:{self.port} → {e}")
            self.running = False
            return

        self.server_socket.listen(50)
        log.info("Listening for incoming connections...")

        while self.running:
            try:
                conn, addr = self.server_socket.accept()
                log.info(f"New connection from {addr}")
                self.pool.submit(self._handle_client, conn, addr)

            except socket.timeout:
                continue

            except Exception as e:
                log.error(f"Accept error: {e}")

    def _handle_client(self, conn: socket.socket, addr):
        """Handle incoming client connection."""
        conn.settimeout(30.0)  # Individual client socket timeout
        
        try:
            while self.running:
                try:
                    message = recv_message(conn)
                except socket.timeout:
                    log.debug(f"Receive timeout from {addr}")
                    break
                except Exception as e:
                    log.warning(f"Failed to receive from {addr}: {e}")
                    break

                if not isinstance(message, dict):
                    log.warning(f"Invalid message format from {addr}")
                    continue

                msg_type = message.get("type")
                if not msg_type:
                    log.warning(f"Message without type from {addr}")
                    continue

                # Inject requester IP so handlers (e.g. file_service) can show
                # the peer's address in approval prompts without needing the socket.
                message["_requester_ip"] = addr[0]

                handler = self.handlers.get(msg_type)
                if handler:
                    try:
                        log.debug(f"Handling {msg_type} from {addr}")
                        handler(conn, message)
                    except Exception as e:
                        log.error(f"Handler error ({msg_type}) from {addr}: {e}")
                else:
                    log.warning(f"No handler for message type: {msg_type}")

        except Exception as e:
            log.warning(f"Client handler error from {addr}: {e}")

        finally:
            try:
                conn.close()
            except Exception:
                pass
            log.debug(f"Connection closed: {addr}")

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