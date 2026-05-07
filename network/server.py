import socket
import threading
from typing import Callable, Dict, Optional
from concurrent.futures import ThreadPoolExecutor

from utils.logger import get_logger
from network.protocol import recv_message

log = get_logger("Server")

Handler = Callable[[socket.socket, dict], None]

# Timeout for general message receive (not for approval waits — those are
# controlled by the application-level approval_queue / result_event).
RECV_TIMEOUT = 30.0

# Timeout for the initial message from a newly accepted connection.
# Must be long enough that slow peers on a congested LAN can still connect.
CONNECT_TIMEOUT = 15.0

# Approval flows (REQUEST_FILE) can take as long as the human needs to decide.
# We give a generous window here; the CLI can timeout on its own if desired.
APPROVAL_TIMEOUT = 300.0


class PeerServer:
    def __init__(self, host: str = "0.0.0.0", port: int = 5000, max_workers: int = 20):
        # IMPORTANT: always bind on 0.0.0.0 so the server accepts connections
        # on every network interface (LAN, WiFi, loopback).
        # Never bind on 127.0.0.1 — that accepts only loopback connections
        # and is invisible to other devices on the network.
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

        # SO_REUSEADDR: lets us restart the node quickly without waiting for
        # the OS to release the port from the previous TIME_WAIT state.
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        # SO_REUSEPORT (where supported): allows multiple processes to bind
        # the same port — useful in testing but harmless in production.
        try:
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except AttributeError:
            pass  # Windows doesn't have SO_REUSEPORT

        # Short accept() timeout so we can poll self.running and shut down cleanly
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
                continue    # Normal — just re-check self.running

            except Exception as e:
                if self.running:
                    log.error(f"Accept error: {e}")

    def _handle_client(self, conn: socket.socket, addr):
        """
        Handle one peer connection.

        A single connection can carry exactly one request (LIST_FILES,
        REQUEST_FILE, etc.).  After the handler returns we close the socket.
        This matches the PeerClient pattern where each operation opens a fresh
        connection.

        The exception is REQUEST_FILE, whose handler holds the socket open
        while streaming file chunks — that's fine because the handler itself
        manages the connection for the duration of the transfer.
        """
        ip, port = addr

        # Use a longer timeout for message receipt than for plain data reads,
        # because the peer might be slow to send (e.g. large approval payloads).
        # For REQUEST_FILE the handler overrides the timeout internally.
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

            # Inject the requester's IP so handlers can display it without
            # needing access to the raw socket address.
            message["_requester_ip"] = ip

            handler = self.handlers.get(msg_type)
            if not handler:
                log.warn(f"No handler registered for message type: '{msg_type}'")
                return

            log.debug(f"Dispatching {msg_type} from {ip}:{port}")

            # For REQUEST_FILE: the handler may block a long time waiting for
            # user approval, then stream the file.  Extend the socket timeout
            # so the connection isn't killed while the user is deciding.
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