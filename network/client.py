import socket
from typing import Optional

from utils.logger import get_logger
from network.protocol import send_message, recv_message

log = get_logger("Client")


class PeerClient:
    def __init__(self, host: str, port: int = 5000, timeout: float = 10): # self represents the current object, others are the inputs I pass
        self.host = host # Store this value inside this object
        self.port = port
        self.timeout = timeout

        self.sock: Optional[socket.socket] = None # NO connection yet. Will be set later in connect()
        self.connected = False # Not connected yet
        # Without self, values don’t belong to the object. This is crucial for managing multiple connections and keeping state.

    def connect(self):
        if self.connected:
            log.warn("Already connected")
            return # If a connection is already open, Don't create another one. Prevents resource leaks and inconsistent state.

        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.settimeout(self.timeout) # TCP Communication channel

            log.info(f"Connecting to {self.host}:{self.port}...")
            self.sock.connect((self.host, self.port)) # Performs the TCP handshake with remote peer. In success you now have a live connection
            # Limits how long operations can block, Avoids hanging forever if peer is unresponsive.

            self.connected = True # Internal flag so other methods know the socket is ready
            log.success("Connected") # Confirms connection is established

        except Exception as e:
            raise ConnectionError(f"Failed to connect: {e}") # Catches any failure (DNS issues, refused connection, timeout, etc.). Re-raises a clean, domain-level error


    def send(self, message: dict):
        self._ensure_connected() # Prevents illegal usage like sending before connect()

        try:
            send_message(self.sock, message) # Calls protocol.py {dict -> JSON -> bytes -> send over socket}. This keeps the network logic separate from business logic
        except Exception as e:
            self.connected = False # Prevents future operations from assuming the connection still works
            raise ConnectionError(f"Send failed: {e}") # Wraps low-level error into a clear, domain-level exception, Makes upstream code easier to handle


    def receive(self) -> dict:
        self._ensure_connected()

        try:
            return recv_message(self.sock) # Calls protocol.py {recv bytes -> JSON -> dict}. This keeps the network logic separate from business logic
        except Exception as e:
            self.connected = False
            raise ConnectionError(f"Receive failed: {e}")


    def send_and_receive(self, message: dict) -> dict:
        self.send(message) # First send the message, Then wait for a response. This is a common pattern for request-response interactions
        return self.receive()
    
    def close(self):
        if self.sock:
            try:
                self.sock.close()
            except Exception:
                pass

        self.connected = False
        log.info("Connection closed")

    def _ensure_connected(self):
        if not self.connected or not self.sock:
            raise RuntimeError("Not connected to peer")