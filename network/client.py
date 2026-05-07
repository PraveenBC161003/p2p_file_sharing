import socket
from typing import Optional

from utils.logger import get_logger
from network.protocol import send_message, recv_message

log = get_logger("Client")


class PeerClient:
    def __init__(self, host: str, port: int = 5000, timeout: float = 30):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.sock: Optional[socket.socket] = None
        self.connected = False

    def connect(self):
        if self.connected:
            log.warn("Already connected")
            return
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.settimeout(self.timeout)
            log.debug(f"Connecting to {self.host}:{self.port} (timeout={self.timeout}s)...")
            self.sock.connect((self.host, self.port))
            self.connected = True
            log.debug(f"Connected to {self.host}:{self.port}")
        except socket.timeout:
            raise ConnectionError(f"Connection timeout to {self.host}:{self.port}")
        except ConnectionRefusedError:
            raise ConnectionError(f"Connection refused by {self.host}:{self.port}")
        except OSError as e:
            raise ConnectionError(f"Failed to connect to {self.host}:{self.port}: {e}")

    def send(self, message: dict):
        self._ensure_connected()
        try:
            send_message(self.sock, message)
            log.debug(f"Sent: {message.get('type', '?')}")
        except socket.timeout:
            self.connected = False
            raise ConnectionError(f"Send timeout to {self.host}:{self.port}")
        except Exception as e:
            self.connected = False
            raise ConnectionError(f"Send failed: {e}")

    def receive(self) -> dict:
        self._ensure_connected()
        try:
            message = recv_message(self.sock)
            log.debug(f"Received: {message.get('type', '?')}")
            return message
        except socket.timeout:
            self.connected = False
            raise ConnectionError(f"Receive timeout from {self.host}:{self.port}")
        except Exception as e:
            self.connected = False
            raise ConnectionError(f"Receive failed: {e}")

    def send_and_receive(self, message: dict) -> dict:
        self.send(message)
        return self.receive()

    def close(self):
        if self.sock:
            try:
                self.sock.close()
            except Exception:
                pass
        self.connected = False
        log.debug("Connection closed")

    def _ensure_connected(self):
        if not self.connected or not self.sock:
            raise RuntimeError(f"Not connected to {self.host}:{self.port}")