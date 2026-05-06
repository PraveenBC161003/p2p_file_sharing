import base64
from pathlib import Path

from utils.logger import get_logger
from utils.config import DOWNLOADS_DIR

from network.client import PeerClient
from network.protocol import make_request_file

log = get_logger("DownloadService")


class DownloadService:
    # Initialization -> Ensures a folder exists for downloads. This prevents crash when saving files, Makes system ready before any download starts.
    # Basically a silent setup to avoid runtime errors
    def __init__(self):
        DOWNLOADS_DIR.mkdir(exist_ok=True)

    def download_file(self, host: str, port: int, filename: str): # This is the orchestrator method that manages the entire download process. 
        # It abstracts away the complexities of network communication and file handling, providing a simple interface for initiating downloads.
        # self -> current object instance
        # host -> IP/hostname of the peer
        # port -> peer's listening port
        # filename -> file you want to download from the peer -> This is the entry point for downloading a file

        client = PeerClient(host, port)
        # Creates a client object to communicate with another peer, Stores target peer details (host + port)

        try:
            client.connect() # Opens a socket connection to the peer, Establishes communication channel -> No data exchange is possible without this

            # Step 1: Send request
            client.send(make_request_file(filename)) # creates request message and sends it to the peer. {"type": "REQUEST_FILE", "filename": "file.txt"} 
            # Basically -> Do you allow me to download this file?

            # Step 2: Wait for approval
            response = client.receive() # Waits for peer's reply and Stores response message -> This is a blocking call, The peer must respond for the download to proceed.
            
            if response["type"] == "REJECTED": # Checks if the peer denied the request
                log.warn(f"Download rejected: {response.get('reason')}")
                return

            if response["type"] != "APPROVED": # Handles unexpected responses 
                log.error("Unexpected response")
                return

            log.info(f"Download approved: {filename}") # Confirms peer accepted the request, Signals start of transfer phase

            # Step 3: Receive file
            self._receive_file(client, filename)

        finally:
            client.close()

    def _receive_file(self, client: PeerClient, filename: str):
        # client -> active connection to peer
        # filename -> name of file to save
        # Called only after download is approved

        output_path = DOWNLOADS_DIR / filename # Decides where the file will be stored

        with output_path.open("wb") as f: # Opens file in write binary mode, It ensures binary safe warnings, overwrite existing files. This prepares file for writing chunks

            while True: # Loops until the transfer is complete.
                message = client.receive() # Extracts message type safely, also prevents crash if "type" is missing

                msg_type = message.get("type")

                if msg_type == "FILE_CHUNK":
                    data = base64.b64decode(message["data"]) # base62 - Original binary data is encoded to text for safe transmission, Now we decode it back to binary before writing to disk
                    f.write(data)

                elif msg_type == "TRANSFER_DONE":
                    log.success(f"Download complete: {filename}")
                    break # Exits loop -> File is fully reconstructed and saved

                else:
                    log.warn(f"Unexpected message: {msg_type}") # Handles unknown message types