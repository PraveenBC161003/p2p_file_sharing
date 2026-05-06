import argparse
import signal
import sys

from utils.logger import get_logger, enable_debug
from core.node import P2PNode

log = get_logger("Main")

# This function reads command-line inputs, applies defaults, and returns them as a structured object (args) for use in the program.
def parse_args():
    # Creates a parser object and Defines metadata for the CLI tool
    parser = argparse.ArgumentParser(
        prog="p2p_node", # Name shown in help and usage messages
        description="CLI P2P File Sharing Node" # Short description of the tool's purpose
    )

    # Define the inputs the program accepts from the command line

    parser.add_argument("--port", type=int, default=5000, help="Port to run this node on")
    # If the user doesn’t provide it, uses 5000 as the default port

    parser.add_argument("--tracker-host", type=str, default="172.16.1.202", help="Tracker host")
    # Accepts a hostname/IP for the tracker, defaulting to localhost. Converts it into a string.

    parser.add_argument("--tracker-port", type=int, default=5002, help="Tracker port")  
    # Accepts a port number for the tracker, defaulting to 5002. Converts it into an integer.

    parser.add_argument("--debug", action="store_true", help="Enable debug logs")
    # A flag that, when included, sets debug mode to True. If not included, it defaults to False.
 
    return parser.parse_args()


# This function runs a loop that continuously takes user commands and executes actions on the P2P node.
def run_cli(node: P2PNode):
    # Through this, CLI can access: file service, download service, discovery service

    log.info("Enter commands: list | download | peers | exit")
    # Displays available commands to the user

    while True:
        try:
            cmd = input(">> ").strip()

            if cmd == "list":
                # show shared files
                files = node.file_service.get_files()
                if not files:
                    print("No files available.")
                else:
                    for f in files:
                        print(f"{f['filename']} ({f['size']} bytes)")

            elif cmd.startswith("download"):
                # format: download <host> <port> <filename>
                parts = cmd.split()

                if len(parts) != 4:
                    print("Usage: download <host> <port> <filename>")
                    continue

                _, host, port, filename = parts
                node.download(host, int(port), filename)

            elif cmd == "peers":
                peers = node.discovery_service.peers
                if not peers:
                    print("No peers found.")
                else:
                    for p in peers:
                        print(p)

            elif cmd in ("exit", "quit"):
                break

            else:
                print("Unknown command")

        except KeyboardInterrupt:
            break

# Initializes the node with CLI settings, starts the system, runs the interactive CLI, and ensures clean shutdown on exit or Ctrl+C.
def main():
    args = parse_args()
    # Calls your argument parser and Produces an object [args.port, args.tracker_host, args.tracker_port, args.debug] that holds the values of the command-line arguments for use in the program.
    # runtime configuration source

    if args.debug:
        enable_debug()
    # If user ran with --debug, turns on verbose logs. Affects logger behavior globally
    # Feature toggle via CLI

    node = P2PNode(
        port=args.port,
        tracker_host=args.tracker_host,
        tracker_port=args.tracker_port
    )
    # Internally prepares: server, file_service, download_service, discovery_service
    # Important: Nothing is running yet—just objects created

    # This function runs when Ctrl+C (SIGINT) is triggered => Ensures graceful shutdown, not abrupt kill
    def shutdown_handler(sig, frame):
        log.info("Shutting down...")
        node.stop()
        sys.exit(0)

    # Handle Ctrl+C gracefully
    signal.signal(signal.SIGINT, shutdown_handler)

    try:
        node.start()
        run_cli(node)

    finally:
        node.stop()


if __name__ == "__main__":
    main()