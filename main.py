import queue
import argparse
import signal
import sys

from utils.logger import get_logger, enable_debug
from core.node import P2PNode

log = get_logger("Main")


def parse_args():
    parser = argparse.ArgumentParser(
        prog="p2p_node",
        description="CLI P2P File Sharing Node"
    )
    parser.add_argument("--port",         type=int, default=5000,          help="Port to run this node on")
    parser.add_argument("--tracker-host", type=str, default="172.16.1.202",help="Tracker host")
    parser.add_argument("--tracker-port", type=int, default=5002,          help="Tracker port")
    parser.add_argument("--debug",        action="store_true",              help="Enable debug logs")
    return parser.parse_args()


def run_cli(node: P2PNode):
    log.info("Commands: list | download <host> <port> <filename> | peers | exit")

    while True:
        try:
            # ── Check for pending approval requests BEFORE blocking on input ──
            # The background server thread puts incoming requests here.
            # We drain all pending ones so the user doesn't miss any.
            try:
                while True:
                    request = node.file_service.approval_queue.get_nowait()

                    filename  = request["filename"]
                    requester = request["requester"]
                    event     = request["result_event"]
                    result    = request["result_box"]

                    print(f"\n{'='*50}")
                    print(f"  INCOMING REQUEST")
                    print(f"  From     : {requester}")
                    print(f"  File     : {filename}")
                    print(f"{'='*50}")

                    answer = input("  Approve? (yes / no): ").strip().lower()

                    if answer == "yes":
                        result[0] = "approved"
                        print(f"  ✓ Approved — sending {filename}\n")
                    else:
                        result[0] = "rejected"
                        print(f"  ✗ Rejected — {filename} will not be sent\n")

                    # Unblocks the handler thread waiting in handle_file_request()
                    event.set()

            except queue.Empty:
                pass  # no pending requests, fall through to normal command input

            cmd = input(">> ").strip()

            if not cmd:
                continue

            if cmd == "list":
                files = node.file_service.get_files()
                if not files:
                    print("No files in shared folder.")
                else:
                    print(f"\n{'Filename':<40} {'Size':>12}")
                    print("-" * 54)
                    for f in files:
                        print(f"{f['filename']:<40} {f['size']:>10} B")
                    print()

            elif cmd.startswith("download"):
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
                print("Unknown command. Try: list | download | peers | exit")

        except KeyboardInterrupt:
            break


def main():
    args = parse_args()

    if args.debug:
        enable_debug()

    node = P2PNode(
        port=args.port,
        tracker_host=args.tracker_host,
        tracker_port=args.tracker_port
    )

    def shutdown_handler(sig, frame):
        log.info("Shutting down...")
        node.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown_handler)

    try:
        node.start()
        run_cli(node)
    finally:
        node.stop()


if __name__ == "__main__":
    main()