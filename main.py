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
    parser.add_argument("--port",         type=int, default=5000,           help="Port to run this node on")
    parser.add_argument("--tracker-host", type=str, default="172.16.1.202", help="Tracker host")
    parser.add_argument("--tracker-port", type=int, default=5002,           help="Tracker port")
    parser.add_argument("--debug",        action="store_true",               help="Enable debug logs")
    return parser.parse_args()


def print_help():
    print("""
  Commands:
    list              — Show your locally shared files
    remote            — Show files available on all discovered peers
    peers             — List all known peers (with index)
    refresh           — Re-fetch peer list and remote file cache from tracker
    download <peer_index> <filename>
                      — Download a file from a specific peer (use 'remote' to see indices)
    exit / quit       — Shut down this node
""")


def run_cli(node: P2PNode):
    print_help()

    while True:
        try:
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
                pass  # No pending requests — fall through to command input

            cmd = input(">> ").strip()

            if not cmd:
                continue

            # list: show LOCAL shared files
            if cmd == "list":
                files = node.file_service.get_files()
                if not files:
                    print("  No files in your shared folder.")
                else:
                    print(f"\n  {'Filename':<40} {'Size':>12}")
                    print("  " + "-" * 54)
                    for f in files:
                        print(f"  {f['filename']:<40} {f['size']:>10} B")
                    print()

            # remote: show files from ALL peers 
            elif cmd == "remote":
                remote_files = node.get_remote_files_display()
                if not remote_files:
                    print("  No remote files found. Try 'refresh' first.")
                else:
                    print(f"\n  {'Peer':>6}  {'Filename':<40} {'Size':>12}")
                    print("  " + "-" * 62)
                    for f in remote_files:
                        print(
                            f"  [{f['from_peer']:>3}]  "
                            f"{f['filename']:<40} "
                            f"{f['size']:>10} B"
                        )
                    print(f"\n  Use: download <peer_index> <filename>\n")

            # peers: show known peers with index
            elif cmd == "peers":
                peers = node.get_peers_display()
                if not peers:
                    print("  No peers found. Try 'refresh'.")
                else:
                    print(f"\n  {'Index':>6}  {'Host':<20} {'Port':>6}")
                    print("  " + "-" * 36)
                    for p in peers:
                        print(f"  [{p['index']:>3}]  {p['host']:<20} {p['port']:>6}")
                    print()

            # refresh: re-fetch peers + remote file cache
            elif cmd == "refresh":
                print("  Refreshing peer list and remote file cache...")
                peers = node.refresh_peers()
                print(f"  Found {len(peers)} peer(s).")

            # download: fetch a file from a specific peer
            elif cmd.startswith("download"):
                parts = cmd.split()
                if len(parts) != 3:
                    print("  Usage: download <peer_index> <filename>")
                    print("  Tip  : run 'remote' to see available files and peer indices")
                    continue

                _, peer_index_str, filename = parts
                try:
                    peer_index = int(peer_index_str)
                except ValueError:
                    print("  Peer index must be a number (see 'peers' or 'remote')")
                    continue

                node.download(peer_index, filename)

            # help 
            elif cmd in ("help", "?"):
                print_help()

            # exit 
            elif cmd in ("exit", "quit"):
                break

            else:
                print(f"  Unknown command: '{cmd}'. Type 'help' for available commands.")

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