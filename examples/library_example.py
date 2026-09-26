import sys
import os.path
sys.path.insert(0, os.path.dirname(sys.argv[0]) + "/..")

import multiconnect
from multiconnect import *

def main():
    if len(sys.argv) < 2:
        print("Usage: python toy-client.py <host> [path]", file=sys.stderr)
        print("Example: python toy-client.py example.com /", file=sys.stderr)
        sys.exit(1)

    host = sys.argv[1]
    path = sys.argv[2] if len(sys.argv) > 2 else "/"

    if not path.startswith("/"):
        path = "/" + path

    sock = get_fastest_connection([(host, 80)], msg=sys.stderr.write, diag=sys.stderr.write)
    sock = get_fastest_connection([(host, 80)], msg=sys.stderr.write, diag=sys.stderr.write)

    sys.stderr.flush()

    if sock == None:
        exit(1)

    try:
        http_request = (
            f"GET {path} HTTP/1.0\r\n"
            f"Host: {host}\r\n"
            f"User-Agent: multiconnect-client-example/1.0\r\n"
            f"Connection: close\r\n"
            f"\r\n"
        ).encode("utf-8")

        sock.sendall(http_request)

        # Receive and display response bytes
        response_data = sock.recv(65536)
        print("=== Received Response ===")
        sys.stdout.buffer.write(response_data)
        sys.stdout.buffer.flush()

    finally:
        sock.close()

if __name__ == "__main__":
    main()
