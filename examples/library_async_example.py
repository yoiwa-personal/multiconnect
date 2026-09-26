import sys
import os.path
sys.path.insert(0, os.path.dirname(sys.argv[0]) + "/..")

import multiconnect
from multiconnect import *

async def main():
    if len(sys.argv) < 2:
        print("Usage: python toy-client.py <host> [path]", file=sys.stderr)
        print("Example: python toy-client.py example.com /", file=sys.stderr)
        sys.exit(1)

    host = sys.argv[1]
    path = sys.argv[2] if len(sys.argv) > 2 else "/"

    if not path.startswith("/"):
        path = "/" + path

    reader, writer = await async_get_fastest_connection([(host, 80)], msg=sys.stderr.write, diag=sys.stderr.write)

    sys.stderr.flush()

    if writer == None:
        exit(1)

    try:
        http_request = (
            f"GET {path} HTTP/1.0\r\n"
            f"Host: {host}\r\n"
            f"User-Agent: multiconnect-client-example/1.0\r\n"
            f"Connection: close\r\n"
            f"\r\n"
        ).encode("utf-8")

        writer.write(http_request)
        await writer.drain()

        # Receive and display response bytes
        response_data = await reader.read(65536)
        print("=== Received Response ===")
        sys.stdout.buffer.write(response_data)
        sys.stdout.buffer.flush()

    finally:
        writer.close()
        await writer.wait_closed()

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
