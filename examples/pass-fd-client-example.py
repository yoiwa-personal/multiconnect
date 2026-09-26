import os
import socket
import struct
import sys
from subprocess import Popen

## This script is to illustrate how to communicate with multiconnect.py
## via socket passing mode.
##
## Obviously, you do not need to implement this on Python:
## you can just use multiconnect.AsyncConnector.get_fastest_connection() directly.

def extract_socket_posix(io_sock):
    """Extract passed socket from UNIX domain socket using socket.recv_fds()."""

    # Read 3-byte header (b"\x92\xc3\xc0") and at most 1 file descriptor via SCM_RIGHTS
    msg, fds, flags, addr = socket.recv_fds(io_sock, 3, 1)

    if not msg:
        raise ConnectionError("No data received from parent process")

    if msg == b"\x92\xc3\xc0":
        if not fds:
            raise RuntimeError("Failed to receive file descriptor via SCM_RIGHTS")
        target_fd = fds[0]
        # Create socket from extracted FD
        sock = socket.fromfd(target_fd, socket.AF_INET, socket.SOCK_STREAM)
        os.close(target_fd)

    elif msg == b"\x92\xc2\xda":
        len = io_sock.recv(2)
        len, = struct.unpack("H", len)
        error_msg = io_sock.recv(len).decode("utf-8")
        raise RuntimeError(f"passed an error message: {error_msg!r}")

    else:
        raise RuntimeError(f"Invalid POSIX payload marker: {msg!r}")

    # Send 0xc0 ACK back to parent via stdin/stdout socket
    io_sock.sendall(b"\xc0")

    return sock

def extract_socket_win32(stdin_bin, stdout_bin):
    """Extract passed socket from stream via WSAPROTOCOL_INFOW binary payload."""

    # Read header: 0x92 0xc3 0xc5 (3 bytes) + length (2 bytes uint16 = 628)
    header = stdin_bin.read(5)
    if len(header) < 5:
        raise ConnectionError("Failed to read payload header")

    marker, length = struct.unpack(">3sH", header)

    if marker == b"\x92\xc3\xc5":
        # Read 628 bytes WSAPROTOCOL_INFOW structure
        proto_info = stdin_bin.read(length)
        if len(proto_info) < length:
            raise ConnectionError("Incomplete WSAPROTOCOL_INFO payload")
        # Restore socket using socket.fromshare()
        sock = socket.fromshare(proto_info)

    elif marker == b"\x92\xc2\xda":
        error_msg = stdin_bin.read(length).encode("utf-8")
        raise RuntimeError(f"passed an error message: {error_msg!r}")

    else:
        raise ValueError(f"Invalid Win32 payload marker: {marker!r}")

    # Send 0xc0 ACK back to parent via stdout
    stdout_bin.write(b"\xc0")
    stdout_bin.flush()

    return sock

def main():
    if len(sys.argv) < 2:
        print("Usage: python toy-client.py <host> [path]", file=sys.stderr)
        print("Example: python toy-client.py example.com /", file=sys.stderr)
        sys.exit(1)

    host = sys.argv[1]
    path = sys.argv[2] if len(sys.argv) > 2 else "/"

    if not path.startswith("/"):
        path = "/" + path

    is_posix = hasattr(socket, 'recv_fds')
    is_win32 = hasattr(socket, 'fromshare')
    if not is_posix and not is_win32:
        raise RuntimeError("no platform API available")

    if is_posix:
        parent_sock, child_sock = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
        proc = Popen(
            ["python3", "./multiconnect.py", "-vvvv", f"--pass-fd", f"{host}:80"],
            stdin=child_sock.fileno(),  # may be a pipe or the same socket
            stdout=child_sock.fileno(), # MUST be AF_UNIX socket
            stderr=sys.stderr,
            pass_fds=[child_sock.fileno()],
        )
        child_sock.close()
        child_sock = None
    elif is_win32:
        proc = subprocess.Popen(
            ["py", "./multiconnect.py", "-vvvv", f"--pass-to-pid={os.getpid()}", f"{host}:80"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=sys.stderr,
        )

    if is_posix:
        sock = extract_socket_posix(parent_sock)
    elif is_win32:
        sock = extract_socket_win32(proc.stdout, proc.stdin)

    try:
        http_request = (
            f"GET {path} HTTP/1.0\r\n"
            f"Host: {host}\r\n"
            f"User-Agent: multiconnect-client-example/1.0\r\n"
            f"Connection: close\r\n"
            f"\r\n"
        ).encode("utf-8")

        sock.sendall(http_request)

        response_data = sock.recv(65536)
        print("=== Received Response ===")
        sys.stdout.buffer.write(response_data)
        sys.stdout.buffer.flush()

    finally:
        sock.close()

if __name__ == "__main__":
    main()
