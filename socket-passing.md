# Socket Passing API Specification

`multiconnect` can now pass a connected TCP socket back to the caller process, instead of performing bidirectional proxying.

The command-line options and APIs described below depend on the underlying OS APIs.

## Common Specification

Communication uses a limited subset of the [MessagePack format](https://msgpack.org/) over standard input and standard output. Standard error continues to produce standard text-based error and diagnostic messages.

If the connection succeeds, the command sends a message starting with byte `0x92`, accompanied by socket information as a payload in an OS-dependent manner (detailed below) to standard output.

Upon receiving the success message, the recipient process must retrieve the socket from the payload and then send a single-byte acknowledgment (`0xc0`, corresponding to `nil` or `None` in MessagePack) to the standard input of the command. The command will then terminate immediately.

If an error occurs, the command outputs a binary message in the following format and terminates:

    0x92 0xc2 0xda (2-byte big-endian integer length n) (n bytes of error message in UTF-8)

In MessagePack, this corresponds to `[false, "error message"]`.

## POSIX Implementation

In POSIX environments, the `--pass-fd` command-line option enables socket passing mode.

The standard output of the command must be connected to a UNIX domain stream socket, typically created via `socketpair(2)`. Standard pipes will not work.

Upon successfully connecting to the remote peer, the command passes the file descriptor of the connected socket via the `sendmsg(2)` system call. The command sends a 3-byte message `0x92 0xc3 0xc0` (representing `[true, null]` in MessagePack) accompanied by an `SCM_RIGHTS` ancillary message.

The recipient should retrieve the descriptor using `recvmsg(2)` and then send the single-byte acknowledgment (`0xc0`) mentioned above.

In Python, `recvmsg` or `socket.recv_fds()` can be used on the recipient side.

## Win32 Implementation

In Win32 environments, the `--pass-to-pid` command-line option enables socket passing mode. This option takes an integer argument specifying the recipient's process ID.

The standard output of the command should be connected to a standard pipe.
The command sends a binary payload in the following format:

    0x92 0xc3 0xc5
    (2-byte big-endian integer 628, i.e., 0x02 0x74)
    (628 bytes of binary data)

The 628-byte binary payload represents a `WSAPROTOCOL_INFOW` structure and can be passed to the `WSASocketW` function to recreate the connected socket. In Python, `socket.fromshare()` can be used to restore the socket.

In MessagePack, this message format corresponds to `[true, b'binary structure']`.
