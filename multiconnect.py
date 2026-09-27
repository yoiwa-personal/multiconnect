#!/usr/bin/python3
"""
multiconnect: A TCP proxy choosing fastest TCP/IP connection.
"""
# (c) 2018-2026 Yutaka OIWA <yutaka@oiwa.jp>.
# All rights reserved.
# Redistributable under Apache License, version 2.0.
# See <https://www.apache.org/licenses/LICENSE-2.0>

import sys, os, io, inspect, struct

import asyncio
import socket

import threading
from threading import Thread

from collections import namedtuple
import traceback
import re
import argparse

__all__ = ["get_fastest_connection", "async_get_fastest_connection"]

_debug = False

def dp(f, **k):
    if _debug:
        if len(k):
            f = f.format(**k)
        print(f.format(**k), file=sys.stderr)

def _print_to_stderr(*a, **k):
    print(*a, **k, file=sys.stderr)

class HostSpec(namedtuple('HostSpec', ['host', 'port', 'mask', 'wait', 'family'])):
    def __str__(self):
        w = ("%g:" % self.wait) if self.wait else ""
        f = "" if self.family == None else "[V%sONLY] " % self.family
        h = self.host
        h = "[" + h + "]" if ":" in h else h
        m = ("/%d" % self.mask) if self.mask else ""
        return "%s%s%s%s:%d" % (w, f, h, m, self.port)
    def short_str(self):
        return "%s:%d" % (self.host, self.port)

    @classmethod
    def fromstrlist(klass, hspecs, default_delay = 0.0):
        return [ klass.fromstr(x, default_delay=(0.0 if i == 0 else default_delay))
                 for (i, x) in enumerate(hspecs) ]

    @classmethod
    def fromstr(klass, hspec, default_delay=0.0):
        if isinstance(hspec, klass): # already processed
            return hspec
        if isinstance(hspec, tuple): # special case
            if len(hspec) != 2: raise ValueError("bad host spec: {}".format(hspec))
            return klass(wait=default_delay, family=None, host = hspec[0], mask = None, port = hspec[1])

        mo = re.match(r"^((?P<wait>\d+(\.\d+)?):)?(?:[vV](?P<family>[46]):)?(\[(?P<host6>[0-9A-Fa-f:]+)\]|(?P<host>[^/:]+))(/(?P<mask>\d+))?:(?P<port>\d+)$", hspec)
        if not mo:
            raise ValueError("bad host spec: {}".format(hspec))
        w = mo.group('wait')
        w = float(w) if w else default_delay
        h = mo.group('host') or mo.group('host6')
        nm = mo.group('mask')
        nm = int(nm) if nm else None
        p = int(mo.group('port'))
        family = mo.group('family')
        if family == None:
            family = None
        elif family == "4":
            family = socket.AF_INET
        elif family == "6":
            family = socket.AF_INET6
        else:
            raise ValueError
        return klass(wait=w, family=family, host = h, mask = nm, port = p)

class TaskCoordinator:
    """
    Generic Coordinator class for Parallel Racing Tasks
    """
    def __init__(self, clean_up_task = (lambda x: None)):
        self.winner_result = None
        self.winner_event = asyncio.Event()
        self.winning_task = None
        self.gathered_exceptions = []
        self.active_tasks = set()
        self.clean_up_task = clean_up_task

    def set_winner(self, result, winning_task):
        """Decide the winner, and cancel all other running tasks"""
        if not self.winner_event.is_set():
            self.winning_task = winning_task
            self.winner_result = result
            self.winner_event.set()
            for task in list(self.active_tasks):
                if task != winning_task and not task.done():
                    task.cancel()

    async def _cleanup_losers(self):
        """
        A helper to gather all remaining runners and reap it
        """
        loser_tasks = [t for t in list(self.active_tasks) if t != self.winning_task]
        if not loser_tasks:
            return

        # collect all remaining tasks
        results = await asyncio.gather(*loser_tasks, return_exceptions=True)

        # call finalizer for any cleanup requirements
        for res in results:
            self.clean_up_task(res)

    async def spawn(self, func, name=None, predecessor=None, delay=0.0):
        """
        Run a new task under the coordinator.
        Wait until predecessor fails or delay seconds, whichever is faster.
        """
        loop = asyncio.get_running_loop()
        if not name:
            name = f"{func.__name__}"

        if predecessor is not None:
            try:
                dp(f"{name}: waiting predecessor or {delay}...")
                await asyncio.wait_for(asyncio.shield(predecessor), timeout=delay)
                dp(f"{name}: waiting predecessor or {delay}... pred finished")
            except asyncio.TimeoutError:
                dp(f"{name}: waiting predecessor or {delay}... time elapsed")
                pass
            except asyncio.CancelledError:
                dp(f"{name}: waiting predecessor or {delay}... CANCELLED")
                func.close()
                return
            except OSError as e:
                dp(f"{name}: waiting predecessor or {delay}... failed {e!r}")
                pass
            except Exception as e:
                dp(f"{name}: waiting predecessor or {delay}... failed {e!r}")
                traceback.print_exception(e, file=sys.stderr)
                pass
        elif delay > 0.0:
            dp(f"{name}: waiting {delay}...")
            await asyncio.sleep(delay)
            dp(f"{name}: time elapsed")
        else:
            dp(f"{name}: no waiting ...")
            pass

        if self.winner_event.is_set():
            dp(f"{name}: winner is determined: cancelling")
            func.close()
            return

        async def _wrapper():
            current_task = asyncio.current_task()

            # Run a task
            try:
                res = await func
                if res is not None and not self.winner_event.is_set():
                    self.set_winner(res, current_task)
            except asyncio.CancelledError:
                raise
            except OSError as e:
                print(f"{name}: {e!r}", file=sys.stderr)
                self.gathered_exceptions.append(e)
            except Exception as e:
                print(f"{name}: {e!r}", file=sys.stderr)
                traceback.print_exception(e)
                self.gathered_exceptions.append(e)
            return None

        _wrapper.__name__ = func.__name__

        task = loop.create_task(_wrapper(), name=name)
        self.active_tasks.add(task)
        task.add_done_callback(lambda t: self.active_tasks.discard(t))
        return task

    async def run_until_complete(self, raise_exception=True):
        """Run tasks and wait a winner"""
        while not self.winner_event.is_set() and self.active_tasks:
            done, _ = await asyncio.wait(
                self.active_tasks,
                return_when=asyncio.FIRST_COMPLETED
            )
            if self.winner_event.is_set():
                break

        if self.winner_event.is_set():
            asyncio.create_task(self._cleanup_losers())
            return self.winner_result
        else:
            if raise_exception and len(self.gathered_exceptions) >= 1:
                raise self.gathered_exceptions[-1]
            return None

class AsyncConnector:
    """Socket connector to find the fastest-available connection among parallel attempts.

    No instance to create by users: See the class method `get_fastest_connection`.
    """

    @staticmethod
    def _force_close_socket(sock: socket.socket):
        """terminate a socket by RST"""
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
            sock.close()
        except Exception:
            pass

    def _looser_sentinel(self, res):
        """Call-back for every returned but not selected results."""
        if res is not None:
            self._force_close_socket(res)

    async def _connect_singleip_worker(self, addr_info, mask, worker_id):
        loop = asyncio.get_running_loop()

        family, type_, proto, canonname, sockaddr = addr_info
        ip_str = sockaddr[0]
        writer = None

        if mask:
            ok = None
            if 0 < mask < 32:
                # IPv4
                if family != socket.AF_INET:
                    ok = False
            elif 32 <= mask <= 128:
                if family != socket.AF_INET6:
                    ok = False
            else:
                raise ValueError(f"invalid mask {mask}")
            if ok != False:
                import ipaddress
                # use UDP connect (externally no-op) to discover the source address after routing
                uaddr = list(addr_info[4])
                uaddr[1] = 80 # dummy port number to something legitimate
                uaddr = tuple(uaddr)
                usock = socket.socket(type=socket.SOCK_DGRAM, family=family, proto=socket.IPPROTO_UDP)
                usock.connect(uaddr)
                laddr = usock.getsockname() # get local-side address
                usock.close()
                remoteip = ipaddress.ip_address(addr_info[4][0])
                local_if = ipaddress.ip_interface("%s/%d" % (laddr[0], mask))
                if remoteip not in local_if.network:
                    self.diag_f(f"{worker_id}: {remoteip} not in network {local_if}\n")
                else:
                    ok = True
            if not ok:
                return None

        hostport = [addr_info[4][0], addr_info[4][1]] # for diag message purposes
        if family == socket.AF_INET6: hostport[0] = "[" + hostport[0] + "]"
        hostport = hostport[0] + ":" + str(hostport[1])

        try:
            dp("Connecting to {hostport}", hostport=hostport)
            sock = socket.socket(family=family, type=socket.SOCK_STREAM, proto=proto)
            sock.setblocking(False)

            try:
                await loop.sock_connect(sock, sockaddr)
                sock.setblocking(True)
            except:
                sock.close()
                raise

            self.msg_f(f"CONNECTED to {hostport}\n")
            return sock
        except asyncio.CancelledError:
            self._force_close_socket(sock)
            raise
        except OSError as e:
            self.diag_f(f"{hostport}: connection failed: {e!r}\n")
            self._force_close_socket(sock)
            return None

    async def _host_happy_eyeballs_worker(self, hostspec, worker_id):
        """Worker coroutine for a single DNS-named host.
        Spawn sub-coroutine for IP addresses among several IP addresses."""

        our_use_v6 = self.use_v6 and hostspec.family != socket.AF_INET
        our_use_v4 = self.use_v4 and hostspec.family != socket.AF_INET6

        loop = asyncio.get_running_loop()

        infos = await loop.getaddrinfo(
            hostspec.host, hostspec.port, family=socket.AF_UNSPEC, type=socket.SOCK_STREAM
        )

        v6_addrs = [i for i in infos if i[0] == socket.AF_INET6] if our_use_v6 else []
        v4_addrs = [i for i in infos if i[0] == socket.AF_INET] if our_use_v4 else []

        ordered_addrs = []
        for idx in range(max(len(v6_addrs), len(v4_addrs))):
            if idx < len(v6_addrs):
                ordered_addrs.append(v6_addrs[idx])
            if idx < len(v4_addrs):
                ordered_addrs.append(v4_addrs[idx])

        if not ordered_addrs:
            self.diag_f("{worker_id}: connection to {host} failed: no useable destination IP")
            return None

        prev_ip_task = None

        for i, addr in enumerate(ordered_addrs):
            delay = 0.0 if i == 0 else self.happy_eyeballs_delay
            host = addr[4][0]
            port = addr[4][1]
            nwi = f"{worker_id}-{i+1!s}"
            new_task = await self.coord.spawn(
                self._connect_singleip_worker(mask=hostspec.mask, addr_info=addr, worker_id=nwi),
                predecessor=prev_ip_task,
                delay=delay,
                name=f"{nwi}:: {host}:{port}"
            )
            prev_ip_task = new_task
            if not new_task: break # Task cancelled

        if prev_ip_task: # may be cancelled during waiting
            dp(f"connection to {host}: waiting for finising last single_ip: task {prev_ip_task.get_name()}")
            await asyncio.wait([prev_ip_task])
            dp(f"connection to {host}: waiting for task {prev_ip_task.get_name()} done. finishing")

    async def _async_get_fastest_connection_core(self, hosts, raise_exception=True):
        """The main coroutine of get_fastest_connection.
        Use get_fastest_connection or async_get_fastest_connection below."""
        self.coord = coord = TaskCoordinator(clean_up_task=self._looser_sentinel)

        hosts = HostSpec.fromstrlist(hosts, default_delay=self.happy_eyeballs_delay)

        prev_task = None
        for i, hostspec in enumerate(hosts):
            prev_task = await coord.spawn(
                self._host_happy_eyeballs_worker(
                    hostspec,
                    worker_id=f"{i+1!s}"),
                name = f"{i+1}:: {hostspec!s}",
                delay=hostspec.wait, predecessor=prev_task)
            if not prev_task: break

        result = await coord.run_until_complete(raise_exception=raise_exception)

        return result

    def __init__(self, msg=None, diag=None, use_v4=True, use_v6=True,
                 happy_eyeballs_delay=0.25):
        """ONLY called from get_fastest_connection"""
        self.use_v4 = use_v4
        self.use_v6 = use_v6
        self.happy_eyeballs_delay = happy_eyeballs_delay
        self.msg_f = msg if msg else lambda x: None
        self.diag_f = diag if diag else lambda x: None

    # Two public APIs for general use
    @classmethod
    def get_fastest_connection(klass, hosts, **k):
        """Try simultanously connecting to given host lists and return the ealienst available one.

           The argument is a list of HostSpec's containing the
           following fields:

             - wait (real): seconds to delay connections.

             - host (string): a target host name or an IPv4 address to
               connect.

             - mask (optional integer): a number of bits for IPv4
               netmask.  If the target host does not belong to the
               same network as the running host, the connection will
               not be attempted.

             - port (integer): a TCP port number to connect.

           For compatibility, a string (`"host:port"` or
           `"[v6addr]:port"`) or and 2-tuple `(host, port)` is also
           accepted for list elements.

           Optional keyword arguments are following:

             - msg and diag: functions receiving a progress and
               diagnostic messages during running.  If omitted, these
               will not be collected.

             - use_v4 and use_v6: default True.

             - happy_eyeballs_delay: default 0.250.  Time in seconds
               between multiple addresses for a single host.

           Returns a connected TCP socket.socket channel when succeeded.

           Upon failure, it may either return None or raise exceptions.

           This is ordinary routine version.

           If your program uses `asyncio`, use
           `async_get_fastest_connection`, or `nest_asyncio` from
           PyPI.

        """
        co = klass(**k)._async_get_fastest_connection_core(hosts)

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop == None:
            return asyncio.run(co)

        try:
            return loop.run_until_complete(co)
        except RuntimeError as e:
            if "this event loop is already running" in str(e).lower():
                args = list(e.args)
                args[0] = args[0] + " (consider to use nest_asyncio from PyPI)"
                raise RuntimeError(*args) from e
            else:
                raise

    @classmethod
    async def async_get_fastest_connection(klass, hosts, rawsocket=False, **kwargs):
        """Try simultanously connecting to given host lists and return the fastest one.

           See get_fastest_connection for arguments.
           This is awaitable coroutine version.

           If keyword option `rawsocket` is set, it will return a raw `socket.socket` object.
           Otherwise, the result will be wrapped to asyncio.StreamReader and StreamWriter.

           In either case, if connection was not succeed, it may either return None(s) or
           raise exceptions.

        """
        if rawsocket:
            _class_kwargs = kwargs
            _aio_kwargs = {}
        else:
            _class_keywords = set(inspect.signature(klass).parameters.keys())
            _class_kwargs = { k: v for k, v in kwargs.items() if k in _class_keywords }
            _aio_kwargs = { k: v for k, v in kwargs.items() if k not in _class_keywords }

        result = await klass(**_class_kwargs)._async_get_fastest_connection_core(hosts, raise_exception=True)

        if rawsocket:
            return result

        if result == None:
            return (None, None)
        result.setblocking(False)
        return await asyncio.open_connection(sock=result,
                                             #host=None, port=None, family=None, proto=None, flags=None,
                                             #happy_eyeballs_delay=None, interleave=None, local_addr=None,
                                             **_aio_kwargs)

### Bidirectional data forwarding (proxying).
### For optimal throughput, it is implemented as a threaded routines, not coroutines.

bufsize = 1048576
class Forwarder(Thread):
    def __init__(self, fr, to):
        super().__init__(daemon=False)
        self.fr = fr
        self.to = to
        self.rd = fr.read if hasattr(fr, "read") else fr.recv
        self.wr = to.write if hasattr(to, "write") else to.send
        # adhoc polymorphism: socket lacks read/write (only socketIO has)

    def run(self):
        try:
            while(True):
                r = self.rd(bufsize)
                if not r:
                    break

                l = len(r)
                r = memoryview(r) # make slicing faster
                while (l > 0):
                    x = self.wr(r)
                    assert x > 0
                    l -= x
                    r = r[x:]
        except OSError as e:
            print("send failed: {e}".format(e=e), file=sys.stderr)

        if hasattr(self.to, "shutdown"):
            try:
                self.to.shutdown(socket.SHUT_WR) # safer to use raw socket because of this
            except OSError as e:
                #print("shutdown failed: {e}".format(e=e), file=sys.stderr)
                pass

    @classmethod
    def run_parallel(klass, ff):
        l = []
        for f in ff:
            l.append(Forwarder(*f))
        for t in l:
            t.start()
        for t in l:
            t.join()

### Inter-process socket passing
try:
    import _winapi
    is_win32_available = True
except ImportError:
    is_win32_available = False

try:
    if hasattr(socket, "AF_UNIX") and hasattr(socket, "SCM_RIGHTS") and hasattr(socket.socket, "sendmsg"):
        is_posix_available = True
    else:
        is_posix_available = False
except NameError:
    is_posix_available = False # no-existence of socket.socket is unlikely...

def _make_msgpack_errormsg(m):
    m = m.encode("utf-8")
    return b"\x92\xc2\xda" + len(m).to_bytes(2, byteorder="big") + m

def _make_msgpack_message(m):
    if m is None:
        return b"\x92\xc3\xc0"
    else:
        return b"\x92\xc3\xc5" + len(m).to_bytes(2, byteorder="big") + m

def _pass_sock_to_fd(channel_fd, sock_to_pass):
    file_sock = os.fdopen(channel_fd, "wb", closefd=False)
    try:
        channel_sock = socket.fromfd(channel_fd, socket.AF_UNIX, socket.SOCK_STREAM)
        x = socket.send_fds(channel_sock, [_make_msgpack_message(None)], fds=[sock_to_pass.fileno()])
        dp("waiting for ack byte")
        r = sys.stdin.buffer.read(1)
        dp("ack byte received {r!r}", r=r)
    except Exception as e:
        file_sock.write(_make_msgpack_errormsg(repr(e)))
        traceback.print_exception(e)

def _pass_sock_win32(pid, sock_to_pass):
    try:
        wsainfo_blob = sock_to_pass.share(pid)
        sys.stdout.buffer.write(_make_msgpack_message(wsainfo_blob))
        sys.stdout.buffer.flush()
        dp("waiting for ack byte")
        r = sys.stdin.buffer.read(1)
        dp("ack byte received {r!r}", r=r)
    except Exception as e:
        sys.stdout.buffer.write(_make_msgpack_errormsg(repr(e)))
        traceback.print_exception(e)

### Commandline Processing and main routine
class OurProcessingError(Exception):
    pass
class CommandLineError(OurProcessingError):
    pass

# using an undocumneted interface...
class ParagraphFillingFormatter(argparse.RawDescriptionHelpFormatter):
    def __init__(self, prog, indent_increment=2, max_help_position=24, width=None):
        if not width:
            # backport from Python 3.9
            import shutil
            width = shutil.get_terminal_size().columns - 2
        super().__init__(prog, indent_increment, max_help_position, width)

    def _fill_text(self, text, width, indent):
        # original wrapping routine, honoring paragraph break by double LF.
        import textwrap
        ps = re.split(r'\n\n+', text)
        ps = [textwrap.wrap(text, width) for text in ps]
        ps = [[indent + text for text in p] for p in ps]
        ps = ['\n'.join(p) for p in ps]
        ps = '\n\n'.join(ps)
        return ps

def main():
    use_messagepack = False

    hostlist = []

    parser = argparse.ArgumentParser(
        description = "A TCP proxy that chooses the first available connection from multiple destination candidates.",
        epilog="""'Host' can be a DNS hostname, an IPv4 address, or an IPv6 address enclosed in [ ].

'delay' specifies a delay in seconds (e.g., `0.5`) before attempting
to connect to this host, to allow preceding hosts in the list to be
prioritized.  If a connection attempt to a preceding host fails before
the delay expires, the remaining delay is skipped.

'protocol' can be either 'v4' or 'v6' to restrict the connection to a specific IP version.

'mask_bits' specifies the subnet mask bits (IPv4 or IPv6) for the
expected local network.  If the resolved destination IP address does
not fall within the local network defined by this mask, the connection
attempt for this spec is skipped.

""",
        formatter_class=ParagraphFillingFormatter #argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('hosts', metavar='hostspec', type=str, nargs='+',
                        help="Connection destination candidates (syntax: '[delay:][protocol:]host[/mask_bits]:port').")
    parser.add_argument('-4', '--use-v4-only', action='store_true',
                        help="Force the use of IPv4 addresses only.")
    parser.add_argument('-6', '--use-v6-only', action='store_true',
                        help="Force the use of IPv6 addresses only.")
    parser.add_argument('-v', '--verbose', action='count', default=1,
                        help="Increse verbosity level for diagnostics")
    parser.add_argument('--delay', '--happy-eyeballs-delay', type=float, default=0.25,
                        help="Staggered delay period (in seconds) between multiple IP address attempts (default: 0.25)")
    parser.add_argument('-q', '--quiet', action='store_const', dest='verbose', const=0,
                        help="Suppress progress/diagnostic messages")
    if is_posix_available:
        parser.add_argument('--pass-fd', action='store_true',
                            help="Enable socket passing mode.")
    else:
        parser.add_argument('--pass-fd', action='store_true',
                            help=argparse.SUPPRESS)
    if is_win32_available:
        parser.add_argument('--pass-to-pid', type=int, metavar="PID",
                            help="Enable socket passing mode.")
    else:
        parser.add_argument('--pass-to-pid', type=int, metavar="PID",
                            help=argparse.SUPPRESS)
    args = parser.parse_args()

    if args.pass_fd or args.pass_to_pid:
        use_messagepack = True

    try:
        global _debug
        if args.verbose >= 3:
            _debug = True

        if not is_posix_available and args.pass_fd:
            raise CommandLineError("--pass-to-fd is not supported on this platform")
        if not is_win32_available and args.pass_to_pid:
            raise CommandLineError("--pass-to-pid is not supported on this platform")
        if args.pass_fd and args.pass_to_pid:
            raise CommandLineError("--pass-to-fd and --pass-to-pid are exclusive")

        if args.use_v4_only and args.use_v6_only:
            raise CommandLineError("--use_v6_only and --use_v4_only are exclusive")

        use_v6 = not args.use_v4_only
        use_v4 = not args.use_v6_only

        try:
            hostlist = HostSpec.fromstrlist(args.hosts, default_delay=args.delay)
        except ValueError as e:
            raise CommandLineError(*e.args)

        msg = io.StringIO()
        diag = io.StringIO()

        c = asyncio.run(
                AsyncConnector(
                    use_v4=use_v4,
                    use_v6=use_v6,
                    msg = msg.write,
                    diag = diag.write,
                    happy_eyeballs_delay=args.delay).
                    _async_get_fastest_connection_core(hostlist, raise_exception=False))

        if not c:
            print("cannot connect to any given host.", file=sys.stderr)
            print(msg.getvalue(), file=sys.stderr)
            print(diag.getvalue(), file=sys.stderr, flush=True)
            raise OurProcessingError("cannot connect to any given host.")

        if args.verbose >= 1:
            print(msg.getvalue(), end="", file=sys.stderr, flush=True)
            if args.verbose >= 2:
                print(diag.getvalue(), end="", file=sys.stderr, flush=True)
    except Exception as e:
        message = "Error: " + e.args[0] if isinstance(e, OurProcessingError) else str(e)
        if isinstance(e, OurProcessingError):
            print(message, file=sys.stderr)
            if isinstance(e, CommandLineError):
                parser.print_usage(file=sys.stderr)
        else:
            traceback.print_exception(e)

        if use_messagepack:
            of = os.fdopen(args.pass_fd, "wb") if args.pass_fd else sys.stdout.buffer
            b = _make_msgpack_errormsg(message)
            of.write(b)

        sys.exit(1)

    if args.pass_fd:
        _pass_sock_to_fd(1, c)
    elif args.pass_to_pid:
        _pass_sock_win32(args.pass_to_pid, c)
    else:
        c.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        c.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        Forwarder.run_parallel(
            ((c, sys.stdout.buffer.raw),
             (sys.stdin.buffer.raw, c)))

    c.close()

    sys.exit(0)

if __name__ == '__main__':
    main()
else:
    get_fastest_connection = AsyncConnector.get_fastest_connection
    async_get_fastest_connection = AsyncConnector.async_get_fastest_connection
