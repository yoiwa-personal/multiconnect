#!/usr/bin/python3
"""
multiconnect: A TCP proxy choosing fastest TCP/IP connection.
"""
# (c) 2018-2021 Yutaka OIWA <yutaka@oiwa.jp>.
# All rights reserved.
# Redistributable under Apache License, version 2.0.
# See <https://www.apache.org/licenses/LICENSE-2.0>

from typing import Any, Callable, Coroutine, Optional, Set, Tuple
import sys

import asyncio
import socket

import threading
from threading import Thread

from collections import namedtuple
import traceback
import re
import argparse

_debug = False
def dp(f, **k):
    if _debug:
        if len(k):
            f = f.format(**k)
        print(f.format(**k), file=sys.stderr)

def _print_to_stderr(*a, **k):
    print(*a, **k, file=stderr)

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

class TaskCoordinator:
    """
    Generic Coordinator class for Parallel Racing Tasks
    """
    def __init__(self, clean_up_task = (lambda x: None)):
        self.winner_result: Optional[Any] = None
        self.winner_event = asyncio.Event()
        self.winning_task = None
        self.active_tasks: Set[asyncio.Task] = set()
        self.clean_up_task = clean_up_task

    def set_winner(self, result: Any, winning_task: asyncio.Task):
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

    async def spawn(
            self,
            func: Coroutine[Any, Any, Any],
            name : Optional[str] = None,
            predecessor: Optional[asyncio.Task] = None,
            delay: float = 0.0,
    ) -> asyncio.Task:
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
                traceback.print_exception(e)
                pass
        elif delay > 0.0:
            dp(f"{name}: waiting {delay}...")
            await asyncio.sleep(delay)
            dp(f"{name}: time elapsed")
        else:
            dp(f"{name}: no waiting ...")
            pass

        if self.winner_event.is_set():
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
            except Exception as e:
                print(f"{name}: {e!r}", file=sys.stderr)
                traceback.print_exception(e)
            return None

        _wrapper.__name__ = func.__name__

        task = loop.create_task(_wrapper(), name=name)
        self.active_tasks.add(task)
        task.add_done_callback(lambda t: self.active_tasks.discard(t))
        return task

    async def run_until_complete(self) -> Any:
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

    def _looser_sentinel(res):
        """Call-back for every returned but not selected results."""
        if res is not None:
            self._force_close_socket(res)

    async def connect_singleip_worker(
            self,
            addr_info: tuple,
            mask: Optional[int],
            worker_id: str
    ):
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
                    self.diag_f(f"{worker_id}: {remoteip} not in network {local_if}")
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

            self.msg_f(f"CONNECTED to {hostport}")
            return sock
        except asyncio.CancelledError:
            self._force_close_socket(sock)
            raise
        except OSError as e:
            self.diag_f(f"{hostport}: connection failed {e!r}")
            self._force_close_socket(sock)
            return None

    async def host_happy_eyeballs_worker(
            self,
            hostspec,
            worker_id: str,
    ):
        """Worker coroutine for a single DNS-named host.
        Spawn sub-coroutine for IP addresses among several IP addresses."""

        our_use_v6 = self.use_v6 and hostspec.family != "4"
        our_use_v4 = self.use_v4 and hostspec.family != "6"

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
            self.diag_v("{worker_id}: connection to {host} failed: no useable destination IP")
            return None

        prev_ip_task = None

        for i, addr in enumerate(ordered_addrs):
            delay = 0.0 if i == 0 else self.happy_eyeballs_delay
            host = addr[4][0]
            port = addr[4][1]
            nwi = f"{worker_id}-{i!s}"
            new_task = await self.coord.spawn(
                self.connect_singleip_worker(mask=hostspec.mask, addr_info=addr, worker_id=nwi),
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

    async def async_get_fastest_connection(self, hosts):
        """The main coroutine of get_fastest_connection. Use get_fastest_connection below."""
        self.coord = coord = TaskCoordinator(clean_up_task=self._looser_sentinel)

        prev_task = None
        for i, hostspec in enumerate(hosts):
            prev_task = await coord.spawn(
                self.host_happy_eyeballs_worker(
                    hostspec,
                    worker_id=f"{i!s}"),
                name = f"{i}:: {hostspec!s}",
                delay=hostspec.wait, predecessor=prev_task)
            if not prev_task: break

        result = await coord.run_until_complete()

        msg = "\n".join(self.msg_v)
        diag = "\n".join(self.diag_v)
        return result, msg, diag

    def __init__(self, msg=None, diag=None, use_v4=True, use_v6=True,
                 happy_eyeballs_delay=0.25):
        """ONLY called from get_fastest_connection"""
        self.use_v4 = use_v4
        self.use_v6 = use_v6
        self.happy_eyeballs_delay = happy_eyeballs_delay
        self.msg_v = []
        self.diag_v = []
        self.msg_f = msg if msg else self.msg_v.append
        self.diag_f = diag if diag else self.diag_v.append

    @classmethod
    def get_fastest_connection(klass, hosts, **k):
        """Try simultanously connecting to given host lists and return the fastest one.

Argument is a list of HostSpec's containing the following fields:

  - wait (real): seconds to delay connections.

  - host (string): a target host name or an IPv4 address to connect.

  - mask (optional integer):
    a number of bits for IPv4 netmask.
    If the target host does not belong to the same network as the running host,
    the connection will not be attempted.

  - port (integer): a TCP port number to connect.

Optional msg and diag are functions receiving a progress and
diagnostic messages during running.  If omitted, these will
be returned in the return values msg and diag below.

Returning a tuple of (c, msg, diag), where
  - c is a connected TCP socket channel or None,
  - msg, diag is a string containing message and diagnostic messages.
"""
        return asyncio.run(klass(**k).
                           async_get_fastest_connection(hosts))

### Bidirectional data forwarding (proxying).
### For optimal throughput, it is implemented as a threaded routine, not coroutines.

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

    args = parser.parse_args()

    global _debug
    if args.verbose >= 3:
        _debug = True

    if args.use_v4_only and args.use_v6_only:
        sys.stderr.write("Error: --use_v6_only and --use_v4_only is exclusive\n")
        parser.print_help()
        sys.exit(2)

    use_v6 = not args.use_v4_only
    use_v4 = not args.use_v6_only

    for hspec in args.hosts:
        mo = re.match(r"^((?P<wait>\d+(\.\d+)?):)?(?:[vV](?P<family>[46]):)?(\[(?P<host6>[0-9A-Fa-f:]+)\]|(?P<host>[^/:]+))(/(?P<mask>\d+))?:(?P<port>\d+)$", hspec)
        if not mo:
            raise RuntimeError("bad spec: {}".format(hspec))
        w = mo.group('wait')
        w = float(w) if w else 0.0
        h = mo.group('host') or mo.group('host6')
        nm = mo.group('mask')
        nm = int(nm) if nm else None
        p = int(mo.group('port'))
        family = mo.group('family')
        hostlist.append(HostSpec(wait = w, family=family, host = h, mask = nm, port = p))

    c, msg, diag = AsyncConnector.get_fastest_connection(
        hostlist,
        use_v4=use_v4,
        use_v6=use_v6,
        happy_eyeballs_delay=args.delay
    )

    if not c:
        print("cannot connect to any given host.", file=sys.stderr)
        print(msg, file=sys.stderr)
        print(diag, file=sys.stderr)
        sys.exit(1)

    if args.verbose >= 1:
        print(msg, file=sys.stderr)
        if args.verbose >= 2:
            print(diag, file=sys.stderr)

    c.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    c.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

    Forwarder.run_parallel(
        ((c, sys.stdout.buffer.raw),
         (sys.stdin.buffer.raw, c)))

    c.close()

    sys.exit(0)

if __name__=='__main__':
    main()
