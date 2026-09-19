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

class HostSpec(namedtuple('HostSpec', ['wait', 'host', 'mask', 'port'])):
    def __str__(self):
        w = ("%g:" % self.wait) if self.wait else ""
        m = ("/%d" % self.mask) if self.mask else ""
        return "%s%s%s:%d" % (w, self.host, m, self.port)
    def short_str(self):
        return "%s:%d" % (self.host, self.port)

class TaskCoordinator:
    """
    Coordinator class for Parallel Racing Tasks
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
                dp(f"{name}: waiting predecessor or {delay}... pred finised")
            except asyncio.TimeoutError:
                dp(f"{name}: waiting predecessor or {delay}... time elapsed")
                pass
            except asyncio.CancelledError:
                dp(f"{name}: waiting predecessor or {delay}... CANCELLED")
                func.close()
                return
            except Exception as e:
                dp(f"{name}: waiting predecessor or {delay}... failed {e!r}")
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
            except Exception as e:
                print(f"{name}: {e!r} {getattr(e,'traceback','')}", file=sys.stderr)
        _wrapper.__name__ = func.__name__

        task = loop.create_task(_wrapper())
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
    @staticmethod
    def _force_close_socket(sock: socket.socket):
        """terminate socket by RST"""
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
            sock.close()
        except Exception:
            pass

    def _looser_sentinel(res):
        """terminate socket by RST"""
        if res is not None:
            self._force_close_socket(res)

    async def connect_singleip_worker(
            self,
            host: str,
            port: int,
            mask: Optional[int],
            addr_info: tuple,
            use_rst: bool = True
    ):
        loop = asyncio.get_running_loop()

        family, type_, proto, _, sockaddr = addr_info
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
                usock = socket.socket(type=socket.SOCK_DGRAM, family=family)
                usock.connect((addr_info[4][0], 80)) # port is dummy
                laddr = usock.getsockname()
                remoteip = ipaddress.ip_address(addr_info[4][0])
                local_if = ipaddress.ip_interface("%s/%d" % (laddr[0], mask))
                if remoteip not in local_if.network:
                    self.diag_f(f"{remoteip} not in network {local_if}")
                else:
                    ok = True
                usock.close()
            if not ok:
                return None

        try:
            dp("Connecting to {host}:{port}", host=host, port=port)

            sock = socket.socket(family, socket.SOCK_STREAM, proto=proto)
            sock.setblocking(False)

            try:
                await loop.sock_connect(sock, sockaddr)
                sock.setblocking(True)
            except:
                sock.close()
                raise

            self.msg_f(f"CONNECTED to {host}:{port}")
            return sock
        except asyncio.CancelledError:
            self._force_close_socket(sock)
            raise
        except OSError as e:
            self.diag_f(f"{host}:{port}: {e!r}")
            self._force_close_socket(sock)
            return None

    async def host_happy_eyeballs_worker(
            self,
            host: str,
            port: int,
            mask: int,
            happy_eyeballs_delay: float = 0.25
    ):
        loop = asyncio.get_running_loop()

        infos = await loop.getaddrinfo(
            host, port, family=socket.AF_UNSPEC, type=socket.SOCK_STREAM
        )

        v6_addrs = [i for i in infos if i[0] == socket.AF_INET6]
        v4_addrs = [i for i in infos if i[0] == socket.AF_INET]

        ordered_addrs = []
        for idx in range(max(len(v6_addrs), len(v4_addrs))):
            if idx < len(v6_addrs):
                ordered_addrs.append(v6_addrs[idx])
            if idx < len(v4_addrs):
                ordered_addrs.append(v4_addrs[idx])

        if not ordered_addrs:
            self.diag_v("connection to {host} failed: no useable destination IP")
            return None

        prev_ip_task = None

        for i, addr in enumerate(ordered_addrs):
            delay = 0.0 if i == 0 else happy_eyeballs_delay
            host = addr[4][0]
            port = addr[4][1]
            new_task = await self.coord.spawn(
                self.connect_singleip_worker(host=host, port=port, mask=mask, addr_info=addr),
                predecessor=prev_ip_task,
                delay=delay,
                name=f"{host}:{port}"
            )
            if not new_task: break
            new_task.name = f"<singleip_worker: {host}:{port}>"
            prev_ip_task = new_task

        if prev_ip_task: # may be cancelled during waiting
            dp(f"connection to {host}: waiting for last single_ip: {prev_ip_task.name}")
            await asyncio.wait([prev_ip_task])
            dp(f"connection to {host}: waiting for {prev_ip_task.name} done. finishing")

    async def async_get_fastest_connection(self, hosts, msg=None, diag=None):
        """The main coroutine of get_fastest_connection. Use get_fastest_connection below."""
        self.coord = coord = TaskCoordinator(clean_up_task=self._looser_sentinel)
        msg_v = []
        diag_v = []
        if not msg:
            self.msg_f = msg_v.append
        if not diag:
            self.diag_f = diag_v.append

        prev_task = None
        for i, hostspec in enumerate(hosts):
            (wait, host, mask, port) = hostspec
            prev_task = await coord.spawn(
                self.host_happy_eyeballs_worker(
                    hostspec.host, hostspec.port, hostspec.mask),
                name = f"{i}:{hostspec!s}",
                delay=hostspec.wait, predecessor=prev_task)
            if not prev_task: break

        result = await coord.run_until_complete()
        msg = "\n".join(msg_v)
        diag = "\n".join(diag_v)

        return result, msg, diag

    @classmethod
    def get_fastest_connection(klass, hosts, msg=None, diag=None):
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
        return asyncio.run(klass().async_get_fastest_connection(hosts, msg, diag))

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
                print("shutdown failed: {e}".format(e=e), file=sys.stderr)

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
        description = "TCP proxy choosing the fastest connection from destination candidates.",
        epilog="""Syntax for each hostspec is "[<delay>:]<host>[/<mask>]:<port>".

It can be as simple as "host:port" (e.g. "example.com:22"), or
as complex as "0.5:192.0.2.45/24:443".

If an optional floating-number prefix <delay> is given, connection is
attempted after the given second is passed since the connection
attempt for the previous argument is started.  The delay is cancelled
if previous argument's connection is determined to be failed.

The optional <mask> specifies the number of netmask bits for the
expected local network.  If the destination IP address does not fall
into the same network of this host, as determined by the mask bits,
connection will not be tried.

The above example means that if the current host is in 192.2.50.0/24
network, try connecting to IPv4 address 192.0.2.45, TCP port 443,
after waiting a half second.


""",
        formatter_class=ParagraphFillingFormatter #argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('hosts', metavar='hostspec', type=str, nargs='+',
                        help="connection destination candidates")
    parser.add_argument('-v', '--verbose', action='count', default=1,
                        help="increse verbosity level")
    parser.add_argument('-q', '--quiet', action='store_const', dest='verbose', const=0,
                        help="set verbosity level to 0")

    args = parser.parse_args()

#    if len(args.hosts) == 0:
#        parser.print_help()
#        sys.exit(2)
    global _debug
    if args.verbose >= 3:
        _debug = True

    for hspec in args.hosts:
        mo = re.match(r"^((?P<wait>\d+(\.\d+)?):)?(?P<host>[^/:]+)(/(?P<mask>\d+))?:(?P<port>\d+)$", hspec)
        if not mo:
            raise RuntimeError("bad spec: {}".format(hspec))
        w = mo.group('wait')
        w = float(w) if w else 0.0
        h = mo.group('host')
        nm = mo.group('mask')
        nm = int(nm) if nm else None
        p = int(mo.group('port'))
        hostlist.append(HostSpec(wait = w, host = h, mask = nm, port = p))

    c, msg, diag = AsyncConnector.get_fastest_connection(hostlist)

    if not c:
        print("cannot connect to any given host.", file=sys.stderr)
        print(msg, file=sys.stderr)
        print(diag, file=sys.stderr)
        sys.exit(1)

    if args.verbose >= 1:
        print(msg, file=sys.stderr)
        if args.verbose >= 2:
            print(diag, file=sys.stderr)

    Forwarder.run_parallel(
        ((c, sys.stdout.buffer.raw),
         (sys.stdin.buffer.raw, c)))

    c.close()

    sys.exit(0)

if __name__=='__main__':
    main()
