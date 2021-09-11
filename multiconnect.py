#!/usr/bin/python3
"""
multiconnect: A TCP proxy choosing fastest TCP/IP connection.
"""
# (c) 2018-2021 Yutaka OIWA <yutaka@oiwa.jp>.
# All rights reserved.
# Redistributable under Apache License, version 2.0.
# See <https://www.apache.org/licenses/LICENSE-2.0>

import sys
import threading
from threading import Thread
from collections import namedtuple
import queue
import socket
import traceback
import time
import re
import select
import argparse

_debug = False
def dp(f, **k):
    if _debug:
        print(f.format(**k), file=sys.stderr)

class HostSpec(namedtuple('HostSpec', ['wait', 'host', 'mask', 'port', 'cascade'])):
    def __str__(self):
        c = "* " if self.cascade else ""
        w = ("%g:" % self.wait) if self.wait else ""
        m = ("/%d" % self.mask) if self.mask else ""
        return "%s%s%s%s:%d" % (c, w, self.host, m, self.port)
    def short_str(self):
        return "%s:%d" % (self.host, self.port)

class ConnectionAborted(RuntimeError):
    pass

class Connector(Thread):
    def __init__(self, hostspec, ret, msg, diag, prev):
        """
Tries to make an connection on background.

Use Connector.get_fastest_connection().
"""
        # Initializer arguments:
        #  - hostspec: see get_fastest_connection().
        #  - ret (Queue):
        #     a channel to send a result.
        #     Possible messages to be sent are either a Socket or None.
        #  - msg (appendable sequence):
        #     a channel to gather a short diagnostic messages.
        #  - diagmsg (appendable sequence):
        #     a channel to gather an diagnostic messages.
        #
        # Instance variables for internal communications:
        #   - .sock: socket on working.
        #   - .no_start: if set to True from outside, the instance will not continue connecting.
        #   - .done: set to True by itself, meaning that the socket is already closed.
        super().__init__(daemon=True, name=repr(str(hostspec)))
        self.hs = hostspec
        self.ret = ret
        self.msg = msg
        self.diag = diag
        self.sock = None
        self.no_start = False
        self.done = False
        self.waitchan = queue.Queue()
        self.nextchan = None
        self.next_cascade = False
        self.atomic_lock = threading.Lock()
        if prev:
            prev.register_as_next(self, cascade=hostspec.cascade)

    def run(self):
        # assignment to and status check on the following variables are
        # protected by self.atomic_lock:

        #   - self.no_start
        #   - self.done
        #   - self.socket

        # Combinations of self.no_start and self.done at status checking:
        #  self.no_start,
        #  |   self.done:

        #  f   f    This thread is working.  Check no_start on next opportunity.

        #  f   f    This thread is acquired a working remote connection.
        #           (abort_connection() can still be called.)

        #  f   T    This worker thread is failed to create a working connection.
        #             This thread takes responsibility to destroy the socket.

        #  T   f    The connection is about to abort.
        #             Thread calling abort_connection() will take
        #             responsibility to destroy self.socket (if set).
        #             The worker thread releases control on self.socket (if set).

        #  T   T    The connection is being destroyed by *another* abort_connection.
        #             No work is needed by this call to abort_connection().

        # If self.no_start is not set by someone, the thread must
        # invoke next_chan.start() and return some single item to
        # self.ret channel, to make counting of remaining workers
        # consistent.

        # If self.no_start is set, it implies the main thread is no
        # more caring about the work counting, and the above
        # consistent requirement is abandoned.

        atomic = self.atomic_lock

        with atomic:
            if self.no_start:
                dp("{hs} not starting at all", hs=self.hs)
                self.ret.put(None)
                return

        dp("{hs} start running", hs=self.hs)
        sock = socket.socket()

        with atomic:
            if self.no_start:
                # small race on who to destroy the socket
                sock.close()
                self.ret.put(None)
                return
            self.sock = sock

        try:
            if self.hs.wait or True:
                # Timed wait with interruption: self.waitchan is used for interuupt.
                dp("{hs} start waiting for {w} seconds", hs=self.hs, w=self.hs.wait)
                try:
                    r = self.waitchan.get(timeout=self.hs.wait)
                    dp("{hs} received cascade signal {r}", hs=self.hs, r=r)
                    if not r:
                        dp("{hs} requested connection cancel", hs=self.hs, r=r)
                        # False must be sent by self.abort_connection().
                        assert(self.no_start == True)
                    else:
                        dp("{hs} requested connection early start", hs=self.hs, r=r)
                except queue.Empty:
                    pass

            with atomic:
                if self.no_start:
                    self.ret.put(None)
                    return

            if self.nextchan:
                self.nextchan.start()

            dp("{hs} wait finished starting", hs=self.hs)
            addr = socket.getaddrinfo(self.hs.host, self.hs.port,
                                      family=socket.AF_INET,
                                      proto=socket.IPPROTO_TCP)[0][4]

            if self.hs.mask:
                # Applying connect() to UDP socket will resolve routing and
                # get an appropriate source address for reaching that destination.
                import ipaddress
                usock = socket.socket(type=socket.SOCK_DGRAM)
                usock.connect(addr)
                laddr = usock.getsockname()

                remoteip = ipaddress.ip_address(addr[0])
                local_if = ipaddress.ip_interface("%s/%d" % (laddr[0], self.hs.mask))
                if remoteip not in local_if.network:
                    raise ConnectionAborted("{} not in network {}".format(
                        remoteip, local_if))

            with atomic:
                if self.no_start:
                    self.ret.put(None)
                    return

            dp("{hs} connecting", hs=self.hs)
            self.sock.connect(addr)

            with atomic:
                dp("{hs} connected", hs=self.hs)
                self.msg.append("CONNECTED to {}:{}".format(self.hs.host, self.hs.port))
                self.ret.put(self.sock)
                return

        except (ConnectionAborted, OSError) as e:
            dp("{hs} connection failed {e}", hs=self.hs, e=e)
            do_close = False
            with atomic:
                if not self.no_start:
                    self.done = True
                    do_close = True

            if self.next_cascade:
                self.nextchan.waitchan.put(True)

            if do_close:
                self.__ignore_os_error(self.sock.close)
                m = "%s: connection failed: %s\n" % (str(self.hs), str(e))
                self.diag.append(m)

            self.ret.put(None)

    def abort_connection(self):
        """abort connection attempts, racing with the running thread."""
        with self.atomic_lock:
            if self.done:
                dp("{hs} already terminated on abort request", hs=self.hs)
                # someone (the runner or another call of abort_connection) is already taking care. No-op.
                return

            dp("{hs} try aborting", hs=self.hs)
            self.done = True
            # 1. tell the runner that no more attempt needed.
            self.no_start = True
            # 2. if time-waiting, interrupt it.
            self.waitchan.put(False)
            # 3. if already start connecting, forcibly destroy the socket.
            #    This will interrupt connect() call on pure Linux with ECONNRESET.
            #    (not working on Windows Subsystem for Linux, however.)
            if not self.sock:
                return

        dp("{hs} try shutdown", hs=self.hs)
        self.__ignore_os_error(self.sock.shutdown, socket.SHUT_RDWR)
        dp("{hs} try close", hs=self.hs)
        self.__ignore_os_error(self.sock.close)

    def register_as_next(self, next, cascade):
        assert(self.nextchan is None)
        self.nextchan = next
        self.next_cascade = cascade

    @staticmethod
    def __ignore_os_error(f, *a, **ka):
        try:
            f(*a, **ka)
        except OSError as e:
            dp("... ignoring OS error {e}", e=e)
            pass

    @classmethod
    def get_fastest_connection(klass, hosts):
        """
    Try simultanously connecting to given host lists and return the fastest one.

    Argument is a list of HostSpec's containing the following fields:

      - wait (real): seconds to delay connections.

      - host (string): a target host name or an IPv4 address to connect.

      - mask (optional integer):
        a number of bits for IPv4 netmask.
        If the target host does not belong to the same network as the running host,
        the connection will not be attempted.

      - port (integer): a TCP port number to connect.

    Returning a tuple of (c, m, dg), where
      - c is a connected TCP socket channel or None,
      - m, dg is a string containing message and diagnostic messages.
"""
        q = queue.Queue()
        msg = []
        diag = []
        l = []
        c = None
        o = None
        for hs in hosts:
            dp("prev={o} hs={hs}", o=o, hs=hs)
            o = Connector(hs, q, msg, diag, prev=o)
            l.append(o)

        l[0].start()

        left = len(l)
        while (left > 0):
            c = q.get()
            left -= 1
            if c:
                break

        for x in reversed(l):
            if x.sock is not c:
                x.abort_connection()

        for x in l:
            try:
                x.join(timeout=0.05)
                # If connection cancelling is not working, or
                # connection attempt is blocked on DNS resolving,
                # this join will block.  Ignore any error with
                # a tiny waiting allowance.
                if x.is_alive():
                    dp("debug oops: {x} still alive.  ignoring.", x=x)
            except RuntimeError:
                pass

        msg = "\n".join(msg)
        diag = "\n".join(diag)

        return c, msg, diag

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
        mo = re.match(r"^((?P<cascade>[-*])?(?P<wait>\d+(\.\d+)?):)?(?P<host>[^/:]+)(/(?P<mask>\d+))?:(?P<port>\d+)$", hspec)
        if not mo:
            raise RuntimeError("bad spec: {}".format(hspec))
        w = mo.group('wait')
        w = float(w) if w else 0.0
        h = mo.group('host')
        nm = mo.group('mask')
        nm = int(nm) if nm else None
        p = int(mo.group('port'))
        c = mo.group('cascade') is None
        hostlist.append(HostSpec(wait = w, host = h, mask = nm, port = p, cascade=c))

    c, msg, diag = Connector.get_fastest_connection(hostlist)

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
