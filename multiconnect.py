#!/usr/bin/python3
import sys
import threading
from threading import Thread
import queue
import socket
import traceback
import time
import re
import select

class Connector(Thread):
    def __init__(self, wait, host, mask, port, ret):
        super().__init__(daemon=True)
        self.host = host
        self.mask = mask
        self.port = port
        self.ret = ret
        self.wait = wait
        self.sock = None
        self.no_start = False
        self.done = False
        self.waitchan = queue.Queue()

    def run(self):
        try:
            self.sock = socket.socket()
            addr = socket.getaddrinfo(self.host, self.port,
                                      family=socket.AF_INET,
                                      proto=socket.IPPROTO_TCP)
            if self.mask:
                usock = socket.socket(type=socket.SOCK_DGRAM)
                usock.connect(addr[0][4])
                saddr = usock.getsockname()
                raddr = int.from_bytes(socket.inet_aton(addr[0][4][0]), 'big')
                laddr = int.from_bytes(socket.inet_aton(saddr[0]), 'big')
                mask = ((1 << self.mask) - 1) << (32 - self.mask)
                lnaddr = laddr & mask
                lnaddrs = socket.inet_ntoa(lnaddr.to_bytes(4, 'big'))
                if (raddr & mask) != lnaddr:
                    raise RuntimeError("{} does not belong to network {}/{}".format(
                        addr[0][4][0], lnaddrs, self.mask))

            #time.sleep(self.wait)
            if self.wait:
                try:
#                    print ("waiting {} sec with {}".format(self.wait, self.sock), file=sys.stderr)
                    self.waitchan.get(timeout=self.wait)
#                    print ("waiting {} sec with {} -> rcvd".format(self.wait, self.sock), file=sys.stderr)
                    return
                except queue.Empty:
#                    print ("waiting {} sec with {} -> none".format(self.wait, self.sock), file=sys.stderr)
                    pass
            if self.no_start:
                return
            self.sock.connect(addr[0][4])
            print("CONNECTED to {}:{}".format(self.host, self.port), file=sys.stderr)
            self.ret.put(self.sock)
        except:
            if not self.no_start:
                print("connect failed", file=sys.stderr)
                traceback.print_exc()
                self.done = True
            self.ret.put(None)

    def abort_connection(self):
        if self.done:
            return
        self.no_start = True
#        print ("sending abort to waiting {} sec with {}".format(self.wait, self.sock), file=sys.stderr)
        self.waitchan.put(True)
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
#            print("shutdown failed", file=sys.stderr)
#            traceback.print_exc()
            pass
        try:
            self.sock.close()
        except OSError:
#            print("close failed", file=sys.stderr)
#            traceback.print_exc()
            pass

    @classmethod
    def get_fastest_connection(klass, hosts):
        q = queue.Queue()
        l = []
        c = None
        for w, h, m, p in hosts:
            l.append(Connector(w, h, m, p, q))

        for x in l:
            x.start()

        left = len(l)
        while (left > 0):
            c = q.get()
            left -= 1
            #print("got c:{}".format(c))
            if c:
                break

        for x in l:
            if x.sock is not c:
                x.abort_connection()

        for x in l:
            x.join()

        return c

bufsize = 1048576
class Forwarder(Thread):
    
    def __init__(self, f, fr, to):
        super().__init__(daemon=False)
        self.fr = fr
        self.to = to
        self.f = f

    def run(self):
        try:
            while(True):
                if self.f:
                    r = self.fr.recv(bufsize)
                else:
                    r = self.fr.read(bufsize)
                if not r:
                    break
#                print("read {}".format(r), file=sys.stderr)
                if self.f:
                    self.to.write(r)
                else:
                    self.to.send(r)
        except OSError:
            print("send failed", file=sys.stderr)
            traceback.print_exc()

        if not self.f:
            try:
                self.to.shutdown(socket.SHUT_WR)
            except OSError:
                print("shutdown failed", file=sys.stderr)
                traceback.print_exc()

    @classmethod
    def run_parallel(klass, ff):
        l = []
        for f in ff:
            l.append(Forwarder(*f))
        for t in l:
            t.start()
        for t in l:
            t.join()

def main(argv):
    hostlist = []
    for hspec in argv:
        mo = re.match(r"^(?:(\d+(?:\.\d+)?):)?([^/:]+)(?:/(\d+))?:(\d+)$", hspec)
        if not mo:
            raise RuntimeError("bad spec: {}".format(hspec))
        w = mo.group(1)
        w = float(w) if w else 0.0
        h = mo.group(2)
        nm = mo.group(3)
        nm = int(nm) if nm else None
        p = int(mo.group(4))
        hostlist.append((w, h, nm, p))

    if not len(hostlist):
        print("no host list given.", file=sys.stderr)
        sys.exit(2)
    
    c = Connector.get_fastest_connection(hostlist)

    if not c:
        print("cannot connect to any given host.", file=sys.stderr)
        sys.exit(2)
    
    Forwarder.run_parallel(
        ((True, c, sys.stdout.buffer.raw),
         (False, sys.stdin.buffer.raw, c)))

    c.close()

    sys.exit(0)

if __name__=='__main__':
    main(sys.argv[1:])
