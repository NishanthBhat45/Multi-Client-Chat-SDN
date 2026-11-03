#!/usr/bin/env python3
"""Load generator: N concurrent clients register, log in, then send chat messages and time the
server's OK acknowledgement. Prints one JSON line with raw samples (milliseconds).

Usage: python3 bench.py --server 10.0.0.1 --prefix vip --n 10 --msgs 30 --interval 0.1
"""
import argparse
import json
import socket
import threading
import time


def worker(i, a, out):
    name = '%s%03d' % (a.prefix, i)
    try:
        t0 = time.perf_counter()
        s = socket.create_connection((a.server, a.port), timeout=15)
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        f = s.makefile('r', encoding='utf-8', errors='replace', newline='\n')

        def req(line):
            s.sendall((line + '\n').encode())
            for resp in f:                       # skip async MSG/PM/INFO lines
                if resp.startswith(('OK', 'ERR', 'PONG')):
                    return resp
            raise ConnectionError('closed')

        req('REGISTER %s pw' % name)             # ERR if it already exists: fine
        if not req('LOGIN %s pw' % name).startswith('OK'):
            raise ConnectionError('login failed')
        out['connect'].append((time.perf_counter() - t0) * 1000)
        time.sleep(a.settle)                     # let the controller re-classify the flow
        for k in range(a.msgs):
            t = time.perf_counter()
            if req('MSG general hello %d' % k).startswith('OK'):
                out['rtt'].append((time.perf_counter() - t) * 1000)
            else:
                out['errors'] += 1
            time.sleep(a.interval)
        req('QUIT')
        s.close()
    except (OSError, ConnectionError):
        out['errors'] += 1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--server', default='10.0.0.1')
    ap.add_argument('--port', type=int, default=5000)
    ap.add_argument('--prefix', default='usr')
    ap.add_argument('--n', type=int, default=10)
    ap.add_argument('--msgs', type=int, default=30)
    ap.add_argument('--interval', type=float, default=0.1)
    ap.add_argument('--settle', type=float, default=1.0)
    a = ap.parse_args()
    out = {'connect': [], 'rtt': [], 'errors': 0}
    ts = [threading.Thread(target=worker, args=(i, a, out)) for i in range(a.n)]
    for t in ts:
        t.start()
        time.sleep(0.02)
    for t in ts:
        t.join()
    print(json.dumps(out))


if __name__ == '__main__':
    main()
