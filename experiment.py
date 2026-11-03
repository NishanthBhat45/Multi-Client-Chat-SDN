#!/usr/bin/env python3
"""Automated evaluation: static baseline vs dynamic SDN QoS, with/without background load,
for several client counts. Needs the Ryu controller already running.

Usage: sudo python3 experiment.py        -> results.csv + printed table
"""
import csv
import json
import os
import socket
import statistics
import subprocess
import time

from mininet.log import setLogLevel

from topo import CTRL_IP, build, setup_qos, teardown_qos

HERE = os.path.dirname(os.path.abspath(__file__))
SIZES = [5, 10, 25, 50]
MSGS = 30


def send_mode(mode):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.sendto(json.dumps({'type': 'mode', 'value': mode}).encode(), ('127.0.0.1', 9999))
    time.sleep(1)


def pct(v, p):
    v = sorted(v)
    return v[min(len(v) - 1, int(len(v) * p))] if v else float('nan')


def run_load(h, prefix, n):
    return h.popen(['python3', os.path.join(HERE, 'bench.py'), '--prefix', prefix, '--n', str(n),
                    '--msgs', str(MSGS)], stdout=subprocess.PIPE, text=True)


def main():
    setLogLevel('warning')
    net, s1, (h1, h2, h3, h4) = build()
    setup_qos()
    rows = []
    try:
        h1.cmd('python3 %s/server.py --ctrl %s:9999 > %s/server.log 2>&1 &' % (HERE, CTRL_IP, HERE))
        h1.cmd('iperf -s -u > /dev/null 2>&1 &')
        time.sleep(2)
        for mode in ('static', 'dynamic'):
            send_mode(mode)
            for bg in (False, True):
                for n in SIZES:
                    bgp = None
                    if bg:
                        bgp = h2.popen(['iperf', '-c', '10.0.0.1', '-u', '-b', '12M', '-t', '60'],
                                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                        time.sleep(2)
                    half = max(1, n // 2)
                    procs = {'premium': run_load(h3, 'vip', half), 'normal': run_load(h4, 'usr', n - half or 1)}
                    for tier, p in procs.items():
                        out, _ = p.communicate()
                        d = json.loads(out.strip().splitlines()[-1])
                        r, c = d['rtt'], d['connect']
                        rows.append({'mode': mode, 'background': int(bg), 'clients': n, 'tier': tier,
                                     'connect_ms': round(statistics.mean(c), 1) if c else '',
                                     'rtt_mean_ms': round(statistics.mean(r), 1) if r else '',
                                     'rtt_p50_ms': round(pct(r, .5), 1), 'rtt_p95_ms': round(pct(r, .95), 1),
                                     'rtt_p99_ms': round(pct(r, .99), 1), 'errors': d['errors']})
                        print(rows[-1], flush=True)
                    if bgp:
                        bgp.terminate()
                        time.sleep(1)
    finally:
        with open(os.path.join(HERE, 'results.csv'), 'w', newline='') as f:
            if rows:
                w = csv.DictWriter(f, fieldnames=rows[0].keys())
                w.writeheader()
                w.writerows(rows)
        h1.cmd('pkill -f server.py; pkill iperf')
        teardown_qos()
        net.stop()


if __name__ == '__main__':
    main()
