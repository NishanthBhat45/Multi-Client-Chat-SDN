#!/usr/bin/env python3
"""Mininet topology: h1=chat server, h2=background traffic, h3/h4=client hosts, one OVS switch.
A root-namespace interface (10.0.0.254) lets the server reach the Ryu event listener.
Bottleneck: 10 Mbps HTB with 3 queues on the switch port towards the server (s1-eth1).

Usage:  sudo python3 topo.py      (opens the Mininet CLI)
"""
import subprocess
from functools import partial

from mininet.cli import CLI
from mininet.log import setLogLevel
from mininet.net import Mininet
from mininet.node import Node, OVSSwitch, RemoteController

CTRL_IP = '10.0.0.254'
RATE = 10_000_000


def connect_to_root(net, switch, ip):
    """Attach a root-namespace interface to the switch (also starts the network)."""
    root = Node('root', inNamespace=False)
    intf = net.addLink(root, switch).intf1
    root.setIP(ip, intf=intf)
    net.start()
    return root


def build():
    net = Mininet(controller=None, switch=partial(OVSSwitch, protocols='OpenFlow13'),
                  autoSetMacs=True)
    net.addController('c0', controller=RemoteController, ip='127.0.0.1', port=6653)
    s1 = net.addSwitch('s1')
    hosts = [net.addHost('h%d' % i, ip='10.0.0.%d/24' % i) for i in range(1, 5)]
    for h in hosts:                       # h1 must be first so it lands on s1-eth1
        net.addLink(h, s1)
    connect_to_root(net, s1, CTRL_IP + '/24')    # also starts the network
    return net, s1, hosts


def setup_qos(port='s1-eth1'):
    def q(name, mn, prio):
        return ['--', '--id=@' + name, 'create', 'queue', 'other-config:min-rate=%d' % mn,
                'other-config:max-rate=%d' % RATE, 'other-config:priority=%d' % prio]
    cmd = ['ovs-vsctl', '--', 'set', 'port', port, 'qos=@qos', '--', '--id=@qos', 'create', 'qos',
           'type=linux-htb', 'other-config:max-rate=%d' % RATE,
           'queues:0=@q0', 'queues:1=@q1', 'queues:2=@q2']
    cmd += q('q0', 1_000_000, 3) + q('q1', 3_000_000, 1) + q('q2', 3_000_000, 2)
    subprocess.run(cmd, check=True)


def teardown_qos(port='s1-eth1'):
    subprocess.run(['ovs-vsctl', 'clear', 'port', port, 'qos'], check=False)
    subprocess.run(['ovs-vsctl', '--all', 'destroy', 'qos'], check=False)
    subprocess.run(['ovs-vsctl', '--all', 'destroy', 'queue'], check=False)


if __name__ == '__main__':
    setLogLevel('info')
    net, s1, hosts = build()
    setup_qos()
    try:
        CLI(net)
    finally:
        teardown_qos()
        net.stop()
