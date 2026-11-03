#!/usr/bin/env python3
"""Switch the controller between 'static' (baseline) and 'dynamic' (SDN QoS) mode.
Usage: python3 setmode.py static|dynamic"""
import json
import socket
import sys

if len(sys.argv) != 2 or sys.argv[1] not in ('static', 'dynamic'):
    sys.exit(__doc__)
socket.socket(socket.AF_INET, socket.SOCK_DGRAM).sendto(
    json.dumps({'type': 'mode', 'value': sys.argv[1]}).encode(), ('127.0.0.1', 9999))
print('mode ->', sys.argv[1])
