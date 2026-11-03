#!/usr/bin/env python3
"""Interactive chat client.  Usage: python3 client.py <server_ip> [port]

Commands: /register u p  /login u p  /join room  /leave room  /msg room text
          /pm user text  /rooms  /users  /ping  /block user  /unblock user  /settier user premium|normal (admin)  /quit
"""
import socket
import sys
import threading
import time

MAP = {'register': 'REGISTER', 'login': 'LOGIN', 'join': 'JOIN', 'leave': 'LEAVE',
       'msg': 'MSG', 'pm': 'PM', 'rooms': 'ROOMS', 'users': 'USERS', 'block': 'BLOCK', 'unblock': 'UNBLOCK', 'settier': 'SETTIER',
       'quit': 'QUIT'}


def reader(sock):
    for line in sock.makefile('r', encoding='utf-8', errors='replace'):
        line = line.rstrip('\n')
        if line.startswith('PONG '):
            line = 'PONG rtt=%.1f ms' % ((time.perf_counter() - float(line[5:])) * 1000)
        print('\r' + line + '\n> ', end='', flush=True)
    print('\n[disconnected]')
    sys.exit(0)


def main():
    host = sys.argv[1] if len(sys.argv) > 1 else '10.0.0.1'
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 5000
    sock = socket.create_connection((host, port))
    threading.Thread(target=reader, args=(sock,), daemon=True).start()
    while True:
        try:
            line = input('> ').strip()
        except EOFError:
            break
        if not line.startswith('/'):
            print('commands start with /  (see header of client.py)')
            continue
        cmd, _, rest = line[1:].partition(' ')
        if cmd == 'ping':
            out = 'PING %f' % time.perf_counter()
        elif cmd in MAP:
            out = (MAP[cmd] + ' ' + rest).strip()
        else:
            print('unknown command')
            continue
        sock.sendall((out + '\n').encode())
        if cmd == 'quit':
            break


if __name__ == '__main__':
    main()
