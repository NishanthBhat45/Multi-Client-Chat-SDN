#!/usr/bin/env python3
"""Multi-client TCP chat server that reports connection events to the SDN controller.

Protocol (newline-delimited text):
  REGISTER <user> <pass> | LOGIN <user> <pass> | JOIN <room> | LEAVE <room>
  ROOMS | USERS | MSG <room> <text> | PM <user> <text> | PING <x> | BLOCK <user> | QUIT
Replies: OK ..., ERR ..., PONG <x>, MSG <room> <from> <text>, PM <from> <text>, INFO ...
"""
import argparse, hashlib, json, os, re, secrets, socket, threading

NAME_RE = re.compile(r'^[A-Za-z0-9_]{3,16}$')
RANK = {'normal': 0, 'premium': 1}
DEFAULT_POLICY = {"default_tier": "normal", "users": {}, "prefix_tiers": {},
                  "rooms": {}, "admins": []}


class Client:
    def __init__(self, sock, addr):
        self.sock, self.addr = sock, addr
        self.name, self.rooms, self.tier = None, set(), 'normal'
        self.closed = False
        self.wlock = threading.Lock()

    def send(self, line):
        try:
            with self.wlock:
                self.sock.sendall((line + '\n').encode())
            return True
        except OSError:
            return False


class ChatServer:
    def __init__(self, a):
        self.a = a
        self.lock = threading.RLock()
        self.users = self._load(a.users, {})
        self.policy = self._load(a.policy, DEFAULT_POLICY)
        self.online, self.rooms = {}, {}
        self.ctrl = None
        if a.ctrl:
            h, p = a.ctrl.split(':')
            self.ctrl = (h, int(p))
            self.usock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    @staticmethod
    def _load(path, default):
        try:
            with open(path) as f:
                return json.load(f)
        except (OSError, ValueError):
            return dict(default)

    def _save_users(self):
        with open(self.a.users, 'w') as f:
            json.dump(self.users, f)

    # ---------- policy / SDN ----------
    def user_tier(self, name):
        p = self.policy
        if name in p.get('users', {}):
            return p['users'][name]
        for pre, tier in p.get('prefix_tiers', {}).items():
            if name.startswith(pre):
                return tier
        return p.get('default_tier', 'normal')

    def conn_tier(self, c):
        tier = self.user_tier(c.name)
        for r in c.rooms:
            rt = self.policy.get('rooms', {}).get(r, 'normal')
            if RANK.get(rt, 0) > RANK.get(tier, 0):
                tier = rt
        return tier

    def notify(self, c, typ):
        if not self.ctrl:
            return
        ev = {"type": typ, "client": [c.addr[0], c.addr[1]],
              "server_ip": c.sock.getsockname()[0], "user": c.name, "tier": c.tier}
        try:
            self.usock.sendto(json.dumps(ev).encode(), self.ctrl)
        except OSError:
            pass

    def update_tier(self, c):
        new = self.conn_tier(c)
        if new != c.tier:
            c.tier = new
            c.send('INFO your tier is now %s' % new)
            self.notify(c, 'conn')

    # ---------- plumbing ----------
    def serve_client(self, c):
        f = c.sock.makefile('r', encoding='utf-8', errors='replace', newline='\n')
        try:
            for raw in f:
                line = raw.strip()
                if line:
                    self.dispatch(c, line)
                if c.closed:
                    break
        except (OSError, ValueError):
            pass
        finally:
            self.drop(c)

    def dispatch(self, c, line):
        cmd, _, rest = line.partition(' ')
        fn = getattr(self, 'cmd_' + cmd.lower(), None) if cmd.isalpha() else None
        if not fn:
            return c.send('ERR unknown command')
        if cmd.upper() not in ('REGISTER', 'LOGIN', 'PING', 'QUIT') and not c.name:
            return c.send('ERR login required')
        fn(c, rest.strip())

    def drop(self, c):
        with self.lock:
            if c.name and self.online.get(c.name) is c:
                for r in list(c.rooms):
                    self.rooms.get(r, set()).discard(c.name)
                    self.broadcast(r, 'INFO %s left %s' % (c.name, r))
                del self.online[c.name]
            c.closed = True
        self.notify(c, 'close')
        try:
            c.sock.close()
        except OSError:
            pass

    def broadcast(self, room, line, skip=None):
        for n in list(self.rooms.get(room, ())):
            t = self.online.get(n)
            if t and t is not skip:
                t.send(line)

    # ---------- commands ----------
    def cmd_register(self, c, rest):
        parts = rest.split()
        if len(parts) != 2 or not NAME_RE.match(parts[0]):
            return c.send('ERR usage: REGISTER <user 3-16 alnum> <pass>')
        name, pw = parts
        with self.lock:
            if name in self.users:
                return c.send('ERR user exists')
            salt = secrets.token_hex(8)
            self.users[name] = {'salt': salt,
                                'hash': hashlib.sha256((salt + pw).encode()).hexdigest()}
            self._save_users()
        c.send('OK registered')

    def cmd_login(self, c, rest):
        parts = rest.split()
        if len(parts) != 2 or c.name:
            return c.send('ERR usage: LOGIN <user> <pass>')
        name, pw = parts
        u = self.users.get(name)
        if not u or hashlib.sha256((u['salt'] + pw).encode()).hexdigest() != u['hash']:
            return c.send('ERR bad credentials')
        if self.user_tier(name) == 'blocked':
            return c.send('ERR blocked')
        with self.lock:
            if name in self.online:
                return c.send('ERR already logged in')
            c.name = name
            self.online[name] = c
            self._join(c, 'general')
            c.tier = self.conn_tier(c)
        c.send('OK logged in tier=%s admin=%d' % (c.tier, c.name in self.policy.get('admins', [])))
        self.notify(c, 'conn')

    def _join(self, c, room):
        self.rooms.setdefault(room, set()).add(c.name)
        c.rooms.add(room)
        self.broadcast(room, 'INFO %s joined %s' % (c.name, room), skip=c)

    def cmd_join(self, c, room):
        if not NAME_RE.match(room):
            return c.send('ERR bad room name')
        with self.lock:
            self._join(c, room)
            self.update_tier(c)
        c.send('OK joined %s' % room)

    def cmd_leave(self, c, room):
        with self.lock:
            if room not in c.rooms:
                return c.send('ERR not in room')
            c.rooms.discard(room)
            self.rooms[room].discard(c.name)
            self.broadcast(room, 'INFO %s left %s' % (c.name, room))
            self.update_tier(c)
        c.send('OK left %s' % room)

    def cmd_rooms(self, c, _):
        with self.lock:
            c.send('OK ' + ' '.join('%s:%d' % (r, len(m)) for r, m in self.rooms.items() if m))

    def cmd_users(self, c, _):
        admin = c.name in self.policy.get('admins', [])
        with self.lock:      # admins also get each user's effective tier as name:tier
            c.send('OK ' + ' '.join('%s:%s' % (n, u.tier) if admin else n
                                    for n, u in self.online.items()))

    def cmd_msg(self, c, rest):
        room, _, text = rest.partition(' ')
        if room not in c.rooms:
            return c.send('ERR not in room')
        self.broadcast(room, 'MSG %s %s %s' % (room, c.name, text), skip=c)
        c.send('OK')

    def cmd_pm(self, c, rest):
        to, _, text = rest.partition(' ')
        t = self.online.get(to)
        if not t:
            return c.send('ERR user offline')
        t.send('PM %s %s' % (c.name, text))
        c.send('OK')

    def cmd_ping(self, c, rest):
        c.send('PONG ' + rest)

    def cmd_block(self, c, target):
        if c.name not in self.policy.get('admins', []):
            return c.send('ERR admin only')
        with self.lock:
            self.policy.setdefault('users', {})[target] = 'blocked'
            t = self.online.get(target)
        if t:
            t.tier = 'blocked'
            self.notify(t, 'block')        # controller installs drop flow
            t.send('ERR blocked by admin')
            t.closed = True
            try:
                t.sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        c.send('OK blocked %s' % target)

    def cmd_settier(self, c, rest):
        if c.name not in self.policy.get('admins', []):
            return c.send('ERR admin only')
        target, _, tier = rest.partition(' ')
        tier = tier.strip()
        if tier not in RANK:
            return c.send('ERR usage: SETTIER <user> <premium|normal>')
        with self.lock:
            if target not in self.users:
                return c.send('ERR no such user')
            if self.policy.get('users', {}).get(target) == 'blocked':
                return c.send('ERR user is blocked; UNBLOCK first')
            self.policy.setdefault('users', {})[target] = tier
            t = self.online.get(target)
            if t:
                self.update_tier(t)       # pushes INFO to the user + event to the controller
        c.send('OK tier of %s set to %s' % (target, tier))

    def cmd_unblock(self, c, target):
        if c.name not in self.policy.get('admins', []):
            return c.send('ERR admin only')
        with self.lock:
            if self.policy.get('users', {}).get(target) == 'blocked':
                del self.policy['users'][target]
        c.send('OK unblocked %s' % target)

    def cmd_quit(self, c, _):
        c.send('OK bye')
        c.closed = True

    def run(self):
        s = socket.socket()
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((self.a.host, self.a.port))
        s.listen(512)
        print('chat server on %s:%d' % (self.a.host, self.a.port), flush=True)
        while True:
            sock, addr = s.accept()
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            threading.Thread(target=self.serve_client, args=(Client(sock, addr),),
                             daemon=True).start()


if __name__ == '__main__':
    here = os.path.dirname(os.path.abspath(__file__))
    ap = argparse.ArgumentParser()
    ap.add_argument('--host', default='0.0.0.0')
    ap.add_argument('--port', type=int, default=5000)
    ap.add_argument('--ctrl', help='controller event address ip:port, e.g. 10.0.0.254:9999')
    ap.add_argument('--policy', default=os.path.join(here, 'policy.json'))
    ap.add_argument('--users', default=os.path.join(here, 'users.json'))
    ChatServer(ap.parse_args()).run()
