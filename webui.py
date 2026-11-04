#!/usr/bin/env python3
"""Web UI for the chat network (stdlib only).

Run this on a Mininet host so the chat traffic crosses the SDN switch, e.g.
    mininet> h3 python3 webui.py --port 8080 > webui_h3.log 2>&1 &
then open  http://10.0.0.3:8080  in a browser inside the VM.
Every browser login opens its own TCP connection to the chat server; the bridge times every
chat-message acknowledgement and an automatic 1 s PING, and the page plots them live.
"""
import argparse
import json
import os
import re
import socket
import subprocess
import sys
import threading
import time
import uuid
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

SESSIONS = {}
KINDS = {'cmd', 'pm', 'msg', 'ping', 'login', 'register', 'join', 'leave', 'rooms', 'users'}


class Session:
    def __init__(self, server, port, ping_every):
        self.sock = socket.create_connection((server, port), timeout=10)
        self.sock.settimeout(None)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.events, self.cv = [], threading.Condition()
        self.pending, self.plock = deque(), threading.Lock()
        self.alive, self.logged, self.ping_every = True, False, ping_every
        threading.Thread(target=self._reader, daemon=True).start()
        threading.Thread(target=self._pinger, daemon=True).start()

    def emit(self, **ev):
        with self.cv:
            self.events.append(ev)
            self.cv.notify_all()

    def send(self, line, kind='cmd', arg=None):
        if '\n' in line or '\r' in line or kind not in KINDS:
            return False
        with self.plock:                      # keep wire order == pending order
            self.pending.append((kind, time.perf_counter(), arg))
            try:
                self.sock.sendall((line + '\n').encode())
                return True
            except OSError:
                self.pending.pop()
                return False

    def close(self):
        self.alive = False
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.sock.close()

    def _pinger(self):
        while self.alive:
            time.sleep(self.ping_every)
            if self.logged:
                self.send('PING %f' % time.perf_counter(), 'ping')

    def _reader(self):
        f = self.sock.makefile('r', encoding='utf-8', errors='replace', newline='\n')
        try:
            for raw in f:
                self._on_line(raw.rstrip('\n'))
        except (OSError, ValueError):
            pass
        self.alive = False
        self.emit(type='closed')

    def _on_line(self, line):
        head, _, rest = line.partition(' ')
        if head in ('OK', 'ERR', 'PONG'):
            with self.plock:
                kind, t0, arg = self.pending.popleft() if self.pending else ('cmd', 0, None)
            ms = (time.perf_counter() - t0) * 1000
            if head == 'ERR':
                return self.emit(type='error', text=rest)
            if kind in ('ping', 'msg'):
                self.emit(type='rtt', kind=kind, ms=round(ms, 2))
            elif kind == 'login':
                self.logged = True
                m = re.search(r'tier=(\w+)', rest)
                self.emit(type='login', tier=m.group(1) if m else 'normal', admin='admin=1' in rest)
                self.send('ROOMS', 'rooms')
                self.send('USERS', 'users')
            elif kind == 'rooms':
                rooms = [dict(zip(('name', 'count'), r.split(':'))) for r in rest.split()]
                self.emit(type='rooms', rooms=rooms)
            elif kind == 'users':
                names, tiers = [], {}
                for tok in rest.split():
                    n, _, t = tok.partition(':')
                    names.append(n)
                    if t:
                        tiers[n] = t
                self.emit(type='users', users=names, tiers=tiers)
            elif kind in ('join', 'leave'):
                self.emit(type=kind, room=arg)
                self.send('ROOMS', 'rooms')
            elif kind == 'pm' and isinstance(arg, dict):
                self.emit(type='pm_sent', to=arg.get('to'), text=arg.get('text'))
            elif kind == 'cmd' and rest:
                self.emit(type='info', text=rest)
                if rest.startswith('tier of'):
                    self.send('USERS', 'users')
        elif head == 'MSG':
            room, user, text = (rest.split(' ', 2) + ['', ''])[:3]
            self.emit(type='msg', room=room, user=user, text=text)
        elif head == 'PM':
            user, _, text = rest.partition(' ')
            self.emit(type='pm', user=user, text=text)
        elif head == 'INFO':
            if rest.startswith('your tier is now '):
                self.emit(type='tier', tier=rest.split()[-1])
            else:
                self.emit(type='info', text=rest)
                if ' joined ' in rest or ' left ' in rest:
                    self.send('USERS', 'users')


HERE = os.path.dirname(os.path.abspath(__file__))
FLOW_RE = re.compile(r'n_packets=(\d+).*?priority=(\d+),tcp,nw_src=([\d.]+),nw_dst=([\d.]+),'
                     r'tp_src=(\d+),tp_dst=(\d+)\s+actions=(\S+)')


def _run(cmd, timeout=5):
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return r.stdout, r.stderr
    except (OSError, subprocess.TimeoutExpired) as e:
        return '', str(e)


def flows_snapshot():
    """Chat flows installed by the controller on s1 (per-connection + drop rules) and queue counters."""
    out, err = _run(['ovs-ofctl', '-O', 'OpenFlow13', 'dump-flows', 's1'])
    if not out:
        return {'error': 'cannot read switch s1: ' + (err.strip() or 'no output') +
                ' (run this UI from a Mininet host so it can reach OVS)'}
    flows = []
    for line in out.splitlines():
        m = FLOW_RE.search(line)
        if not m or int(m.group(2)) < 100:
            continue
        pk, prio, src, dst, sp, dp, act = m.groups()
        q = re.search(r'set_queue:(\d+)', act)
        flows.append({'src': '%s:%s' % (src, sp), 'dst': '%s:%s' % (dst, dp), 'pkts': int(pk),
                      'queue': int(q.group(1)) if q else None, 'drop': act == 'drop'})
    qout, _ = _run(['ovs-ofctl', '-O', 'OpenFlow13', 'queue-stats', 's1'])
    queues = {}
    for line in qout.splitlines():
        m = re.search(r'port\s+1\s+queue\s+(\d+).*?tx pkts=(\d+)', line)
        if m:
            queues[m.group(1)] = int(m.group(2))
    return {'flows': flows, 'queues': queues}


def set_mode(value):
    if value not in ('static', 'dynamic'):
        return {'error': 'bad mode'}
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.sendto(json.dumps({'type': 'mode', 'value': value}).encode(), Handler.ctrl)
    return {'ok': True}


def set_load(on, rate):
    p = Handler.load_proc
    if p and p.poll() is not None:
        Handler.load_proc = p = None
    if on and not p:
        if not re.fullmatch(r'\d{1,3}[MK]', rate):
            return {'error': 'bad rate'}
        try:
            Handler.load_proc = subprocess.Popen(
                ['iperf', '-c', Handler.default_server, '-u', '-b', rate, '-t', '300'],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except OSError as e:
            return {'error': 'cannot start iperf (%s). Is it installed?' % e}
    elif not on and p:
        p.terminate()
        Handler.load_proc = None
    return {'on': Handler.load_proc is not None}


def pct(v, p):
    v = sorted(v)
    return v[min(len(v) - 1, int(len(v) * p))] if v else float('nan')


def run_bench(n, msgs, prefix):
    cmd = [sys.executable, os.path.join(HERE, 'bench.py'), '--server', Handler.default_server,
           '--port', str(Handler.default_port), '--prefix', prefix, '--n', str(n),
           '--msgs', str(msgs), '--interval', '0.1', '--settle', '1']
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=180)
        d = json.loads(r.stdout.strip().splitlines()[-1])
    except (OSError, subprocess.TimeoutExpired, ValueError, IndexError) as e:
        return {'error': 'benchmark failed: %s' % e}
    rt, cn = d['rtt'], d['connect']
    f = lambda x: round(x, 1) if x == x else None
    return {'connect_ms': f(sum(cn) / len(cn)) if cn else None,
            'rtt_mean': f(sum(rt) / len(rt)) if rt else None,
            'rtt_p50': f(pct(rt, .5)), 'rtt_p95': f(pct(rt, .95)), 'rtt_p99': f(pct(rt, .99)),
            'errors': d['errors']}


class Handler(BaseHTTPRequestHandler):
    default_server, default_port, ping_every = '10.0.0.1', 5000, 1.0
    ctrl, load_proc = ('10.0.0.254', 9999), None

    def log_message(self, *a):
        pass

    def _json(self, obj, code=200):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        u = urlparse(self.path)
        if u.path == '/':
            page = PAGE.replace('__SERVER__', self.default_server).replace('__PORT__', str(self.default_port))
            body = page.encode()
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif u.path == '/api/poll':
            q = parse_qs(u.query)
            s = SESSIONS.get(q.get('sid', [''])[0])
            if not s:
                return self._json({'error': 'no session'}, 404)
            since = int(q.get('since', ['0'])[0])
            with s.cv:
                if len(s.events) <= since:
                    s.cv.wait(timeout=15)
                evs = s.events[since:]
            self._json({'events': evs, 'next': since + len(evs)})
        elif u.path == '/api/flows':
            self._json(flows_snapshot())
        else:
            self._json({'error': 'not found'}, 404)

    def do_POST(self):
        try:
            data = json.loads(self.rfile.read(int(self.headers.get('Content-Length', 0))) or b'{}')
        except ValueError:
            return self._json({'error': 'bad json'}, 400)
        if self.path == '/api/mode':
            return self._json(set_mode(data.get('value')))
        if self.path == '/api/load':
            return self._json(set_load(bool(data.get('on')), str(data.get('rate', '12M'))))
        if self.path == '/api/bench':
            try:
                n, m = max(1, min(200, int(data.get('n', 10)))), max(1, min(100, int(data.get('msgs', 20))))
            except (TypeError, ValueError):
                return self._json({'error': 'bad numbers'}, 400)
            prefix = data.get('prefix') if data.get('prefix') in ('vipld', 'usrld') else 'usrld'
            return self._json(run_bench(n, m, prefix))
        if self.path == '/api/connect':
            try:
                s = Session(data.get('server') or self.default_server,
                            int(data.get('port') or self.default_port), self.ping_every)
            except OSError as e:
                return self._json({'error': 'cannot connect: %s' % e}, 502)
            sid = uuid.uuid4().hex
            SESSIONS[sid] = s
            user, pw = str(data.get('user', '')).strip(), str(data.get('password', '')).strip()
            if data.get('register'):
                s.send('REGISTER %s %s' % (user, pw), 'register')
            s.send('LOGIN %s %s' % (user, pw), 'login')
            return self._json({'sid': sid})
        s = SESSIONS.get(data.get('sid'))
        if not s:
            return self._json({'error': 'no session'}, 404)
        if self.path == '/api/send':
            ok = s.send(str(data.get('line', '')), data.get('kind', 'cmd'), data.get('arg'))
            return self._json({'ok': ok})
        if self.path == '/api/disconnect':
            s.close()
            SESSIONS.pop(data.get('sid'), None)
            return self._json({'ok': True})
        self._json({'error': 'not found'}, 404)


PAGE = r'''<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>SDN Chat</title>
<style>
:root{--bg:#f6f7f9;--card:#fff;--fg:#1c2330;--mut:#6b7585;--line:#dde1e8;--acc:#2f6fed;--ok:#1f9d55;--warn:#d9822b;--me:#e5eeff;--grid:#e8ebf0}
@media (prefers-color-scheme:dark){:root{--bg:#12151b;--card:#1a1f28;--fg:#e6e9ef;--mut:#8a94a6;--line:#2b3340;--acc:#6b9bff;--ok:#3ecf80;--warn:#f0a050;--me:#25324d;--grid:#262d38}}
[hidden]{display:none!important}*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--fg);font:14px system-ui,sans-serif}
.card{background:var(--card);border:1px solid var(--line);border-radius:10px}
#login{max-width:340px;margin:12vh auto;padding:24px}#login h2{margin:0 0 14px}
label{display:block;font-size:12px;color:var(--mut);margin:10px 0 3px}
input,select{width:100%;padding:8px 10px;border:1px solid var(--line);border-radius:7px;background:var(--bg);color:var(--fg);font:inherit}
button{padding:8px 12px;border:1px solid var(--line);border-radius:7px;background:var(--card);color:var(--fg);font:inherit;cursor:pointer}
button.p{background:var(--acc);border-color:var(--acc);color:#fff}button:hover{filter:brightness(.96)}
.row{display:flex;gap:8px;margin-top:14px}.row>*{flex:1}.err{color:#d64545;margin-top:10px;min-height:1em;font-size:13px}
#app{display:grid;grid-template-columns:250px minmax(0,1fr) 380px;grid-template-rows:minmax(0,1fr);gap:12px;padding:12px;height:100vh}
aside,main,section{padding:12px;display:flex;flex-direction:column;min-height:0}
h3{margin:0 0 8px;font-size:12px;text-transform:uppercase;letter-spacing:.05em;color:var(--mut)}
.list{overflow:auto;margin-bottom:12px}.item{padding:6px 8px;border-radius:6px;cursor:pointer;display:flex;justify-content:space-between}
.item:hover{background:var(--bg)}.item.sel{background:var(--me);font-weight:600}.cnt{color:var(--mut)}
.badge{display:inline-block;padding:2px 9px;border-radius:99px;font-size:12px;font-weight:600;color:#fff;background:var(--mut)}
.badge.premium{background:var(--ok)}.badge.normal{background:var(--acc)}
#log{flex:1;overflow:auto;border:1px solid var(--line);border-radius:8px;padding:10px;background:var(--bg)}
.m{margin:3px 0;word-wrap:anywhere}.m .u{font-weight:600}.m.me{background:var(--me);border-radius:6px;padding:2px 6px}
.m.info{color:var(--mut);font-style:italic}.m.pm .u{color:var(--warn)}.m.error{color:#d64545}
.send{display:flex;gap:8px;margin-top:10px}.send input{flex:1}.send button{flex:0 0 auto}
canvas{width:100%;height:240px;border:1px solid var(--line);border-radius:8px;background:var(--bg)}
.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin:10px 0}.stat{border:1px solid var(--line);border-radius:8px;padding:6px 8px}
.stat b{display:block;font-size:17px}.stat span{font-size:11px;color:var(--mut)}
.lg{font-size:12px;color:var(--mut);display:flex;gap:14px;margin-top:6px}.dot{display:inline-block;width:10px;height:10px;border-radius:50%;margin-right:5px;vertical-align:-1px}
.mut{color:var(--mut)}aside,section{overflow:auto}.list{flex:0 0 auto;max-height:160px}.badge.adm{background:var(--warn)}
.box{border:1px solid var(--line);border-radius:8px;padding:10px;margin-bottom:12px}.hint{font-size:11px;color:var(--mut);margin-top:6px}
.seg{display:flex;gap:6px}.seg button{flex:1}.row2{display:flex;gap:8px;align-items:center;margin-top:8px}.row2 input[type=number]{width:70px}
.x{color:var(--mut);padding:0 4px;cursor:pointer}.x:hover{color:#d64545}.item.av{color:var(--mut)}
table{width:100%;border-collapse:collapse;font-size:12px;margin-top:8px}td,th{text-align:left;padding:3px 4px;border-bottom:1px solid var(--line)}
.chathead{display:flex;align-items:center;gap:8px;margin-bottom:8px;min-height:30px}.chathead h3{margin:0;flex:1;font-size:13px}
button.small{padding:3px 10px;font-size:12px}.composer{margin:0 0 10px!important}.badge.dm{background:var(--warn)}
main.dm{border-color:var(--warn)}main.dm .chathead{border-left:4px solid var(--warn);padding-left:8px}
.empty{color:var(--mut);text-align:center;margin-top:30px}.ub{background:#d64545;color:#fff;border-radius:99px;font-size:11px;padding:0 6px;margin-left:6px;font-weight:600}
.item.off{opacity:.55}.item>span:last-child{white-space:nowrap}
@media(max-width:1000px){#app{grid-template-columns:minmax(0,1fr);grid-template-rows:none;height:auto}main{order:-1}#log{flex:none;height:55vh;min-height:260px}}
</style></head><body>
<div id="login" class="card"><h2>SDN Chat</h2>
<label>Server</label><input id="srv" value="__SERVER__:__PORT__">
<label>Username (3-16 letters/digits/_)</label><input id="usr" autocomplete="off">
<label>Password</label><input id="pw" type="password">
<div class="row"><button class="p" id="bLogin">Log in</button><button id="bReg">Register &amp; log in</button></div>
<div class="err" id="lerr"></div></div>

<div id="app" hidden>
<aside class="card">
 <h3>You</h3><div style="margin-bottom:14px;display:flex;align-items:center;gap:6px;flex-wrap:wrap"><b id="me"></b> <span class="badge" id="tier">?</span> <span class="badge adm" id="adm" hidden>admin</span><button class="small" id="bOut" style="margin-left:auto">Disconnect</button></div>
 <h3>Your rooms</h3><div class="list" id="rooms"></div>
 <h3>Other rooms (click to join)</h3><div class="list" id="avail"></div>
 <div class="send" style="margin:0 0 14px"><input id="jr" placeholder="new/other room"><button id="bJoin">Join</button></div>
 <h3>Direct messages (click to chat)</h3><div class="list" id="users"></div>
 <div id="admin" hidden><h3>Admin</h3><div class="box"><input id="adu" placeholder="username">
  <div class="row2"><button id="bBlock">Block</button><button id="bUnblock">Unblock</button></div>
  <div class="row2"><button id="bUp">&#9650; Premium</button><button id="bDown">&#9660; Normal</button></div>
  <div class="hint">Block closes the connection and installs drop flows. Tier changes re-classify the user's flows at once. Room policy (e.g. announce) can still raise a user's effective tier.</div></div></div>
</aside>
<main class="card">
 <div class="chathead"><h3 id="tgt">Room: #general</h3><span class="badge dm" id="dmtag" hidden>private</span><button class="small" id="bClearChat" title="Clear this conversation from your screen">Clear chat</button></div>
 <div class="send composer"><input id="txt" placeholder="Type a message..." autocomplete="off"><button class="p" id="bSend">Send</button><button id="bBurst" title="Send 20 messages back-to-back to create load">Burst x20</button></div>
 <div id="log"></div>
</main>
<section class="card">
 <h3>Round-trip time (ms)</h3><canvas id="chart"></canvas>
 <div class="lg"><span><i class="dot" style="background:var(--acc)"></i>PING (every 1 s)</span><span><i class="dot" style="background:var(--warn)"></i>Chat message ack</span><span id="win"></span></div>
 <div class="stats"><div class="stat"><b id="sLast">-</b><span>last</span></div><div class="stat"><b id="sMean">-</b><span>mean</span></div><div class="stat"><b id="sP95">-</b><span>p95</span></div><div class="stat"><b id="sMax">-</b><span>max</span></div></div>
 <div class="row" style="margin-top:0"><button id="bCsv">Export RTT CSV</button><button id="bClr">Clear graph</button></div>
 <h3 style="margin-top:16px">SDN control</h3>
 <div class="box"><div class="seg"><button id="mStatic">Static (baseline)</button><button id="mDyn" class="p">Dynamic (SDN QoS)</button></div>
  <div class="row2"><button id="bLoad">Start congestion (12 Mbps UDP)</button><span id="loadst" class="mut">off</span></div>
  <div class="hint">Mode starts as dynamic. Congestion needs <code>iperf -s -u</code> running on h1.</div></div>
 <h3>SDN state (switch s1)</h3>
 <div class="box"><div class="row2"><button id="bFlows">Refresh</button><label class="mut"><input type="checkbox" id="auto"> auto (3 s)</label></div>
  <div id="fsum" class="hint"></div><div id="ftab"></div><div id="fq" class="hint"></div></div>
 <h3>Scalability test</h3>
 <div class="box"><div class="row2"><span class="mut">clients</span><input type="number" id="bn" value="20" min="1" max="200"><span class="mut">msgs</span><input type="number" id="bm" value="20" min="1" max="100"></div>
  <div class="row2"><select id="bt"><option value="usrld">normal users</option><option value="vipld">premium users</option></select><button id="bRun">Run test</button></div>
  <div class="hint" id="bstat"></div><div id="bres" style="margin-top:6px;font-size:12px"></div></div>
</section></div>

<script>
const $=id=>document.getElementById(id);
const convs={}, unread={};
let lastUsers=[], lastTiers={}, isAdmin=false, mode='dynamic', loadOn=false, sid=null, since=0, me='', target={type:'room',name:'general'}, joined=new Set(), samples=[], t0=Date.now();
const WINDOW=90; // seconds shown on the graph

async function api(path,body){const r=await fetch(path,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});return r.json();}
async function connect(reg){
  $('lerr').textContent='';
  const [server,port]=$('srv').value.split(':');
  const j=await api('/api/connect',{server,port,user:$('usr').value,password:$('pw').value,register:reg});
  if(j.error){$('lerr').textContent=j.error;return;}
  sid=j.sid;since=0;me=$('usr').value.trim();poll();
}
$('bLogin').onclick=()=>connect(false);$('bReg').onclick=()=>connect(true);
$('pw').onkeydown=e=>{if(e.key==='Enter')connect(false)};

async function poll(){
  const mine=sid;
  while(sid===mine&&sid){
    try{const r=await fetch(`/api/poll?sid=${sid}&since=${since}`);const j=await r.json();
      if(j.error){break;} since=j.next; j.events.forEach(handle);}
    catch(e){await new Promise(r=>setTimeout(r,1000));}
  }
}
async function send(line,kind,arg){
  if(!sid){addLog('error','',null,'Not connected. Click Disconnect, then log in again.');return {ok:false};}
  let j;try{j=await api('/api/send',{sid,line,kind,arg});}catch(e){j={ok:false};}
  if(!j.ok){addLog('error','',null,'Message not sent: connection lost. Click Disconnect, then log in again.');}
  return j;
}
const keyOf=t=>t.type+':'+t.name;
const conv=k=>convs[k]||(convs[k]=[]);
function entryEl(e){const d=document.createElement('div');d.className='m '+e.cls;
  if(e.user){const u=document.createElement('span');u.className='u';u.textContent=e.user+': ';d.appendChild(u);d.appendChild(document.createTextNode(e.text));}
  else d.textContent=e.text;return d;}
function renderLog(){
  const log=$('log');log.innerHTML='';const list=conv(keyOf(target));
  if(!list.length){const d=document.createElement('div');d.className='empty';
    d.textContent=target.type==='pm'?'No private messages with '+target.name+' yet. Type below and press Send to start.':'No messages in #'+target.name+' yet.';log.appendChild(d);}
  list.forEach(e=>log.appendChild(entryEl(e)));log.scrollTop=log.scrollHeight;}
function addLog(cls,html,user,text,key){
  key=key||keyOf(target);const e={cls,user,text};conv(key).push(e);
  if(conv(key).length>500)conv(key).shift();
  if(key===keyOf(target)){const log=$('log');const em=log.querySelector('.empty');if(em)em.remove();log.appendChild(entryEl(e));log.scrollTop=log.scrollHeight;}
  else if(user&&!/\bme\b/.test(cls)){unread[key]=(unread[key]||0)+1;renderRooms();renderUsers();}
}
function setTarget(t){
  target=t;unread[keyOf(t)]=0;const pm=t.type==='pm';
  $('tgt').textContent=pm?'Private chat with '+t.name:'Room: #'+t.name;
  $('dmtag').hidden=!pm;document.querySelector('main').classList.toggle('dm',pm);
  $('txt').placeholder=pm?'Message '+t.name+' privately...':'Message #'+t.name+'...';
  $('bBurst').disabled=pm;renderRooms();renderUsers();renderLog();$('txt').focus();
}
function dmNames(){const s=new Set(lastUsers.filter(u=>u!==me));
  Object.keys(convs).forEach(k=>{if(k.startsWith('pm:')&&convs[k].length)s.add(k.slice(3));});
  if(target.type==='pm')s.add(target.name);return [...s].sort();}
function clearConv(k){convs[k]=[];if(k===keyOf(target))renderLog();renderUsers();}
function resetChats(){Object.keys(convs).forEach(k=>delete convs[k]);Object.keys(unread).forEach(k=>delete unread[k]);}
$('bClearChat').onclick=()=>clearConv(keyOf(target));
function setTier(t){const b=$('tier');b.textContent=t;b.className='badge '+t;}

function handle(ev){
  switch(ev.type){
    case 'login': isAdmin=!!ev.admin;$('adm').hidden=!isAdmin;$('admin').hidden=!isAdmin;$('login').hidden=true;$('app').hidden=false;$('me').textContent=me;setTier(ev.tier);joined.add('general');resetChats();setTarget({type:'room',name:'general'});samples=[];t0=Date.now();draw();break;
    case 'tier': setTier(ev.tier);addLog('info','',null,'Your SDN traffic tier is now '+ev.tier);break;
    case 'rtt': samples.push({t:Date.now(),ms:ev.ms,kind:ev.kind});if(samples.length>5000)samples.shift();draw();break;
    case 'msg': addLog('','',ev.user,ev.text,'room:'+ev.room);break;
    case 'pm': addLog('pm','',ev.user,ev.text,'pm:'+ev.user);break;
    case 'pm_sent': addLog('pm me','','me',ev.text,'pm:'+ev.to);break;
    case 'info': {const m=/ (?:joined|left) (\w+)$/.exec(ev.text);addLog('info','',null,ev.text,m?'room:'+m[1]:undefined);break;}
    case 'error': if(!$('app').hidden)addLog('error','',null,'Error: '+ev.text);else if(ev.text!=='user exists'){$('lerr').textContent=ev.text;const s=sid;sid=null;if(s)api('/api/disconnect',{sid:s});} break;
    case 'join': joined.add(ev.room);renderRooms();break;
    case 'leave': joined.delete(ev.room);if(target.type==='room'&&target.name===ev.room)setTarget({type:'room',name:'general'});renderRooms();break;
    case 'rooms': renderRooms(ev.rooms);break;
    case 'users': lastUsers=ev.users;lastTiers=ev.tiers||{};renderUsers();break;
    case 'closed': if(sid){addLog('error','',null,'Disconnected from server. Click Disconnect, then log in again.');sid=null;if($('app').hidden)$('lerr').textContent=$('lerr').textContent||'Connection closed.';}break;
  }
}
let roomCounts={};
function renderRooms(list){
  if(list&&list.length){roomCounts={};list.forEach(r=>roomCounts[r.name]=r.count);}
  const box=$('rooms');box.innerHTML='';
  [...joined].sort().forEach(n=>{const d=document.createElement('div');d.className='item'+(target.type==='room'&&target.name===n?' sel':'');
    const l=document.createElement('span');l.textContent='# '+n;if(unread['room:'+n]){const b=document.createElement('span');b.className='ub';b.textContent=unread['room:'+n];l.appendChild(b);}const r=document.createElement('span');r.className='cnt';r.textContent=roomCounts[n]||'';
    if(n!=='general'){const x=document.createElement('span');x.className='x';x.textContent='\u2715';x.title='leave room';
      x.onclick=e=>{e.stopPropagation();send('LEAVE '+n,'leave',n);};r.appendChild(x);}
    d.append(l,r);d.onclick=()=>selectRoom(n);box.appendChild(d);});
  const av=$('avail');av.innerHTML='';
  Object.keys(roomCounts).filter(n=>!joined.has(n)).sort().forEach(n=>{const d=document.createElement('div');d.className='item av';
    const l=document.createElement('span');l.textContent='+ '+n;const r=document.createElement('span');r.className='cnt';r.textContent=roomCounts[n];
    d.append(l,r);d.onclick=()=>send('JOIN '+n,'join',n);av.appendChild(d);});
}
function renderUsers(){
  const box=$('users');box.innerHTML='';
  const act=(txt,title,fn)=>{const b=document.createElement('span');b.className='x';b.textContent=txt;b.title=title;b.onclick=e=>{e.stopPropagation();fn();};return b;};
  const names=dmNames();
  if(!names.length){const d=document.createElement('div');d.className='hint';d.textContent='No one else is online yet.';box.appendChild(d);}
  names.forEach(n=>{const online=lastUsers.includes(n);
    const d=document.createElement('div');d.className='item'+(target.type==='pm'&&target.name===n?' sel':'')+(online?'':' off');
    const l=document.createElement('span');l.textContent=n+(online?'':' (offline)');
    if(lastTiers[n]){const t=document.createElement('span');t.className='badge '+lastTiers[n];t.style.marginLeft='6px';t.textContent=lastTiers[n];l.appendChild(t);}
    if(unread['pm:'+n]){const b=document.createElement('span');b.className='ub';b.textContent=unread['pm:'+n];l.appendChild(b);}
    d.appendChild(l);
    const a=document.createElement('span');
    if(isAdmin&&online)a.append(act('\u25B2','Upgrade to premium',()=>send('SETTIER '+n+' premium','cmd')),
               act('\u25BC','Downgrade to normal',()=>send('SETTIER '+n+' normal','cmd')),
               act('block','Block this user',()=>{if(confirm('Block '+n+'?'))send('BLOCK '+n,'cmd');}));
    if((convs['pm:'+n]||[]).length)a.append(act('clear','Clear this conversation',()=>clearConv('pm:'+n)));
    d.appendChild(a);
    d.onclick=()=>setTarget({type:'pm',name:n});
    box.appendChild(d);});
}
function selectRoom(n){setTarget({type:'room',name:n});}
$('bJoin').onclick=()=>{const r=$('jr').value.trim();if(r){send('JOIN '+r,'join',r);$('jr').value='';}};
$('jr').onkeydown=e=>{if(e.key==='Enter')$('bJoin').click()};

async function sendText(text){
  if(target.type==='room'){const j=await send('MSG '+target.name+' '+text,'msg');if(j.ok)addLog('me','',me,text);}
  else{send('PM '+target.name+' '+text,'pm',{to:target.name,text});}   // echoed only after the server confirms delivery
}
$('bSend').onclick=()=>{const t=$('txt').value.trim();if(t){sendText(t);$('txt').value='';}};
$('txt').onkeydown=e=>{if(e.key==='Enter')$('bSend').click()};
$('bBurst').onclick=async()=>{if(target.type!=='room'){addLog('error','',null,'Select a room for a burst.');return;}
  for(let i=0;i<20;i++){send('MSG '+target.name+' burst '+(i+1),'msg');}addLog('info','',null,'Sent 20 messages in a burst.');};
$('bOut').onclick=async()=>{if(sid){const s=sid;sid=null;await api('/api/disconnect',{sid:s});}$('app').hidden=true;$('login').hidden=false;resetChats();joined.clear();target={type:'room',name:'general'};renderLog();};
$('bClr').onclick=()=>{samples=[];t0=Date.now();draw();};
setInterval(()=>{if(sid&&!$('app').hidden){send('USERS','users');send('ROOMS','rooms');}},5000);
setInterval(draw,1000);

$('bCsv').onclick=()=>{
  let csv='time_s,kind,rtt_ms\n';samples.forEach(s=>csv+=((s.t-t0)/1000).toFixed(2)+','+s.kind+','+s.ms+'\n');
  const a=document.createElement('a');a.href=URL.createObjectURL(new Blob([csv],{type:'text/csv'}));a.download='rtt_'+me+'.csv';a.click();
};

function pct(arr,p){if(!arr.length)return NaN;const s=[...arr].sort((a,b)=>a-b);return s[Math.min(s.length-1,Math.floor(s.length*p))];}
function fmt(v){return isNaN(v)?'-':v.toFixed(1);}
function draw(){
  const c=$('chart');if(!c.offsetWidth)return;
  const dpr=window.devicePixelRatio||1,W=c.clientWidth,H=c.clientHeight;
  if(c.width!==W*dpr||c.height!==H*dpr){c.width=W*dpr;c.height=H*dpr;}
  const x=c.getContext('2d');x.setTransform(dpr,0,0,dpr,0,0);x.clearRect(0,0,W,H);
  const cs=getComputedStyle(document.body),acc=cs.getPropertyValue('--acc'),warn=cs.getPropertyValue('--warn'),grid=cs.getPropertyValue('--grid'),mut=cs.getPropertyValue('--mut');
  const now=Date.now(),vis=samples.filter(s=>now-s.t<=WINDOW*1000);
  let max=Math.max(1,...vis.map(s=>s.ms))*1.15;
  const pad={l:40,r:8,t:8,b:18},pw=W-pad.l-pad.r,ph=H-pad.t-pad.b;
  const step=[0.5,1,2,5,10,20,50,100,200,500,1000,2000].find(s=>max/s<=5)||2000;max=Math.ceil(max/step)*step;
  x.font='11px system-ui';x.fillStyle=mut;x.strokeStyle=grid;x.lineWidth=1;
  for(let v=0;v<=max;v+=step){const y=pad.t+ph-(v/max)*ph;x.beginPath();x.moveTo(pad.l,y);x.lineTo(W-pad.r,y);x.stroke();x.fillText(v,4,y+4);}
  x.fillText('-'+WINDOW+'s',pad.l,H-4);x.fillText('now',W-pad.r-22,H-4);
  const px=s=>pad.l+(1-(now-s.t)/(WINDOW*1000))*pw,py=s=>pad.t+ph-(s.ms/max)*ph;
  const pings=vis.filter(s=>s.kind==='ping');
  x.strokeStyle=acc;x.lineWidth=2;x.beginPath();pings.forEach((s,i)=>i?x.lineTo(px(s),py(s)):x.moveTo(px(s),py(s)));x.stroke();
  x.fillStyle=acc;pings.forEach(s=>{x.beginPath();x.arc(px(s),py(s),2.5,0,7);x.fill();});
  x.fillStyle=warn;vis.filter(s=>s.kind==='msg').forEach(s=>{x.beginPath();x.arc(px(s),py(s),3.2,0,7);x.fill();});
  const ms=vis.map(s=>s.ms);
  $('sLast').textContent=fmt(vis.length?vis[vis.length-1].ms:NaN);
  $('sMean').textContent=fmt(ms.length?ms.reduce((a,b)=>a+b,0)/ms.length:NaN);
  $('sP95').textContent=fmt(pct(ms,.95));$('sMax').textContent=fmt(ms.length?Math.max(...ms):NaN);
  $('win').textContent=vis.length+' samples';
}
$('bBlock').onclick=()=>{const u=$('adu').value.trim();if(u&&confirm('Block '+u+'?'))send('BLOCK '+u,'cmd');};
$('bUnblock').onclick=()=>{const u=$('adu').value.trim();if(u)send('UNBLOCK '+u,'cmd');};
$('bUp').onclick=()=>{const u=$('adu').value.trim();if(u)send('SETTIER '+u+' premium','cmd');};
$('bDown').onclick=()=>{const u=$('adu').value.trim();if(u)send('SETTIER '+u+' normal','cmd');};

function setModeUI(){$('mStatic').className=mode==='static'?'p':'';$('mDyn').className=mode==='dynamic'?'p':'';}
async function setMode(v){const j=await api('/api/mode',{value:v});
  if(j.error){addLog('error','',null,'Mode change failed: '+j.error);return;}
  mode=v;setModeUI();addLog('info','',null,'Controller mode set to '+v+'; chat flows will be re-classified.');}
$('mStatic').onclick=()=>setMode('static');$('mDyn').onclick=()=>setMode('dynamic');setModeUI();

$('bLoad').onclick=async()=>{const j=await api('/api/load',{on:!loadOn,rate:'12M'});
  if(j.error){addLog('error','',null,j.error);return;}
  loadOn=j.on;$('bLoad').textContent=loadOn?'Stop congestion':'Start congestion (12 Mbps UDP)';$('loadst').textContent=loadOn?'running':'off';
  addLog('info','',null,loadOn?'Background UDP flood started toward the server.':'Background flood stopped.');};

async function refreshFlows(){
  let j;try{j=await (await fetch('/api/flows')).json();}catch(e){return;}
  const sum=$('fsum'),tab=$('ftab');tab.innerHTML='';
  if(j.error){sum.textContent=j.error;$('fq').textContent='';return;}
  const f=j.flows,c=fn=>f.filter(fn).length;
  sum.textContent='premium (q1): '+c(x=>x.queue===1)+'  |  normal (q2): '+c(x=>x.queue===2)+'  |  no queue: '+c(x=>!x.drop&&x.queue===null)+'  |  dropped: '+c(x=>x.drop);
  const t=document.createElement('table');t.innerHTML='<tr><th>from</th><th>to</th><th>pkts</th><th>action</th></tr>';
  f.sort((a,b)=>b.pkts-a.pkts).slice(0,30).forEach(x=>{const r=t.insertRow();
    [x.src,x.dst,x.pkts,x.drop?'DROP':(x.queue===null?'forward':'queue '+x.queue)].forEach(v=>{r.insertCell().textContent=v;});});
  tab.appendChild(t);
  const q=j.queues||{};$('fq').textContent=Object.keys(q).length?'server-port queue tx packets: '+Object.keys(q).sort().map(k=>'q'+k+'='+q[k]).join('  '):'';
}
$('bFlows').onclick=refreshFlows;
setInterval(()=>{if($('auto').checked&&!$('app').hidden)refreshFlows();},3000);

$('bRun').onclick=async()=>{const n=+$('bn').value||10,m=+$('bm').value||20,p=$('bt').value,b=$('bRun');
  b.disabled=true;$('bstat').textContent='running '+n+' clients...';
  let j;try{j=await api('/api/bench',{n,msgs:m,prefix:p});}catch(e){j={error:String(e)};}
  b.disabled=false;$('bstat').textContent='';
  if(j.error){$('bstat').textContent=j.error;return;}
  const d=document.createElement('div');d.className='m';
  d.textContent='['+mode+(loadOn?', congested':'')+'] '+n+' '+(p==='vipld'?'premium':'normal')+' clients: connect '+j.connect_ms+' ms | rtt mean '+j.rtt_mean+', p95 '+j.rtt_p95+', p99 '+j.rtt_p99+' ms | errors '+j.errors;
  $('bres').prepend(d);};
window.addEventListener('resize',draw);
</script></body></html>'''


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--host', default='0.0.0.0')
    ap.add_argument('--port', type=int, default=8080)
    ap.add_argument('--server', default='10.0.0.1', help='chat server IP')
    ap.add_argument('--chat-port', type=int, default=5000)
    ap.add_argument('--ping-every', type=float, default=1.0)
    ap.add_argument('--ctrl', default='10.0.0.254:9999', help='controller event address (for mode changes)')
    a = ap.parse_args()
    Handler.ctrl = (a.ctrl.split(':')[0], int(a.ctrl.split(':')[1]))
    Handler.default_server, Handler.default_port, Handler.ping_every = a.server, a.chat_port, a.ping_every
    print('web UI on http://%s:%d  (chat server %s:%d)' % (a.host, a.port, a.server, a.chat_port), flush=True)
    ThreadingHTTPServer((a.host, a.port), Handler).serve_forever()
