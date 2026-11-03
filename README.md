# Multi-Client Chat Network with SDN Traffic Prioritisation

Mini project 8 (SDN - Socket Programming): a multi-client chat application over **TCP sockets**, deployed in a
**Mininet** network and managed by a **Ryu (OpenFlow 1.3)** controller that classifies chat traffic per connection and
applies user and room policies through OVS QoS queues.

## 1. What it does
- Concurrent TCP chat server: registration, login, chat rooms, private messages, online-user list.
- The server reports connection events (login, tier change, block, close) to the SDN controller.
- The controller classifies each chat connection, assigns it to a priority queue, and drops traffic of blocked users.
- Policies: per-user tier, per-room tier (joining a premium room raises a user's priority), admin block/unblock/tier change.
- Evaluation: scalability and response time under congestion, **dynamic SDN QoS vs. a static baseline**.
- Browser UI with a live RTT graph, SDN flow/queue viewer, mode switch and load test.

## 2. Architecture
```
   browser (in VM)                   Ryu controller  (OpenFlow 1.3, tcp/6653)
        | HTTP                        ^  OpenFlow           ^
 +------v-------+                     |                     | UDP JSON events (udp/9999)
 | webui.py     |  h3, h4             |                     |
 | client.py    |-----TCP :5000--> [ s1 : Open vSwitch ] <---+-- via root-namespace link 10.0.0.254
 +--------------+                     |  s1-eth1: 10 Mbps HTB, queues q1 / q2 / q0
 h2: iperf UDP flood ---------------->|
                                      v
                              h1: server.py (chat server, 10.0.0.1)
```
| Host | IP | Role |
|---|---|---|
| h1 | 10.0.0.1 | chat server (`server.py`), iperf receiver; its switch port `s1-eth1` is the 10 Mbps bottleneck |
| h2 | 10.0.0.2 | background traffic generator (iperf UDP) |
| h3 | 10.0.0.3 | clients / web UI (premium users in the experiment) |
| h4 | 10.0.0.4 | clients / web UI (normal users in the experiment) |

**Queues on `s1-eth1`** (HTB, 10 Mbps): `q1` premium (priority 1, min 3 Mbps), `q2` normal (priority 2, min 3 Mbps),
`q0` everything else, i.e. background traffic (priority 3, min 1 Mbps).

**Controller flow table (table 0)**

| Priority | Match | Action |
|---|---|---|
| 200 | blocked connection | drop |
| 100 | one chat connection (4-tuple, both directions) | `set_queue(tier)` + output (idle timeout 30 s) |
| 50 | any TCP packet to/from port 5000 | send to controller (classification point) |
| 1 | destination MAC (learning switch) | output (non-chat traffic, queue 0) |
| 0 | table-miss | send to controller |

**Modes:** `dynamic` = QoS by policy; `static` = baseline, same topology and queues but no `set_queue`, so chat shares
queue 0 with the background traffic.

**Socket-SDN integration:** `server.py` sends UDP JSON events to the controller:
`conn` (login or tier change), `block`, `close`; the controller also accepts `mode`. On `conn` it deletes that
connection's flows so the next packet is re-classified; on `block` it installs priority-200 drop flows.

## 3. Chat protocol (TCP port 5000, newline-delimited text)
| Command | Reply |
|---|---|
| `REGISTER <user> <pass>` | `OK registered` / `ERR user exists` |
| `LOGIN <user> <pass>` | `OK logged in tier=<t> admin=<0/1>` |
| `JOIN <room>`, `LEAVE <room>` | `OK ...` |
| `ROOMS`, `USERS` | `OK ...` (admins also get each user's tier) |
| `MSG <room> <text>` | `OK`; members receive `MSG <room> <from> <text>` |
| `PM <user> <text>` | `OK` / `ERR user offline`; recipient receives `PM <from> <text>` |
| `PING <x>` | `PONG <x>` (used for RTT measurement) |
| `BLOCK <user>`, `UNBLOCK <user>`, `SETTIER <user> premium\|normal` | admin only |
| `QUIT` | `OK bye` |

Unsolicited lines: `INFO ...` (joins, leaves, "your tier is now ..."). Passwords are stored salted and hashed (SHA-256).

**Policy (`policy.json`):** `users` (explicit tiers, `blocked`), `prefix_tiers` (e.g. names starting `vip` are premium),
`rooms` (e.g. `announce` is premium), `admins`. A connection's tier is the highest of its user tier and the tiers of the rooms it has joined.

## 4. Repository layout
| File | Role |
|---|---|
| `server.py` | threaded TCP chat server |
| `client.py` | interactive terminal client |
| `webui.py` | browser UI bridge (stdlib only), live RTT graph, admin and SDN panels |
| `controller.py` | Ryu app: learning switch, classification, queues, blocking, modes |
| `topo.py` | Mininet topology and QoS queue setup |
| `bench.py` | concurrent load generator (connect time and message ack latency) |
| `experiment.py` | automated static-vs-dynamic evaluation, writes `results.csv` |
| `setmode.py` | switch the controller between `static` and `dynamic` |
| `policy.json` | user, room and admin policies |

## 5. Environment setup
Tested on Ubuntu in VirtualBox with Python 3.10. Requirements: Mininet, Open vSwitch, `iperf`, Ryu.
```bash
sudo apt update && sudo apt install -y mininet openvswitch-switch iperf python3-pip python3-venv git
python3 -m venv ~/ryu-env && source ~/ryu-env/bin/activate
pip install --upgrade pip wheel "setuptools==67.6.1" pbr
pip install --no-build-isolation ryu "eventlet==0.33.3"
# Ryu 4.34 imports a symbol that newer eventlet removed; replace that import with a placeholder
# (only needed for Ryu's websocket support, not for OpenFlow apps):
sed -i 's/from eventlet.wsgi import ALREADY_HANDLED/ALREADY_HANDLED = object()/' \
  ~/ryu-env/lib/python3.10/site-packages/ryu/app/wsgi.py
ryu-manager --version      # should print: ryu-manager 4.34
```

## 6. How to run
```bash
# Terminal 1 - controller (inside the Ryu venv)
source ~/ryu-env/bin/activate && ryu-manager controller.py

# Terminal 2 - network (no venv)
sudo mn -c && sudo python3 topo.py
mininet> h1 python3 server.py --ctrl 10.0.0.254:9999 > server.log 2>&1 &
mininet> h1 iperf -s -u > /dev/null 2>&1 &
mininet> h3 python3 webui.py --port 8080 > webui_h3.log 2>&1 &
mininet> h4 python3 webui.py --port 8080 > webui_h4.log 2>&1 &
# open http://10.0.0.3:8080 and http://10.0.0.4:8080 in a browser inside the VM
# or use terminal clients:  mininet> xterm h3     then   python3 client.py 10.0.0.1
```
Switch mode: `python3 setmode.py static` or `dynamic` (or use the buttons in the UI).
Inspect SDN state: `sudo ovs-ofctl -O OpenFlow13 dump-flows s1` and `... queue-stats s1`.

**Automated evaluation** (controller must be running; stop any manually started server first):
```bash
sudo python3 experiment.py        # writes results.csv
```
It runs static and dynamic modes, with and without a 12 Mbps UDP background flood, for 5, 10, 25 and 50 clients
(half premium on h3, half normal on h4, 30 messages each) and records connect time and p50/p95/p99 message latency.

## 7. Test cases
| # | Scenario | Steps | Expected result |
|---|---|---|---|
| T1 | Registration and login | `/register`, `/login` | `OK`; bad password gives `ERR bad credentials` |
| T2 | Rooms | `/join`, `/msg`, `/leave` | members receive messages; non-members get `ERR` |
| T3 | Private message | `/pm user text` | only the recipient receives it; offline user gives `ERR user offline` |
| T4 | Concurrent clients | `bench.py` or UI scalability test, 50 clients | all log in, 0 errors |
| T5 | User policy | log in as `alice` (premium) and a normal user, `dump-flows` | alice's flows `set_queue:1`, others `set_queue:2` |
| T6 | Room policy | normal user `/join announce` | flows change to `set_queue:1`; `/leave announce` reverts |
| T7 | Congestion | start the iperf flood, compare static and dynamic RTT | dynamic mode keeps chat RTT low |
| T8 | Blocking | admin `/block user` | connection closed, priority-200 drop flows, re-login refused |
| T9 | Tier change | admin `/settier user premium` | user's flows are re-classified immediately |

## 8. Results
_Fill in from `results.csv` after running `experiment.py`, and add graphs or screenshots under `docs/`._

| Mode | Background load | Clients | Tier | Mean RTT (ms) | p95 (ms) | p99 (ms) | Errors |
|---|---|---|---|---|---|---|---|
| static | off | | | | | | |
| static | on | | | | | | |
| dynamic | on | | | | | | |

## 9. Known limitations
- One OVS switch; the bottleneck is modelled on the server-facing port only.
- Controller events use UDP (fire-and-forget); a lost event delays reclassification until the flow idle timeout.
- Tier changes and blocks are kept in server memory and reset on restart; blocking is also enforced at login.
- No TLS; password hashing is for demonstration only.
- Premium and normal queues both have guaranteed bandwidth, so the largest effect is chat vs background traffic.

## 10. Team
| Name | SRN | Contribution |
|---|---|---|
| Nishanth Ramesh Bhat | PES1UG25CS350 | Chat application: server.py, client.py, policy.json, webui.py |
| Nithin Kumar K | PES1UG25CS353 | SDN and network: controller.py, topo.py |
| Nihal Narayanan | PES1UG25CS335 | Integration: bench.py, experiment.py, README |
