"""Ryu app: learning switch + per-connection chat classification, QoS queues and blocking.

Run:  ryu-manager controller.py
Flow table layout (table 0):
  priority 200  drop flows for blocked connections
  priority 100  per-connection chat flows (4-tuple) -> set_queue(tier) + output
  priority  50  any TCP packet to/from port 5000 -> controller (classification point)
  priority   1  plain L2 forwarding (non-chat traffic, default queue 0 = low priority)
  priority   0  table-miss -> controller
Server->controller events (UDP JSON, port 9999): conn, block, close, mode.
"""
import json
import socket

from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, set_ev_cls
from ryu.lib import hub
from ryu.lib.packet import ethernet, ipv4, packet, tcp
from ryu.ofproto import ofproto_v1_3

CHAT_PORT = 5000
CTRL_PORT = 9999
QUEUE = {'premium': 1, 'normal': 2}      # queue 0 = everything else (background)
ETH_IP, TCP_PROTO = 0x0800, 6


class ChatSDN(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dps = {}
        self.mac_port = {}       # dpid -> {mac: port}
        self.conn_tier = {}      # (client_ip, client_port) -> tier
        self.blocked = set()
        self.mode = 'dynamic'    # 'dynamic' = QoS by policy, 'static' = baseline (no queues)
        self.count = {'premium': 0, 'normal': 0, 'dropped': 0}
        hub.spawn(self._listen)

    # ---------- helpers ----------
    def _flow(self, dp, prio, match, actions, idle=0, hard=0):
        p, o = dp.ofproto_parser, dp.ofproto
        inst = [p.OFPInstructionActions(o.OFPIT_APPLY_ACTIONS, actions)] if actions else []
        dp.send_msg(p.OFPFlowMod(datapath=dp, priority=prio, match=match, instructions=inst,
                                 idle_timeout=idle, hard_timeout=hard))

    def _del(self, dp, match):
        p, o = dp.ofproto_parser, dp.ofproto
        dp.send_msg(p.OFPFlowMod(datapath=dp, command=o.OFPFC_DELETE, out_port=o.OFPP_ANY,
                                 out_group=o.OFPG_ANY, match=match))

    def _out(self, dp, in_port, port, data, queue=None):
        p, o = dp.ofproto_parser, dp.ofproto
        acts = ([p.OFPActionSetQueue(queue)] if queue is not None else []) + \
               [p.OFPActionOutput(port)]
        dp.send_msg(p.OFPPacketOut(datapath=dp, buffer_id=o.OFP_NO_BUFFER, in_port=in_port,
                                   actions=acts, data=data))

    def _queue(self, tier):
        return QUEUE.get(tier, 2) if self.mode == 'dynamic' else None

    def _redirect_flows(self, dp):
        p, o = dp.ofproto_parser, dp.ofproto
        to_ctrl = [p.OFPActionOutput(o.OFPP_CONTROLLER, o.OFPCML_NO_BUFFER)]
        for field in ('tcp_dst', 'tcp_src'):
            m = p.OFPMatch(eth_type=ETH_IP, ip_proto=TCP_PROTO, **{field: CHAT_PORT})
            self._flow(dp, 50, m, to_ctrl)

    @staticmethod
    def _tuples(p, cip, cport, sip):
        up = p.OFPMatch(eth_type=ETH_IP, ip_proto=TCP_PROTO, ipv4_src=cip, tcp_src=cport,
                        ipv4_dst=sip, tcp_dst=CHAT_PORT)
        down = p.OFPMatch(eth_type=ETH_IP, ip_proto=TCP_PROTO, ipv4_src=sip, tcp_src=CHAT_PORT,
                          ipv4_dst=cip, tcp_dst=cport)
        return up, down

    # ---------- switch setup ----------
    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def features(self, ev):
        dp = ev.msg.datapath
        p, o = dp.ofproto_parser, dp.ofproto
        self.dps[dp.id] = dp
        self._flow(dp, 0, p.OFPMatch(),
                   [p.OFPActionOutput(o.OFPP_CONTROLLER, o.OFPCML_NO_BUFFER)])
        self._redirect_flows(dp)
        self.logger.info('switch %s connected', dp.id)

    # ---------- data plane ----------
    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def packet_in(self, ev):
        msg = ev.msg
        dp, o, p = msg.datapath, msg.datapath.ofproto, msg.datapath.ofproto_parser
        in_port = msg.match['in_port']
        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocol(ethernet.ethernet)
        if eth is None or eth.ethertype == 0x88cc:
            return
        table = self.mac_port.setdefault(dp.id, {})
        table[eth.src] = in_port
        out_port = table.get(eth.dst, o.OFPP_FLOOD)
        ip, t = pkt.get_protocol(ipv4.ipv4), pkt.get_protocol(tcp.tcp)

        if ip and t and CHAT_PORT in (t.src_port, t.dst_port):
            return self._chat(dp, msg, in_port, out_port, ip, t)

        if out_port != o.OFPP_FLOOD:
            self._flow(dp, 1, p.OFPMatch(eth_dst=eth.dst), [p.OFPActionOutput(out_port)])
        self._out(dp, in_port, out_port, msg.data)

    def _chat(self, dp, msg, in_port, out_port, ip, t):
        o, p = dp.ofproto, dp.ofproto_parser
        key = (ip.src, t.src_port) if t.dst_port == CHAT_PORT else (ip.dst, t.dst_port)
        match = p.OFPMatch(eth_type=ETH_IP, ip_proto=TCP_PROTO, ipv4_src=ip.src,
                           ipv4_dst=ip.dst, tcp_src=t.src_port, tcp_dst=t.dst_port)
        if key in self.blocked:
            self._flow(dp, 200, match, [], hard=60)       # empty actions = drop
            self.count['dropped'] += 1
            return
        tier = self.conn_tier.get(key, 'normal')
        q = self._queue(tier)
        acts = ([p.OFPActionSetQueue(q)] if q is not None else []) + [p.OFPActionOutput(out_port)]
        if out_port != o.OFPP_FLOOD:
            self._flow(dp, 100, match, acts, idle=30)
            self.count[tier if tier in self.count else 'normal'] += 1
        self._out(dp, in_port, out_port, msg.data, q)

    # ---------- events from the chat server ----------
    def _listen(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(('0.0.0.0', CTRL_PORT))
        self.logger.info('event listener on udp/%d', CTRL_PORT)
        while True:
            data, _ = s.recvfrom(4096)
            try:
                self._event(json.loads(data))
            except Exception as e:                           # never kill the listener
                self.logger.warning('bad event %r: %s', data, e)

    def _event(self, ev):
        typ = ev.get('type')
        if typ == 'mode':
            self.mode = ev['value']
            for dp in self.dps.values():
                p = dp.ofproto_parser
                self._del(dp, p.OFPMatch(eth_type=ETH_IP, ip_proto=TCP_PROTO, tcp_dst=CHAT_PORT))
                self._del(dp, p.OFPMatch(eth_type=ETH_IP, ip_proto=TCP_PROTO, tcp_src=CHAT_PORT))
                self._redirect_flows(dp)
            self.logger.info('mode -> %s', self.mode)
            return
        key = (ev['client'][0], int(ev['client'][1]))
        for dp in self.dps.values():
            up, down = self._tuples(dp.ofproto_parser, key[0], key[1], ev['server_ip'])
            if typ == 'conn':                 # tier changed: drop old flows, re-classify on next packet
                self.conn_tier[key] = ev['tier']
                self._del(dp, up); self._del(dp, down)
            elif typ == 'block':
                self.blocked.add(key)
                self._del(dp, down)
                self._flow(dp, 200, up, [], hard=60)
                self._flow(dp, 200, down, [], hard=60)
            elif typ == 'close':
                self.conn_tier.pop(key, None)
                self._del(dp, up); self._del(dp, down)
        self.logger.info('event %s user=%s tier=%s client=%s:%s counts=%s', typ,
                         ev.get('user'), ev.get('tier'), key[0], key[1], self.count)
