from __future__ import annotations
import time
from typing import Dict, List, Optional
from utils import log, now_iso, ExpiringSet
from protocols import TYPE_MESSAGE, TYPE_HELLO, TYPE_ECHO, TYPE_LSP, build_message, PROTO_LSR
from dijkstra import build_next_hop_table

class LSDB:
    def __init__(self, max_age:float=60.0):
        self.db: Dict[str, Dict] = {}
        self.max_age = max_age

    def apply_lsp(self, origin:str, seq:int, links:Dict[str, float]) -> bool:
        cur = self.db.get(origin)
        if cur is None or seq > cur['seq']:
            self.db[origin] = {'seq': seq, 'links': dict(links), 'ts': time.time()}
            return True
        return False

    def age_out(self):
        now = time.time()
        stale = [o for o, v in self.db.items() if now - v['ts'] > self.max_age]
        for o in stale:
            self.db.pop(o, None)

    def graph(self) -> Dict[str, Dict[str, float]]:
        # DIRIGIDO: solo origin -> nb (no duplicar aristas en ambos sentidos)
        g: Dict[str, Dict[str, float]] = {}
        for origin, rec in self.db.items():
            g.setdefault(origin, {})
            for nb, cost in rec['links'].items():
                g[origin][str(nb)] = float(cost)
        return g

    def as_dict(self) -> Dict:
        return {o: {'seq': v['seq'], 'links': v['links'], 'age': round(time.time()-v['ts'],1)} for o, v in self.db.items()}

class LSRRouter:
    def __init__(self, node, metric:str='hop', lsp_interval:float=10.0, max_age:float=60.0):
        self.node = node
        self.metric = metric
        self.lsdb = LSDB(max_age=max_age)
        self.seq = 0
        self.lsp_interval = lsp_interval
        self.seen_lsp_ids = ExpiringSet(120)
        self.next_hop: Dict[str, str] = {}
        self.dist: Dict[str, float] = {}
        self.paths: Dict[str, List[str]] = {}

    def originate_lsp(self):
        self.seq += 1
        links = {}
        for nb in self.node.neighbors:
            if self.metric == 'rtt':
                cost = float(self.node.last_rtt(nb) or 1.0)
            else:
                cost = 1.0
            links[nb] = cost
        payload = {"origin": self.node.id, "seq": self.seq,
                   "links": [{"to": k, "cost": v} for k,v in links.items()]}
        msg = build_message(PROTO_LSR, TYPE_LSP, self.node.id, "*", ttl=16,
                            payload=payload, headers={"ts": now_iso()})
        changed = self._apply_lsp_message(msg)
        if changed:
            self._run_spf()
        self._flood_lsp(msg, exclude=None)

    def handle_lsp(self, msg:dict, incoming_neighbor:Optional[str]):
        if not self.seen_lsp_ids.add_if_new(msg["id"]):
            return
        changed = self._apply_lsp_message(msg)
        if changed:
            self._run_spf()
        self._flood_lsp(msg, exclude=incoming_neighbor)

    def _apply_lsp_message(self, msg:dict) -> bool:
        payload = msg.get("payload") or {}
        origin = payload.get("origin")
        seq = int(payload.get("seq", 0))
        links_list = payload.get("links", [])
        links = {item["to"]: float(item["cost"]) for item in links_list if "to" in item}
        return self.lsdb.apply_lsp(origin, seq, links)

    def _flood_lsp(self, msg:dict, exclude:Optional[str]):
        if msg.get("ttl", 0) <= 0:
            return
        fwd = dict(msg)
        fwd["ttl"] = msg["ttl"] - 1
        headers = dict(fwd.get("headers") or {})
        headers["last_hop"] = self.node.id
        trail = list(headers.get("trail", []))
        trail = (trail + [self.node.id])[-3:]
        headers["trail"] = trail
        fwd["headers"] = headers
        for nb in self.node.neighbors:
            if nb == exclude:
                continue
            self.node.send_direct(nb, fwd)

    def _run_spf(self):
        self.lsdb.age_out()
        g = self.lsdb.graph()
        self.next_hop, self.dist, self.paths = build_next_hop_table(g, self.node.id)

    def on_receive(self, msg:dict, incoming_neighbor:Optional[str]):
        mtype = msg.get("type")
        ttl = msg.get("ttl", 0)

        if mtype == TYPE_MESSAGE:
            if msg.get("to") == self.node.id:
                log(f"[DATA] from {msg['from']} to {self.node.id}: {msg.get('payload')}")
                return
            if ttl <= 0:
                return

            dest = msg.get("to")
            nh = self.next_hop.get(dest)

            # Atajo: si es vecino directo
            if nh is None and dest in getattr(self.node, "neighbors", []):
                nh = dest

            # Fallback para transitividad cuando el origen aún no aprendió rutas:
            # - evita eco al que lo envió (incoming_neighbor)
            # - usa un vecino determinístico
            if nh is None:
                nbs = [n for n in self.node.neighbors if n != incoming_neighbor]
                if nbs:
                    nh = sorted(nbs)[0]  # determinista

            if not nh:
                log(f"[drop] no route from {self.node.id} to {dest}")
                return

            fwd = dict(msg)
            fwd["ttl"] = ttl - 1
            headers = dict(fwd.get("headers") or {})
            headers["last_hop"] = self.node.id

            # ---- FIX: anti-bucle correcto ----
            # Solo descartamos si ya nos vimos en el trail y el paquete VIENE de la red.
            trail = list(headers.get("trail", []))
            if incoming_neighbor is not None and self.node.id in trail:
                return
            trail = (trail + [self.node.id])[-3:]
            headers["trail"] = trail
            # ----------------------------------

            fwd["headers"] = headers
            self.node.send_direct(nh, fwd)

        elif mtype == TYPE_LSP:
            self.handle_lsp(msg, incoming_neighbor)

        elif mtype == TYPE_HELLO:
            # No responder con ECHO en el cable (algunos peers lo rechazan)
            return

        elif mtype == TYPE_ECHO:
            self.node.on_echo(msg)

    def on_tick(self):
        self._run_spf()
