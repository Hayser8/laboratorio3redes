import argparse, threading, time, json, os, sys
from typing import Optional, List, Dict, Any
from utils import log, now_iso, pretty
from protocols import (
    new_message, new_hello, new_info, sanitize_incoming,
    TYPE_LSP, TYPE_MESSAGE, TYPE_HELLO, TYPE_INFO, TYPE_ECHO, PROTO_LSR
)
from transport_redis import RedisTransport
from config_loader import load_neighbors_only
from lsr import LSRRouter

# ---------------- names-redis.json loader ----------------
def load_names_redis(names_path: Optional[str]) -> tuple[Optional[str], Optional[int], Optional[str], Dict[str, str]]:
    """
    names-redis.json:
    {
      "host":"...", "port":6379, "pwd":"...",
      "type":"names",
      "config":{ "A":{"channel":"secX.topoY.node1"}, ... }
    }
    """
    if not names_path:
        return None, None, None, {}
    if not os.path.exists(names_path):
        raise FileNotFoundError(f"names file not found: {names_path}")
    with open(names_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if data.get("type") != "names":
        raise ValueError("names file must have type='names'")
    host = data.get("host"); port = data.get("port"); pwd = data.get("pwd")
    cfg = data.get("config") or {}
    chmap: Dict[str, str] = {}
    for nid, val in cfg.items():
        if isinstance(val, dict):
            ch = val.get("channel")
            if ch:
                chmap[str(nid)] = str(ch)
    return host, port, pwd, chmap

# ---------------- util: normalizadores robustos ----------------
def _normalize_links_any_to_list(links_any) -> List[Dict[str, float]]:
    """
    Convierte lo que sea (dict/list/tuplas/strings) a:
      [{'to':'B','cost':1.0}, ...]
    Acepta:
      - dict: {"B":1, "C":2}
      - list[dict]: [{"to":"B","cost":1}, {"id":"C","w":2}, {"neighbor":"D"}]
      - list[list|tuple]: [("B",1), ["C",2]]
      - list[str]: ["B","C"]  -> cost 1.0
    """
    out: List[Dict[str, float]] = []
    if links_any is None:
        return out

    def _pick_to(d: Dict[str, Any]) -> Optional[str]:
        for k in ("to", "id", "node", "nbr", "neighbor", "target"):
            if k in d and d[k] is not None:
                return str(d[k])
        return None

    if isinstance(links_any, dict):
        for k, v in links_any.items():
            try:
                c = float(v)
            except Exception:
                c = 1.0
            out.append({"to": str(k), "cost": c})
        return out

    if isinstance(links_any, list):
        for it in links_any:
            if isinstance(it, (list, tuple)) and len(it) >= 1:
                nb = str(it[0])
                try:
                    c = float(it[1]) if len(it) > 1 else 1.0
                except Exception:
                    c = 1.0
                out.append({"to": nb, "cost": c})
            elif isinstance(it, dict):
                nb = _pick_to(it)
                if not nb:
                    continue
                try:
                    c = float(it.get("cost", it.get("weight", it.get("w", 1))))
                except Exception:
                    c = 1.0
                out.append({"to": nb, "cost": c})
            elif isinstance(it, str):
                out.append({"to": it, "cost": 1.0})
        return out
    return out

def links_to_dict_for_print(links_any) -> Dict[str, float]:
    """Solo para imprimir topología bonita."""
    return {item["to"]: float(item["cost"]) for item in _normalize_links_any_to_list(links_any)}

def headers_to_dict(h) -> Dict[str, Any]:
    """Convierte headers (list/dict/None) a dict aceptable por lsr.py (solo para el router interno)."""
    if isinstance(h, dict):
        d = dict(h)
        if "trail" in d and not isinstance(d["trail"], list):
            try:
                d["trail"] = list(d["trail"])
            except Exception:
                d["trail"] = []
        return d
    if isinstance(h, list):
        return {"trail": list(h)}
    return {}

# ---------------- Node ----------------
class Node:
    def __init__(
        self, node_id: str, topo_path: str,
        *, hello_interval: float = 5.0, lsp_interval: float = 10.0,
        names_path: Optional[str] = None,
        redis_host: str = "localhost", redis_port: int = 6379, redis_db: int = 0,
        redis_pass: Optional[str] = None, debug: bool = False
    ):
        self.id = node_id
        self.debug = debug
        self.neighbors: List[str] = load_neighbors_only(topo_path, self.id)

        # names-redis (opcional): canal por nodo + credenciales
        n_host, n_port, n_pwd, chmap = (None, None, None, {})
        if names_path:
            try:
                n_host, n_port, n_pwd, chmap = load_names_redis(names_path)
            except Exception as e:
                log(f"[warn] names-redis load failed: {e}")

        host = n_host or redis_host
        port = n_port or redis_port
        pwd  = n_pwd  or redis_pass

        self.transport = RedisTransport(
            node_id=self.id, on_packet=self._on_packet,
            host=host, port=port, db=redis_db, password=pwd,
            channel_map=chmap
        )

        # Router LSR interno
        self.router = LSRRouter(self, metric='hop', lsp_interval=lsp_interval, max_age=60.0)

        self.hello_interval = hello_interval
        self.lsp_interval = lsp_interval
        self.default_ttl = 8

        self._hello_stop = threading.Event()
        self._lsp_stop = threading.Event()

        # RTT (si alguna vez reactivas ECHO con peers compatibles)
        self._hello_sent_ts: Dict[str, float] = {}
        self._last_rtt_ms: Dict[str, float] = {}

        # Conjunto de nodos “vistos”
        self._seen_nodes: set[str] = set([self.id])

    # --------- FILTRO DE SALIDA: convertir LSP -> INFO en el cable ----------
    def _convert_lsp_to_info_wire(self, pkt: dict) -> dict:
        pl = pkt.get("payload") or {}
        origin = str(pl.get("origin") or pkt.get("from") or self.id)
        try:
            seq = int(pl.get("seq") or int(time.time()))
        except Exception:
            seq = int(time.time())

        # Mapa { "B": 1.0, "C": 1.0 } para máxima interoperabilidad
        links_list = _normalize_links_any_to_list(pl.get("links") or {})
        links_map = {str(it["to"]): float(it.get("cost", 1.0)) for it in links_list}

        payload = {"origin": origin, "seq": seq, "links": links_map, "ts": now_iso()}
        info = new_info(self.id, payload, proto=PROTO_LSR, ttl=int(pkt.get("ttl", 5)))
        info["msg_id"] = pkt.get("msg_id")
        return info

    # -------- envío (toda salida pasa por aquí) --------
    def send_direct(self, neighbor_id: str, pkt: dict):
        # No publicar ECHO por el cable (peers de tu compa lo rechazan)
        if pkt.get("type") == TYPE_ECHO:
            if self.debug:
                log(f"[debug:{self.id}] drop ECHO to {neighbor_id} (wire-compat)")
            return

        wire_pkt = pkt
        if pkt.get("type") == TYPE_LSP:
            wire_pkt = self._convert_lsp_to_info_wire(pkt)

        if self.debug:
            log(f"[debug:{self.id}] send_direct -> {neighbor_id} type={wire_pkt.get('type')} ttl={wire_pkt.get('ttl')}")
        try:
            self.transport.publish_packet(neighbor_id, wire_pkt)
        except Exception as e:
            log(f"[error] publish to {neighbor_id} failed: {e}")

    # -------- APIs que el router usa --------
    def on_echo(self, msg: dict):
        sender = str(msg.get("from"))
        t0 = self._hello_sent_ts.pop(sender, None)
        if t0 is not None:
            rtt_ms = (time.monotonic() - t0) * 1000.0
            self._last_rtt_ms[sender] = rtt_ms
            if self.debug:
                log(f"[debug:{self.id}] RTT {sender} ≈ {rtt_ms:.1f} ms")

    def last_rtt(self, neighbor_id: str) -> Optional[float]:
        return self._last_rtt_ms.get(neighbor_id)

    # -------- ciclo de vida --------
    def start(self):
        log(f"[node] {self.id} up | neighbors={self.neighbors} | metric={self.router.metric}")
        try:
            self.transport.start()
        except Exception as e:
            log(f"[fatal] transport start failed: {e}")
            raise

        threading.Thread(target=self._hello_loop, name=f"hello-{self.id}", daemon=True).start()
        threading.Thread(target=self._lsp_loop,   name=f"lsp-{self.id}",   daemon=True).start()

        self._console_loop()

    def _hello_loop(self):
        while not self._hello_stop.is_set():
            pkt = new_hello(self.id, proto=PROTO_LSR, ttl=2)  # headers = [self_id]
            for nb in self.neighbors:
                self._hello_sent_ts[nb] = time.monotonic()
                self.send_direct(nb, pkt)
            self._hello_stop.wait(self.hello_interval)

    def _lsp_loop(self):
        # Genera LSP interno (no sale como LSP; send_direct lo convertirá a INFO)
        time.sleep(0.5)
        self.router.originate_lsp()
        while not self._lsp_stop.is_set():
            self._lsp_stop.wait(self.lsp_interval)
            if self._lsp_stop.is_set():
                break
            self.router.originate_lsp()

    # -------- recepción Redis --------
    def _on_packet(self, pkt: dict, _src: str):
        try:
            pkt = sanitize_incoming(pkt)  # deja headers como LISTA
        except Exception as e:
            if self.debug:
                log(f"[debug:{self.id}] sanitize_incoming failed: {e}")
            return

        # id para dedupe del router
        if "id" not in pkt and "msg_id" in pkt:
            pkt["id"] = pkt["msg_id"]

        # Calcula last_hop y normaliza headers a dict (para el router)
        h_orig = pkt.get("headers", [])
        if isinstance(h_orig, list) and h_orig:
            last_hop = h_orig[-1]
        elif isinstance(h_orig, dict):
            last_hop = h_orig.get("last_hop")
        else:
            last_hop = None
        pkt["headers"] = headers_to_dict(h_orig)

        mtype = pkt.get("type")

        # --- Detección de externos / first seen ---
        if mtype == TYPE_HELLO:
            frm = str(pkt.get("from"))
            if frm not in self.neighbors and frm != self.id:
                log(f"[warn] HELLO from non-configured neighbor {frm} (topology is directional; not adding)")
            if frm not in self._seen_nodes:
                self._seen_nodes.add(frm)
                log(f"[event] first_seen node {frm} via HELLO")

        elif mtype == TYPE_LSP:
            pl = pkt.get("payload") or {}
            origin = str(pl.get("origin") or pkt.get("from") or "")
            if origin and origin not in self._seen_nodes and origin != self.id:
                self._seen_nodes.add(origin)
                log(f"[event] first_seen node {origin} via LSP")

        elif mtype == TYPE_INFO:
            info = pkt.get("payload") or {}
            origin = str(info.get("origin") or pkt.get("from") or "")
            if origin and origin not in self._seen_nodes and origin != self.id:
                self._seen_nodes.add(origin)
                log(f"[event] first_seen node {origin} via INFO")

            # Compatibilidad: INFO -> LSP interno para LSDB/SPF
            links_any = info.get("links") or info.get("neighbors") or {}
            links_list = _normalize_links_any_to_list(links_any)
            try:
                seq = int(info.get("seq") or int(time.time()))
            except Exception:
                seq = int(time.time())
            fake = dict(pkt)
            fake["type"] = TYPE_LSP
            fake["payload"] = {"origin": origin, "seq": seq, "links": links_list}
            fake["headers"] = headers_to_dict(fake.get("headers"))
            try:
                self.router.on_receive(fake, last_hop)
            except Exception as e:
                log(f"[error] router.on_receive(info->lsp) failed: {e}")
            return  # ya manejado

        elif mtype == TYPE_MESSAGE:
            frm = str(pkt.get("from"))
            if frm and frm not in self._seen_nodes and frm != self.id:
                self._seen_nodes.add(frm)
                log(f"[event] first_seen node {frm} via MESSAGE")

        if self.debug:
            log(f"[debug:{self.id}] on_packet type={mtype} from={pkt.get('from')} ttl={pkt.get('ttl')} last_hop={last_hop}")

        try:
            self.router.on_receive(pkt, last_hop)
        except Exception as e:
            log(f"[error] router.on_receive failed: {e}")

    # -------- consola --------
    def _console_loop(self):
        help_text = (
            "Commands:\n"
            "  send <DEST> <TEXT>   - send DATA via LSR (router)\n"
            "  table                - routing table (next-hop, cost)\n"
            "  route <DEST>         - SPF path\n"
            "  topo                 - topology from LSDB (adjacency + edges)\n"
            "  graph                - DOT graph (copy to Graphviz)\n"
            "  lsdb                 - print LSDB (raw)\n"
            "  ttl <N>              - set default TTL for messages\n"
            "  lsp                  - originate LSP now (router)\n"
            "  help                 - show help\n"
            "  quit                 - exit\n"
        )
        log(help_text)
        while True:
            try:
                raw = input(f"[{self.id}]> ").strip()
            except (EOFError, KeyboardInterrupt):
                break
            if not raw:
                continue
            parts = raw.split()
            cmd = parts[0].lower()

            if cmd == "send" and len(parts) >= 3:
                dest = parts[1]; text = " ".join(parts[2:])
                self._send_data(dest, text)

            elif cmd == "table":
                self._print_table()

            elif cmd == "route" and len(parts) == 2:
                self._print_route(parts[1])

            elif cmd == "lsdb":
                try:
                    d = self.router.lsdb.as_dict()
                    log(pretty(d))
                except Exception:
                    log("[warn] LSDB not available")

            elif cmd == "topo":
                self._print_topology()

            elif cmd == "graph":
                self._print_graph_dot()

            elif cmd == "ttl":
                if len(parts) == 2:
                    try:
                        self.default_ttl = int(parts[1]); log(f"[cfg] TTL={self.default_ttl}")
                    except ValueError:
                        log("[warn] ttl must be integer")
                else:
                    log("[warn] usage: ttl <N>")

            elif cmd == "lsp":
                self.router.originate_lsp()

            elif cmd == "help":
                log(help_text)

            elif cmd == "quit":
                break

            else:
                log("Unknown command. Type 'help'.")
        self._shutdown()

    # -------- helpers consola --------
    def _send_data(self, dest: str, text: str):
        # Paquete "de cable"
        pkt_wire = new_message(self.id, dest, text, proto=PROTO_LSR, ttl=self.default_ttl)
        pkt_wire["id"] = pkt_wire.get("msg_id")

        # Copia para el router (headers como dict)
        pkt_rtr = dict(pkt_wire)
        pkt_rtr["headers"] = headers_to_dict(pkt_wire.get("headers"))

        # Deja que el router decida (directo, SPF o fallback)
        try:
            self.router.on_receive(pkt_rtr, None)
        except Exception as e:
            log(f"[error] router.on_receive(message) failed: {e}")

    def _print_table(self):
        table = getattr(self.router, "next_hop", {})
        if not table:
            log("[warn] routing table empty (aún no hay LSP/INFO válidos)")
            return
        log("Routing table (next-hop | cost):")
        for d in sorted(table.keys()):
            nh = table[d]
            cost = getattr(self.router, "dist", {}).get(d, "?")
            log(f"  {self.id}->{d} : next-hop={nh} cost={cost}")

    def _print_route(self, dest: str):
        path = getattr(self.router, "paths", {}).get(dest, [])
        log(" -> ".join(path) if path else f"[no-path] {self.id}->{dest}")

    def _print_topology(self):
        try:
            lsdb = self.router.lsdb.as_dict()
        except Exception:
            log("[warn] LSDB not available")
            return
        if not isinstance(lsdb, dict) or not lsdb:
            log("[warn] no topology yet")
            return

        log("Topology (adjacency):")
        for nid in sorted(lsdb.keys()):
            rec = lsdb[nid]
            if nid in ("null", None) or not isinstance(rec, dict):
                continue
            links = links_to_dict_for_print(rec.get("links", {}))
            nbs = list(links.keys())
            log(f"  {nid}: {', '.join(nbs) if nbs else '-'}")

        log("Edges (undirected, deduped):")
        printed = set()
        for a in sorted(k for k in lsdb.keys() if k not in ("null", None)):
            links_a = links_to_dict_for_print(lsdb[a].get("links", {}))
            for b, cost in links_a.items():
                edge = tuple(sorted((a, b)))
                if edge in printed or a == b:
                    continue
                links_b = links_to_dict_for_print(lsdb.get(b, {}).get("links", {}))
                opp = links_b.get(a)
                if opp is not None and opp != cost:
                    log(f"  {edge[0]} -- {edge[1]} (cost {cost}/{opp})")
                else:
                    log(f"  {edge[0]} -- {edge[1]} (cost {cost})")
                printed.add(edge)

    def _print_graph_dot(self):
        try:
            lsdb = self.router.lsdb.as_dict()
        except Exception:
            log("[warn] LSDB not available")
            return
        edges = set()
        nodes = set(k for k in lsdb.keys() if k not in ("null", None))
        for a in nodes:
            links = links_to_dict_for_print(lsdb[a].get("links", {}))
            for b, cost in links.items():
                edge = tuple(sorted((a, b)))
                edges.add((edge[0], edge[1], cost))
        lines = ["graph G {"]
        for n in sorted(nodes):
            lines.append(f'  "{n}";')
        for a, b, cost in sorted(edges):
            lines.append(f'  "{a}" -- "{b}" [label="{cost}"];')
        lines.append("}")
        log("\n".join(lines))

    def _shutdown(self):
        self._hello_stop.set(); self._lsp_stop.set()
        try:
            self.transport.stop()
        except Exception:
            pass
        log(f"[node] {self.id} down")

# ---------------- main ----------------
def main():
    ap = argparse.ArgumentParser(description="LSR Node (Redis Pub/Sub) — LSP interno, INFO en la red (interop)")
    ap.add_argument("--id", required=True)
    ap.add_argument("--topo", required=True)
    ap.add_argument("--names", default=None, help="ruta a names-redis.json (opcional)")
    ap.add_argument("--hello", type=float, default=5.0)
    ap.add_argument("--lsp", type=float, default=10.0)
    ap.add_argument("--redis-host", default="localhost")
    ap.add_argument("--redis-port", type=int, default=6379)
    ap.add_argument("--redis-db", type=int, default=0)
    ap.add_argument("--redis-pass", default=None)
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    try:
        node = Node(
            args.id, args.topo,
            hello_interval=args.hello, lsp_interval=args.lsp,
            names_path=args.names,
            redis_host=args.redis_host, redis_port=args.redis_port,
            redis_db=args.redis_db, redis_pass=args.redis_pass,
            debug=args.debug
        )
        node.start()
    except Exception as e:
        log(f"[fatal] node crashed during start: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
