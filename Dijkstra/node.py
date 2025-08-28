import argparse, threading, time, json, os, heapq
from typing import Optional, List, Dict, Any, Tuple
from utils import log, now_iso, pretty
from protocols import (
    new_hello, new_message, sanitize_incoming, forward_transform, ExpiringSet,
    TYPE_HELLO, TYPE_MESSAGE,
    PROTO_DIJKSTRA,
)
from transport_redis import RedisTransport
from config_loader import load_neighbors_only, load_graph

# ---------- names-redis loader (opcional) ----------
def load_names_redis(names_path: Optional[str]) -> Tuple[Optional[str], Optional[int], Optional[str], Dict[str,str]]:
    if not names_path:
        return None, None, None, {}
    if not os.path.exists(names_path):
        raise FileNotFoundError(f"names file not found: {names_path}")
    with open(names_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if data.get("type") != "names":
        raise ValueError("names file must have type='names'")
    host = data.get("host")
    port = data.get("port")
    pwd  = data.get("pwd")
    cfg = data.get("config") or {}
    chmap = {}
    for nid, val in cfg.items():
        ch = val.get("channel") if isinstance(val, dict) else None
        if ch:
            chmap[str(nid)] = str(ch)
    return host, port, pwd, chmap

def _coerce_headers_list(h: Any) -> List[str]:
    # Por compat con el otro programa (que a veces manda dict)
    if isinstance(h, list):
        return [str(x) for x in h]
    if isinstance(h, dict):
        if "trail" in h and isinstance(h["trail"], list):
            return [str(x) for x in h["trail"]]
        if "path" in h and isinstance(h["path"], list):
            return [str(x) for x in h["path"]]
        if "last_hop" in h and h["last_hop"] is not None:
            return [str(h["last_hop"])]
    return []

# ---------- Nodo Dijkstra ----------
class Node:
    """
    Dijkstra centralizado (estático):
    - Carga el grafo completo del topo y calcula caminos más cortos desde self.id.
    - Usa next_hop/paths para forwardear MESSAGE.
    - HELLO sólo como keep-alive (no altera rutas).
    """
    def __init__(self, node_id:str, topo_path:str,
                 names_path: Optional[str] = None,
                 metric:str='hop', default_ttl:int=8, hello_interval:float=5.0,
                 redis_host:str="localhost", redis_port:int=6379, redis_db:int=0,
                 redis_pass: Optional[str] = None):
        self.id = node_id
        self.metric = metric
        self.G: Dict[str, Dict[str, int]] = load_graph(topo_path)
        self.neighbors: List[str] = load_neighbors_only(topo_path, self.id)

        # Redis (con names-redis si aplica)
        n_host, n_port, n_pwd, chmap = (None, None, None, {})
        if names_path:
            try:
                n_host, n_port, n_pwd, chmap = load_names_redis(names_path)
            except Exception as e:
                log(f"[warn] names-redis load failed ({e}); falling back to CLI host/port.")

        host = n_host or redis_host
        port = n_port or redis_port
        pwd  = n_pwd  or redis_pass

        self.transport = RedisTransport(
            node_id=self.id, on_packet=self._on_packet,
            host=host, port=port, db=redis_db, password=pwd,
            channel_map=chmap
        )

        self.default_ttl = default_ttl
        self.hello_interval = hello_interval
        self._hello_stop = threading.Event()
        self.seen = ExpiringSet(ttl_seconds=60)

        # Tablas
        self.dist: Dict[str, int] = {}
        self.prev: Dict[str, Optional[str]] = {}
        self.next_hop: Dict[str, str] = {}
        self.paths: Dict[str, List[str]] = {}

        self._recompute_routes()

    # ---------- Dijkstra ----------
    def _recompute_routes(self):
        src = self.id
        dist = {n: float('inf') for n in self.G}
        prev: Dict[str, Optional[str]] = {n: None for n in self.G}
        dist[src] = 0
        pq = [(0, src)]
        while pq:
            d, u = heapq.heappop(pq)
            if d != dist[u]:
                continue
            for v, w in self.G.get(u, {}).items():
                cost = 1 if self.metric == "hop" else int(w or 1)
                nd = d + cost
                if nd < dist[v]:
                    dist[v] = nd
                    prev[v] = u
                    heapq.heappush(pq, (nd, v))

        # reconstruye paths y next_hop
        paths: Dict[str, List[str]] = {}
        nh: Dict[str, str] = {}
        for dst in self.G:
            if dst == src or dist[dst] == float('inf'):
                continue
            # reconstrucción
            path = []
            cur = dst
            while cur is not None:
                path.append(cur)
                cur = prev[cur]
            path.reverse()  # src ... dst
            paths[dst] = [src] + path[1:] if path and path[0] == src else path
            if len(paths[dst]) >= 2:
                nh[dst] = paths[dst][1]

        self.dist = {k: (int(v) if v != float('inf') else v) for k, v in dist.items()}
        self.prev = prev
        self.paths = paths
        self.next_hop = nh
        log(f"[spf] computed: next_hop={nh}")

    # ---------- envío ----------
    def send_direct(self, neighbor_id:str, pkt:dict):
        self.transport.publish_packet(neighbor_id, pkt)

    def start(self):
        log(f"[node] {self.id} neighbors={self.neighbors} (dijkstra)")
        self.transport.start()
        time.sleep(0.5)
        threading.Thread(target=self._hello_loop, name=f"hello-{self.id}", daemon=True).start()
        self._console_loop()

    def _hello_loop(self):
        while not self._hello_stop.is_set():
            pkt = new_hello(self.id, proto=PROTO_DIJKSTRA, ttl=2)
            # opcional: hello solo a vecinos para no “contaminar” otros programas
            for nb in self.neighbors:
                self.send_direct(nb, pkt)
            self._hello_stop.wait(self.hello_interval)

    # ---------- recepción ----------
    def _on_packet(self, pkt:dict, _src:str):
        # compat: si headers llega como dict, conviértelo a lista antes de sanitize
        if isinstance(pkt, dict) and not isinstance(pkt.get("headers"), list):
            pkt = dict(pkt)
            pkt["headers"] = _coerce_headers_list(pkt.get("headers"))

        try:
            pkt = sanitize_incoming(pkt)
        except Exception as e:
            log(f"[drop] sanitize failed: {e}; raw={pretty(pkt)}")
            return

        # duplicados
        mid = pkt.get("msg_id")
        if mid and not self.seen.add_if_new(mid):
            return

        ptype = pkt.get("type")
        dest  = pkt.get("to")
        headers_list = pkt.get("headers", [])
        prev_hop = headers_list[-1] if headers_list else None

        if ptype == TYPE_HELLO:
            return  # no cambiamos rutas con HELLO

        if ptype == TYPE_MESSAGE:
            if dest == self.id:
                log(f"[deliver] {self.id} <- {pkt.get('from')}: {pkt.get('payload')}")
                return
            nh = self.next_hop.get(dest)
            # fallback directo si es vecino
            if not nh and dest in self.neighbors:
                nh = dest
                log(f"[fallback] using direct neighbor {dest} as next-hop")
            if not nh:
                log(f"[drop] no route {self.id}->{dest}")
                return
            fwd = forward_transform(pkt, self.id)
            if fwd is None:
                return
            self.send_direct(nh, fwd)
            return

        # Ignora otros tipos (LSP/INFO) para dijkstra centralizado
        return

    # ---------- consola ----------
    def _console_loop(self):
        help_text = (
            "Commands:\n"
            "  send <DEST> <TEXT>   - send DATA via Dijkstra (next-hop)\n"
            "  table                - print routing table (next-hop, cost)\n"
            "  route <DEST>         - show shortest path\n"
            "  recompute            - recompute SPF from topo\n"
            "  ttl <N>              - set default TTL\n"
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
            elif cmd == "recompute":
                self._recompute_routes(); log("[spf] recomputed.")
            elif cmd == "ttl" and len(parts) == 2:
                try:
                    self.default_ttl = int(parts[1]); log(f"default TTL set to {self.default_ttl}")
                except ValueError:
                    log("ttl must be an integer")
            elif cmd == "help":
                log(help_text)
            elif cmd == "quit":
                break
            else:
                log("Unknown command. Type 'help'.")
        self._shutdown()

    def _send_data(self, dest:str, text:str):
        pkt = new_message(self.id, dest, text, proto=PROTO_DIJKSTRA, ttl=self.default_ttl)
        nh = self.next_hop.get(dest)
        if not nh and dest in self.neighbors:
            nh = dest
            log(f"[fallback] using direct neighbor {dest} as next-hop")
        if not nh:
            log(f"[drop] no route {self.id}->{dest}")
            return
        self.send_direct(nh, pkt)

    def _print_table(self):
        log("Routing table (next-hop | cost):")
        for d in sorted(self.next_hop.keys()):
            nh = self.next_hop[d]
            cost = self.dist.get(d, "?")
            log(f"  {self.id}->{d} : next-hop={nh} cost={cost}")

    def _print_route(self, dest:str):
        path = self.paths.get(dest, [])
        log(" -> ".join(path) if path else f"[no-path] {self.id}->{dest}")

    def _shutdown(self):
        log(f"[node] {self.id} shutting down...")
        self._hello_stop.set()
        try: self.transport.stop()
        except: pass

# ---------- main ----------
def main():
    ap = argparse.ArgumentParser(description="Dijkstra Node (Redis Pub/Sub)")
    ap.add_argument("--id", required=True)
    ap.add_argument("--topo", required=True, help="Topology JSON (type=topo)")
    ap.add_argument("--names", default=None, help="names-redis.json (opcional)")
    ap.add_argument("--metric", choices=["hop","rtt"], default="hop")
    ap.add_argument("--ttl", type=int, default=8)
    ap.add_argument("--hello", type=float, default=5.0)
    ap.add_argument("--redis-host", default="localhost")
    ap.add_argument("--redis-port", type=int, default=6379)
    ap.add_argument("--redis-db", type=int, default=0)
    ap.add_argument("--redis-pass", default=None)
    args = ap.parse_args()

    node = Node(args.id, args.topo, names_path=args.names,
                metric=args.metric, default_ttl=args.ttl, hello_interval=args.hello,
                redis_host=args.redis_host, redis_port=args.redis_port,
                redis_db=args.redis_db, redis_pass=args.redis_pass)
    node.start()

if __name__ == "__main__":
    main()
