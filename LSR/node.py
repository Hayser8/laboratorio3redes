import argparse, threading, time, json, os, sys
from typing import Optional, List, Dict, Any
from utils import log, now_iso, pretty
from protocols import (
    new_message, new_hello, sanitize_incoming,
    TYPE_LSP, TYPE_MESSAGE, TYPE_HELLO, PROTO_LSR
)
from transport_redis import RedisTransport
from config_loader import load_neighbors_only
from lsr import LSRRouter


def links_to_dict_for_print(links_any) -> Dict[str, float]:
    """ SOLO para imprimir topología (LSDB -> dict); el router NO usa esto. """
    out: Dict[str, float] = {}
    if not links_any:
        return out
    if isinstance(links_any, dict):
        for k, v in links_any.items():
            try:
                out[str(k)] = float(v)
            except Exception:
                out[str(k)] = 1.0
        return out
    if isinstance(links_any, list):
        for it in links_any:
            if isinstance(it, dict) and "to" in it:
                try:
                    out[str(it["to"])] = float(it.get("cost", 1))
                except Exception:
                    out[str(it.get("to") or "")] = 1.0
    return out


def headers_to_dict(h) -> Dict[str, Any]:
    """Convierte headers cualquiera (list/dict/None) a dict aceptable por lsr.py."""
    if isinstance(h, dict):
        # ya es dict; garantizamos que 'trail' sea lista si existe
        d = dict(h)
        if "trail" in d and not isinstance(d["trail"], list):
            try:
                d["trail"] = list(d["trail"])
            except Exception:
                d["trail"] = []
        return d
    if isinstance(h, list):
        return {"trail": list(h)}
    return {}  # fallback


class Node:
    def __init__(
        self, node_id: str, topo_path: str,
        *, hello_interval: float = 5.0, lsp_interval: float = 10.0,
        redis_host: str = "localhost", redis_port: int = 6379, redis_db: int = 0,
        redis_pass: Optional[str] = None, debug: bool = False
    ):
        self.id = node_id
        self.debug = debug
        self.neighbors: List[str] = load_neighbors_only(topo_path, self.id)

        self.transport = RedisTransport(
            node_id=self.id, on_packet=self._on_packet,
            host=redis_host, port=redis_port, db=redis_db, password=redis_pass
        )

        # Router LSR (usa tu lsr.py)
        self.router = LSRRouter(self, metric='hop', lsp_interval=lsp_interval, max_age=60.0)

        self.hello_interval = hello_interval
        self.lsp_interval = lsp_interval
        self.default_ttl = 8

        self._hello_stop = threading.Event()
        self._lsp_stop = threading.Event()

        # RTT simple por vecino (para --metric rtt si lo habilitas en LSRRouter)
        self._hello_sent_ts: Dict[str, float] = {}  # nb -> t_sent (monotonic)
        self._last_rtt_ms: Dict[str, float] = {}    # nb -> rtt en ms

    # -------- envío de bajo nivel (lo usa el router) --------
    def send_direct(self, neighbor_id: str, pkt: dict):
        if self.debug:
            log(f"[debug:{self.id}] send_direct -> {neighbor_id} type={pkt.get('type')} ttl={pkt.get('ttl')}")
        try:
            self.transport.publish_packet(neighbor_id, pkt)
        except Exception as e:
            log(f"[error] publish to {neighbor_id} failed: {e}")

    # -------- APIs que el router puede llamar --------
    def on_echo(self, msg: dict):
        """Llamado por LSRRouter cuando llega un ECHO de respuesta a un HELLO (mide RTT)."""
        sender = str(msg.get("from"))
        t0 = self._hello_sent_ts.pop(sender, None)
        if t0 is not None:
            rtt_ms = (time.monotonic() - t0) * 1000.0
            self._last_rtt_ms[sender] = rtt_ms
            if self.debug:
                log(f"[debug:{self.id}] RTT {sender} ≈ {rtt_ms:.1f} ms")

    def last_rtt(self, neighbor_id: str) -> Optional[float]:
        """ Devuelve el último RTT (ms) estimado al vecino. Usado si metric='rtt'. """
        return self._last_rtt_ms.get(neighbor_id)

    # -------- ciclo de vida --------
    def start(self):
        log(f"[node] {self.id} up | neighbors={self.neighbors} | metric={self.router.metric}")
        try:
            self.transport.start()
        except Exception as e:
            log(f"[fatal] transport start failed: {e}")
            raise

        # Hilos: HELLO y LSP del router
        threading.Thread(target=self._hello_loop, name=f"hello-{self.id}", daemon=True).start()
        threading.Thread(target=self._lsp_loop,   name=f"lsp-{self.id}",   daemon=True).start()

        self._console_loop()

    def _hello_loop(self):
        while not self._hello_stop.is_set():
            pkt = new_hello(self.id, proto=PROTO_LSR, ttl=2)
            # Enviar HELLO directo a cada vecino y registrar ts para RTT
            for nb in self.neighbors:
                self._hello_sent_ts[nb] = time.monotonic()
                self.send_direct(nb, pkt)
            self._hello_stop.wait(self.hello_interval)

    def _lsp_loop(self):
        # Origina un LSP pronto para acelerar convergencia
        time.sleep(0.5)
        self.router.originate_lsp()
        while not self._lsp_stop.is_set():
            self._lsp_stop.wait(self.lsp_interval)
            if self._lsp_stop.is_set():
                break
            self.router.originate_lsp()

    # -------- recepción desde Redis --------
    def _on_packet(self, pkt: dict, _src: str):
        try:
            pkt = sanitize_incoming(pkt)
        except Exception as e:
            if self.debug:
                log(f"[debug:{self.id}] sanitize_incoming failed: {e}")
            return

        # Asegura un 'id' para deduplicación del router (este usa msg["id"])
        if "id" not in pkt and "msg_id" in pkt:
            pkt["id"] = pkt["msg_id"]

        # Calcula last_hop del valor original y luego normaliza headers a dict
        h_orig = pkt.get("headers", [])
        if isinstance(h_orig, list) and h_orig:
            last_hop = h_orig[-1]
        elif isinstance(h_orig, dict):
            last_hop = h_orig.get("last_hop")
        else:
            last_hop = None
        pkt["headers"] = headers_to_dict(h_orig)

        if self.debug:
            log(f"[debug:{self.id}] on_packet type={pkt.get('type')} from={pkt.get('from')} ttl={pkt.get('ttl')} last_hop={last_hop}")

        # Pasa TODO al router tal cual (él sabe manejar LSP/MSG/HELLO/ECHO)
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

            elif cmd == "ttl" and len(parts) == 2:
                try:
                    self.default_ttl = int(parts[1]); log(f"[cfg] TTL={self.default_ttl}")
                except ValueError:
                    log("[warn] ttl must be integer")

            elif cmd == "lsp":
                self.router.originate_lsp()

            elif cmd == "help":
                log(help_text)

            elif cmd == "quit":
                break

            else:
                log("Unknown command. Type 'help'.")
        self._shutdown()

    # -------- helpers de consola --------
    def _send_data(self, dest: str, text: str):
        pkt = new_message(self.id, dest, text, proto=PROTO_LSR, ttl=self.default_ttl)
        # Igual que en recepción: asegura 'id' y headers en dict antes del router
        pkt["id"] = pkt.get("msg_id")
        pkt["headers"] = headers_to_dict(pkt.get("headers"))
        try:
            self.router.on_receive(pkt, None)
        except Exception as e:
            log(f"[error] router.on_receive(message) failed: {e}")

    def _print_table(self):
        table = getattr(self.router, "next_hop", {})
        if not table:
            log("[warn] routing table empty (aún no hay LSPs válidos)")
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


def main():
    ap = argparse.ArgumentParser(description="LSR Node (Redis Pub/Sub) compatible con lsr.py")
    ap.add_argument("--id", required=True)
    ap.add_argument("--topo", required=True)
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
