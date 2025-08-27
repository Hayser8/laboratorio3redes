import argparse, threading, time
from typing import Optional, List, Dict, Any
from utils import log, now_iso, pretty
from protocols import (
    new_hello, new_lsp, new_message,
    sanitize_incoming, forward_transform, ExpiringSet,
    TYPE_HELLO, TYPE_LSP, TYPE_MESSAGE,
    PROTO_LSR,
)
from transport_redis import RedisTransport
from config_loader import load_neighbors_only, load_names
from lsr import LSRRouter


class Node:
    def __init__(self, node_id:str, topo_path:str, names_path:str,
                 metric:str='hop', default_ttl:int=8,
                 hello_interval:float=5.0, lsp_interval:float=10.0, lsp_max_age:float=60.0,
                 redis_host:str="localhost", redis_port:int=6379, redis_db:int=0):
        self.id = node_id
        self.addr_map = load_names(names_path)               # (no usado con Redis)
        self.neighbors: List[str] = load_neighbors_only(topo_path, self.id)

        self.transport = RedisTransport(
            node_id=self.id, on_packet=self._on_packet,
            host=redis_host, port=redis_port, db=redis_db
        )

        # Router LSR (mantiene LSDB y SPF). Se asume next_hop/dist/paths públicos.
        self.router = LSRRouter(self, metric=metric, lsp_interval=lsp_interval, max_age=lsp_max_age)

        self.default_ttl = default_ttl
        self.hello_interval = hello_interval
        self.lsp_interval = lsp_interval

        self._hello_stop = threading.Event()
        self._lsp_stop = threading.Event()

        self.seen = ExpiringSet(ttl_seconds=60)

    # --- envío ---
    def send_direct(self, neighbor_id:str, pkt:dict):
        self.transport.publish_packet(neighbor_id, pkt)

    def broadcast(self, pkt:dict, exclude:Optional[str]=None):
        self.transport.broadcast(self.neighbors, pkt, exclude=exclude)

    # --- ciclo de vida ---
    def start(self):
        log(f"[node] {self.id} neighbors={self.neighbors} metric={self.router.metric}")
        self.transport.start()
        time.sleep(1.0)
        threading.Thread(target=self._hello_loop, name=f"hello-{self.id}", daemon=True).start()
        threading.Thread(target=self._lsp_loop, name=f"lsp-{self.id}", daemon=True).start()
        self._console_loop()

    def _hello_loop(self):
        while not self._hello_stop.is_set():
            pkt = new_hello(self.id, proto=PROTO_LSR, ttl=2)
            self.broadcast(pkt)
            self._hello_stop.wait(self.hello_interval)

    def _lsp_loop(self):
        time.sleep(1.0)
        self._originate_lsp()
        while not self._lsp_stop.is_set():
            self._lsp_stop.wait(self.lsp_interval)
            if self._lsp_stop.is_set(): break
            self._originate_lsp()

    # Construye un LSP simple (id + enlaces a vecinos con costo)
    def _build_lsp_payload(self) -> Dict[str, Any]:
        links = []
        for nb in self.neighbors:
            cost = 1 if self.router.metric == "hop" else (self.router.rtt(nb) or 1)
            links.append({"to": nb, "cost": cost})
        return {"origin": self.id, "links": links, "ts": now_iso()}

    def _originate_lsp(self):
        lsp_payload = self._build_lsp_payload()
        pkt = new_lsp(self.id, lsp_payload, ttl=5)
        # Se reinyecta localmente para que el router lo integre a su LSDB
        self._consume_control(pkt, incoming_last_hop=self.id)
        # Y se difunde
        self.broadcast(pkt)

    # --- recepción ---
    def _on_packet(self, pkt:dict, _src:str):
        try:
            pkt = sanitize_incoming(pkt)
        except Exception:
            return

        mid = pkt.get("msg_id")
        if mid and not self.seen.add_if_new(mid):
            return

        ptype = pkt.get("type")
        dest  = pkt.get("to")
        headers = pkt.get("headers", [])
        prev_hop = headers[-1] if headers else None

        if ptype == TYPE_HELLO:
            return

        if ptype == TYPE_LSP:
            self._consume_control(pkt, incoming_last_hop=prev_hop)
            fwd = forward_transform(pkt, self.id)
            if fwd is not None:
                self.broadcast(fwd, exclude=prev_hop)
            return

        if ptype == TYPE_MESSAGE:
            if dest == self.id:
                log(f"[deliver] {self.id} <- {pkt.get('from')}: {pkt.get('payload')}")
                return
            nh = self.router.next_hop.get(dest)
            if not nh:
                log(f"[drop] no route {self.id}->{dest}")
                return
            fwd = forward_transform(pkt, self.id)
            if fwd is None:
                return
            self.send_direct(nh, fwd)
            return

    # Integra LSP/INFO en la LSDB y recalcula rutas (delegado al router)
    def _consume_control(self, pkt:dict, incoming_last_hop:Optional[str]):
        try:
            # Mantengo firma compatible con tu LSRRouter
            self.router.on_receive(pkt, incoming_last_hop)
        except Exception as e:
            log(f"[warn] router.on_receive error: {e}")

    # --- consola ---
    def _console_loop(self):
        help_text = (
            "Commands:\n"
            "  send <DEST> <TEXT>   - send DATA via LSR (next-hop)\n"
            "  table                - print routing table (next-hop, cost)\n"
            "  route <DEST>         - show SPF path\n"
            "  lsdb                 - print LSDB\n"
            "  ttl <N>              - set default TTL\n"
            "  lsp                  - originate LSP now\n"
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
                    log(pretty(self.router.lsdb.as_dict()))
                except Exception:
                    log("[warn] LSDB not available")
            elif cmd == "ttl" and len(parts) == 2:
                try:
                    self.default_ttl = int(parts[1]); log(f"default TTL set to {self.default_ttl}")
                except ValueError:
                    log("ttl must be an integer")
            elif cmd == "lsp":
                self._originate_lsp()
            elif cmd == "help":
                log(help_text)
            elif cmd == "quit":
                break
            else:
                log("Unknown command. Type 'help'.")
        self._shutdown()

    def _send_data(self, dest:str, text:str):
        if dest == self.id:
            log("[info] destination is self; printing locally: " + text)
            return
        nh = self.router.next_hop.get(dest)
        if not nh:
            log(f"[drop] no route {self.id}->{dest}")
            return
        pkt = new_message(self.id, dest, text, proto=PROTO_LSR, ttl=self.default_ttl)
        self.send_direct(nh, pkt)

    def _print_table(self):
        log("Routing table (next-hop | cost):")
        try:
            for d, nh in sorted(self.router.next_hop.items()):
                cost = self.router.dist.get(d, "?")
                log(f"  {self.id}->{d} : next-hop={nh} cost={cost}")
        except Exception:
            log("[warn] routing table not available yet")

    def _print_route(self, dest:str):
        try:
            path = self.router.paths.get(dest, [])
            log(" -> ".join(path) if path else f"[no-path] {self.id}->{dest}")
        except Exception:
            log("[warn] no path data yet")

    def _shutdown(self):
        log(f"[node] {self.id} shutting down...")
        self._hello_stop.set(); self._lsp_stop.set()
        try: self.transport.stop()
        except: pass


def main():
    ap = argparse.ArgumentParser(description="LSR Node (Redis Pub/Sub)")
    ap.add_argument("--id", required=True)
    ap.add_argument("--topo", required=True, help="Neighbors-only JSON")
    ap.add_argument("--names", required=True)
    ap.add_argument("--metric", choices=["hop","rtt"], default="hop")
    ap.add_argument("--ttl", type=int, default=8)
    ap.add_argument("--hello", type=float, default=5.0)
    ap.add_argument("--lsp", type=float, default=10.0)
    ap.add_argument("--maxage", type=float, default=60.0)
    ap.add_argument("--redis-host", default="localhost")
    ap.add_argument("--redis-port", type=int, default=6379)
    ap.add_argument("--redis-db", type=int, default=0)
    args = ap.parse_args()

    node = Node(args.id, args.topo, args.names, metric=args.metric,
                default_ttl=args.ttl, hello_interval=args.hello,
                lsp_interval=args.lsp, lsp_max_age=args.maxage,
                redis_host=args.redis_host, redis_port=args.redis_port, redis_db=args.redis_db)
    node.start()

if __name__ == "__main__":
    main()
