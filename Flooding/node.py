import argparse, threading, time
from typing import Optional, List
from utils import log, now_iso
from protocols import (
    new_hello, new_message,
    sanitize_incoming, forward_transform, ExpiringSet,
    TYPE_HELLO, TYPE_MESSAGE,
    PROTO_FLOODING,
)
from transport_redis import RedisTransport
from config_loader import load_topology, load_names


class Node:
    def __init__(self, node_id:str, topo_path:str, names_path:str,
                 default_ttl:int=6, hello_interval:float=10.0,
                 redis_host:str="localhost", redis_port:int=6379, redis_db:int=0):
        self.id = node_id
        self.topo = load_topology(topo_path)                # {A:[B,C], ...} o {A:{B:1,...}}
        neighs = self.topo.get(self.id, {})
        self.neighbors: List[str] = list(neighs.keys() if isinstance(neighs, dict) else neighs)
        self.addr_map = load_names(names_path)              # no se usa con Redis, lo dejamos por compat.

        self.transport = RedisTransport(
            node_id=self.id, on_packet=self._on_packet,
            host=redis_host, port=redis_port, db=redis_db
        )

        self.default_ttl = default_ttl
        self.hello_interval = hello_interval
        self._hello_stop = threading.Event()

        self.seen = ExpiringSet(ttl_seconds=60)

    # --- envío ---
    def send_direct(self, neighbor_id:str, pkt:dict):
        self.transport.publish_packet(neighbor_id, pkt)

    def broadcast(self, pkt:dict, exclude:Optional[str]=None):
        self.transport.broadcast(self.neighbors, pkt, exclude=exclude)

    # --- ciclo de vida ---
    def start(self):
        log(f"[node] {self.id} neighbors={self.neighbors}")
        self.transport.start()
        threading.Thread(target=self._hello_loop, name=f"hello-{self.id}", daemon=True).start()
        self._console_loop()

    def _hello_loop(self):
        time.sleep(1.0)
        while not self._hello_stop.is_set():
            pkt = new_hello(self.id, proto=PROTO_FLOODING, ttl=2)
            self.broadcast(pkt)
            self._hello_stop.wait(self.hello_interval)

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

        if ptype == TYPE_MESSAGE:
            if dest == self.id:
                log(f"[deliver] {self.id} <- {pkt.get('from')}: {pkt.get('payload')}")
                return
            # Flood: reenviar a TODOS excepto de donde vino
            fwd = forward_transform(pkt, self.id)
            if fwd is None:
                return
            self.broadcast(fwd, exclude=prev_hop)
            return

        # otros tipos: ignorar en flooding puro

    # --- consola ---
    def _console_loop(self):
        help_text = (
            "Commands:\n"
            "  send <DEST> <TEXT>   - flooding DATA\n"
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
                pkt = new_message(self.id, dest, text, proto=PROTO_FLOODING, ttl=self.default_ttl)
                self.broadcast(pkt)
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

    def _shutdown(self):
        log(f"[node] {self.id} shutting down...")
        self._hello_stop.set()
        try: self.transport.stop()
        except: pass


def main():
    ap = argparse.ArgumentParser(description="Flooding Node (Redis Pub/Sub)")
    ap.add_argument("--id", required=True)
    ap.add_argument("--topo", required=True)
    ap.add_argument("--names", required=True)
    ap.add_argument("--ttl", type=int, default=6)
    ap.add_argument("--hello", type=float, default=10.0)
    ap.add_argument("--redis-host", default="localhost")
    ap.add_argument("--redis-port", type=int, default=6379)
    ap.add_argument("--redis-db", type=int, default=0)
    args = ap.parse_args()

    node = Node(args.id, args.topo, args.names, args.ttl, args.hello,
                redis_host=args.redis_host, redis_port=args.redis_port, redis_db=args.redis_db)
    node.start()

if __name__ == "__main__":
    main()
