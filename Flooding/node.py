# Flooding/node.py
import argparse, threading, time, json
from typing import Optional, List, Dict
from utils import log
from protocols import (
    new_hello, new_message,
    sanitize_incoming, forward_transform, ExpiringSet,
    TYPE_HELLO, TYPE_MESSAGE, PROTO_FLOODING,
)
from transport_redis import RedisTransport


# ----------------------------
# Helpers (no depende de config_loader)
# ----------------------------
def load_neighbors_only(topo_path: str, my_id: str) -> List[str]:
    """Lee topo-*.json y devuelve SOLO la lista de vecinos de my_id."""
    with open(topo_path, "r", encoding="utf-8") as f:
        obj = json.load(f)
    assert obj.get("type") == "topo", "topology file must have type=topo"
    cfg = obj["config"]
    neighs = cfg.get(my_id, [])
    return list(neighs.keys() if isinstance(neighs, dict) else neighs)


def load_names_meta(names_path: str) -> Dict:
    """
    Lee names-*.json y retorna metadatos de Redis + channel_map:
    {
      "type": "names",
      "host": "...", "port": 6379, "pwd": ".....",
      "config": { "A": {"channel": "..."}, ... }
    }
    """
    with open(names_path, "r", encoding="utf-8") as f:
        obj = json.load(f)
    assert obj.get("type") == "names", "names file must have type=names"
    meta = {}
    if "host" in obj: meta["host"] = obj["host"]
    if "port" in obj: meta["port"] = obj["port"]
    if "pwd"  in obj: meta["pwd"]  = obj["pwd"]
    # channel_map id->canal (si existe)
    chmap: Dict[str, str] = {}
    cfg = obj.get("config", {})
    for nid, entry in cfg.items():
        if isinstance(entry, dict) and "channel" in entry:
            chmap[str(nid)] = str(entry["channel"])
    meta["channels"] = chmap
    return meta


class Node:
    def __init__(self, node_id:str, topo_path:str, names_path:str,
                 default_ttl:int=6, hello_interval:float=10.0,
                 redis_host:str="localhost", redis_port:int=6379, redis_db:int=0):
        self.id = node_id
        self.neighbors: List[str] = load_neighbors_only(topo_path, self.id)

        # Metadatos de Redis (si existen en names)
        self.redis_meta = load_names_meta(names_path)

        # Resolver conexión a Redis (names tiene prioridad, CLI como fallback)
        host = self.redis_meta.get("host", redis_host)
        port = int(self.redis_meta.get("port", redis_port))
        password = self.redis_meta.get("pwd", None)
        chmap = self.redis_meta.get("channels", {})  # id -> canal exacto

        # Crear transporte Redis (con channel_map + logs)
        self.transport = RedisTransport(
            node_id=self.id,
            on_packet=self._on_packet,
            host=host,
            port=port,
            db=redis_db,
            password=password,
            channel_map=chmap,
            on_log=log,
            on_error=self._on_transport_error,
        )

        self.default_ttl = default_ttl
        self.hello_interval = hello_interval
        self._hello_stop = threading.Event()

        # Evita duplicados/tormentas
        self.seen = ExpiringSet(ttl_seconds=60)

    # --- envío ---
    def send_direct(self, neighbor_id:str, pkt:dict):
        self.transport.publish_packet(neighbor_id, pkt)

    def broadcast(self, pkt:dict, exclude:Optional[str]=None):
        self.transport.broadcast(self.neighbors, pkt, exclude=exclude)

    # --- ciclo de vida ---
    def start(self):
        log(f"[node] {self.id} neighbors={self.neighbors} (flooding)")
        self.transport.start()
        # Hint útil de depuración cuando todos comparten canal:
        if self.redis_meta.get("channels"):
            sample = list(self.redis_meta["channels"].items())[:5]
            log(f"[node] channel_map sample: {sample}")
        threading.Thread(target=self._hello_loop, name=f"hello-{self.id}", daemon=True).start()
        self._console_loop()

    def _hello_loop(self):
        time.sleep(1.0)  # pequeño delay para que todos subscriban
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
            # duplicado: no volvemos a imprimir ni reenviar
            return

        ptype   = pkt.get("type")
        dest    = pkt.get("to")
        src     = pkt.get("from")
        payload = pkt.get("payload")
        ttl     = pkt.get("ttl")
        headers = pkt.get("headers", [])
        prev_hop = headers[-1] if headers else None

        if ptype == TYPE_HELLO:
            return

        if ptype == TYPE_MESSAGE:
            # --- Requisito: que se vea en TODOS los nodos por donde pasa ---
            is_broadcast = (dest in ("*", None))
            log(f"[tap] {self.id} sees {src} -> {dest}: {payload} (ttl={ttl})")

            # Entregar al usuario si soy el destino o si es broadcast
            if dest == self.id or is_broadcast:
                log(f"[deliver] {self.id} <- {src}: {payload}")
                # En unicast, si ya entregué (soy destino), no reenvío
                if not is_broadcast and dest == self.id:
                    return

            # Reenviar por flooding a todos los vecinos excepto de donde vino
            fwd = forward_transform(pkt, self.id)
            if fwd is None:
                return
            self.broadcast(fwd, exclude=prev_hop)
            return

        # otros tipos: ignorar en flooding puro

    def _on_transport_error(self, exc: Exception, raw: Optional[str]):
        try:
            log(f"[transport-error] {exc} raw={raw!r}")
        except Exception:
            pass

    # --- consola ---
    def _console_loop(self):
        help_text = (
            "Commands:\n"
            "  send <DEST|*> <TEXT> - flooding DATA (unicast o broadcast con '*')\n"
            "  ttl <N>              - set default TTL\n"
            "  neighbors            - show my neighbors\n"
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
                dest = parts[1]
                text = " ".join(parts[2:])
                # Soporte broadcast de aplicación con '*' o 'ALL'
                if dest == "*" or dest.lower() == "all":
                    dest = "*"
                pkt = new_message(self.id, dest, text, proto=PROTO_FLOODING, ttl=self.default_ttl)
                self.broadcast(pkt)
            elif cmd == "ttl" and len(parts) == 2:
                try:
                    self.default_ttl = int(parts[1]); log(f"default TTL set to {self.default_ttl}")
                except ValueError:
                    log("ttl must be an integer")
            elif cmd == "neighbors":
                log(f"neighbors={self.neighbors}")
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
        try:
            self.transport.stop()
        except:
            pass


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
    # Nota: host/port/password se leen de names-redis.json; los CLI sirven como fallback.
    args = ap.parse_args()

    node = Node(args.id, args.topo, args.names, args.ttl, args.hello,
                redis_host=args.redis_host, redis_port=args.redis_port, redis_db=args.redis_db)
    node.start()

if __name__ == "__main__":
    main()
