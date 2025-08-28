import argparse, threading, time, json, os
from typing import Optional, List, Dict, Any, Tuple
from utils import log, now_iso, pretty
from protocols import (
    new_hello, new_message, sanitize_incoming, forward_transform, ExpiringSet,
    TYPE_HELLO, TYPE_LSP, TYPE_MESSAGE, TYPE_INFO,
    PROTO_FLOODING
)
from transport_redis import RedisTransport
from config_loader import load_neighbors_only


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
    """Acepta variantes de headers (list/dict) y devuelve una lista."""
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


class Node:
    """
    Flooding puro:
      - Reenvía TODO (HELLO/LSP/INFO/MESSAGE) con TTL>0, excepto al prev_hop.
      - Suprime duplicados por msg_id.
      - Evita ciclos si nuestro id ya está en headers.
    """
    def __init__(self, node_id:str, topo_path:str,
                 names_path: Optional[str] = None,
                 default_ttl:int=8, hello_interval:float=5.0,
                 redis_host:str="localhost", redis_port:int=6379, redis_db:int=0,
                 redis_pass: Optional[str] = None):
        self.id = node_id
        self.neighbors: List[str] = load_neighbors_only(topo_path, self.id)

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

        # Counters
        self.stats = {
            "rx": 0,          # paquetes recibidos
            "tx": 0,          # publicaciones (mensajes emitidos a vecinos)
            "fwd": 0,         # reenvíos (número de copias reenviadas)
            "drop_dup": 0,    # descartes por duplicado
            "drop_ttl": 0,    # descartes por TTL (o TTL->0 al reenviar)
            "drop_cycle": 0,  # descartes por detectar nuestro id en headers
            "drop_bad": 0,    # descartes por paquete inválido/sanitize
        }

    # -------- envío ----------
    def send_direct(self, neighbor_id:str, pkt:dict):
        self.transport.publish_packet(neighbor_id, pkt)
        self.stats["tx"] += 1

    def _broadcast_counting(self, pkt:dict, exclude:Optional[str]=None):
        """Broadcast con conteo de tx/fwd."""
        nbs = [nb for nb in self.neighbors if not (exclude and nb == exclude)]
        for nb in nbs:
            self.transport.publish_packet(nb, pkt)
        self.stats["tx"] += len(nbs)
        self.stats["fwd"] += len(nbs)

    def broadcast(self, pkt:dict, exclude:Optional[str]=None):
        # por compat, pero usamos la versión contada
        self._broadcast_counting(pkt, exclude=exclude)

    # -------- ciclo de vida ----------
    def start(self):
        log(f"[node] {self.id} neighbors={self.neighbors} (flooding)")
        self.transport.start()
        time.sleep(0.5)
        threading.Thread(target=self._hello_loop, name=f"hello-{self.id}", daemon=True).start()
        self._console_loop()

    def _hello_loop(self):
        while not self._hello_stop.is_set():
            pkt = new_hello(self.id, proto=PROTO_FLOODING, ttl=2)
            # hello es broadcast: cuenta tx/fwd
            self._broadcast_counting(pkt)
            self._hello_stop.wait(self.hello_interval)

    # -------- recepción ----------
    def _on_packet(self, pkt:dict, _src:str):
        # Compat: si headers viene como dict, conviértelo a lista antes de sanitize
        if isinstance(pkt, dict) and not isinstance(pkt.get("headers"), list):
            pkt = dict(pkt)
            pkt["headers"] = _coerce_headers_list(pkt.get("headers"))

        try:
            pkt = sanitize_incoming(pkt)
        except Exception as e:
            self.stats["drop_bad"] += 1
            log(f"[drop] sanitize failed: {e}; raw={pretty(pkt)}")
            return

        self.stats["rx"] += 1

        # Duplicados
        mid = pkt.get("msg_id")
        if mid and not self.seen.add_if_new(mid):
            self.stats["drop_dup"] += 1
            return

        ptype = pkt.get("type")
        dest  = pkt.get("to")
        headers_list = pkt.get("headers", [])
        prev_hop = headers_list[-1] if headers_list else None

        # Entrega de datos si es para mí
        if ptype == TYPE_MESSAGE and dest == self.id:
            log(f"[deliver] {self.id} <- {pkt.get('from')}: {pkt.get('payload')}")
            return

        # Evitar ciclo/ttl antes de forward_transform (para contar razones)
        if self.id in set(headers_list or []):
            self.stats["drop_cycle"] += 1
            return
        if int(pkt.get("ttl", 0)) <= 1:
            self.stats["drop_ttl"] += 1
            return

        # Forward (rota headers y ttl--)
        fwd = forward_transform(pkt, self.id)
        if fwd is None:
            # si llega aquí normalmente ya lo habríamos contado
            return

        # Reenvía a todos excepto prev_hop
        self._broadcast_counting(fwd, exclude=prev_hop)

    # -------- consola ----------
    def _console_loop(self):
        help_text = (
            "Commands:\n"
            "  send <DEST> <TEXT>   - flood del mensaje; se entrega si logra llegar a DEST\n"
            "  neighbors            - muestra tus vecinos directos (desde topo)\n"
            "  stats                - counters: rx/tx/fwd/drop_dup/drop_ttl/drop_cycle/drop_bad\n"
            "  table                - Flooding no usa tabla de ruteo; muestra vecinos\n"
            "  ttl <N>              - cambia el TTL por defecto para 'send'\n"
            "  help                 - muestra este menú\n"
            "  quit                 - salir\n"
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

            elif cmd == "neighbors":
                log(f"Neighbors: {self.neighbors}")

            elif cmd == "stats":
                s = self.stats
                log("Stats:")
                log(f"  rx={s['rx']}  tx={s['tx']}  fwd={s['fwd']}")
                log(f"  drop_dup={s['drop_dup']}  drop_ttl={s['drop_ttl']}  drop_cycle={s['drop_cycle']}  drop_bad={s['drop_bad']}")
                log(f"  default_ttl={self.default_ttl}")

            elif cmd == "table":
                log("Flooding no mantiene tabla de ruteo. Vecinos directos:")
                for nb in self.neighbors:
                    log(f"  - {nb}")

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
        pkt = new_message(self.id, dest, text, proto=PROTO_FLOODING, ttl=self.default_ttl)
        # Flood inicial hacia TODOS los vecinos
        self._broadcast_counting(pkt)

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
    ap.add_argument("--topo", required=True, help="Neighbors-only JSON (type=topo)")
    ap.add_argument("--names", default=None, help="names-redis.json (opcional)")
    ap.add_argument("--ttl", type=int, default=8)
    ap.add_argument("--hello", type=float, default=5.0)
    ap.add_argument("--redis-host", default="localhost")
    ap.add_argument("--redis-port", type=int, default=6379)
    ap.add_argument("--redis-db", type=int, default=0)
    ap.add_argument("--redis-pass", default=None)
    args = ap.parse_args()

    node = Node(args.id, args.topo, names_path=args.names,
                default_ttl=args.ttl, hello_interval=args.hello,
                redis_host=args.redis_host, redis_port=args.redis_port,
                redis_db=args.redis_db, redis_pass=args.redis_pass)
    node.start()


if __name__ == "__main__":
    main()
