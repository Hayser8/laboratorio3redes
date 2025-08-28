import argparse, threading, time, json, os
from typing import Optional, List, Dict, Any, Tuple
from utils import log, now_iso, pretty
from protocols import (
    new_hello, new_lsp, new_message,
    sanitize_incoming, forward_transform, ExpiringSet,
    TYPE_HELLO, TYPE_LSP, TYPE_MESSAGE, TYPE_INFO,
    PROTO_LSR,
)
from transport_redis import RedisTransport
from config_loader import load_neighbors_only
from lsr import LSRRouter


def load_names_redis(names_path: Optional[str]) -> Tuple[Optional[str], Optional[int], Optional[str], Dict[str,str]]:
    """
    Lee names-redis.json para usar host/port/pwd y mapa de canales:
    {
      "host": "...", "port": 6379, "pwd": "pass", "type":"names",
      "config": { "A": {"channel":"net:inbox:A"}, "B": {...} }
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


class Node:
    def __init__(self, node_id:str, topo_path:str,
                 names_path: Optional[str] = None,
                 metric:str='hop', default_ttl:int=8,
                 hello_interval:float=5.0, lsp_interval:float=10.0, lsp_max_age:float=60.0,
                 redis_host:str="localhost", redis_port:int=6379, redis_db:int=0,
                 redis_pass: Optional[str] = None):
        self.id = node_id
        self.neighbors: List[str] = load_neighbors_only(topo_path, self.id)

        # names-redis.json (si existe) tiene prioridad sobre CLI
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

        self.router = LSRRouter(self, metric=metric, lsp_interval=lsp_interval, max_age=lsp_max_age)

        self.default_ttl = default_ttl
        self.hello_interval = hello_interval
        self.lsp_interval = lsp_interval

        self._hello_stop = threading.Event()
        self._lsp_stop = threading.Event()
        self.seen = ExpiringSet(ttl_seconds=60)

    # ---------- helpers de normalización ----------
    @staticmethod
    def _normalize_links_to_pairs(links_in) -> List[List[Any]]:
        """
        Devuelve SIEMPRE [[neighbor, cost], ...]
        Acepta:
        - dict: {"B":1,"C":2}
        - lista de dicts: [{"to":"B","cost":1}, ...]
        - lista de pares: [["B",1], ["C",2]]
        - lista de strings: ["B","C"]  -> [["B",1],["C",1]]
        - None/otros -> []
        """
        pairs: List[List[Any]] = []
        if links_in is None:
            return pairs

        if isinstance(links_in, dict):
            for k, v in links_in.items():
                try:
                    pairs.append([str(k), int(v)])
                except Exception:
                    pairs.append([str(k), 1])
            return pairs

        if isinstance(links_in, list):
            if len(links_in) == 0:
                return pairs

            if isinstance(links_in[0], dict):
                for d in links_in:
                    if "to" in d:
                        try:
                            pairs.append([str(d["to"]), int(d.get("cost", 1))])
                        except Exception:
                            pairs.append([str(d.get("to")), 1])
                return pairs

            for item in links_in:
                if isinstance(item, (list, tuple)) and len(item) >= 2:
                    try:
                        nb, cost = item[0], int(item[1])
                    except Exception:
                        nb, cost = item[0], 1
                    pairs.append([str(nb), cost])
                elif isinstance(item, str):
                    pairs.append([item, 1])
            return pairs

        return pairs

    @staticmethod
    def _headers_to_dict(headers_list: List[str], prev_hop: Optional[str]) -> Dict[str, Any]:
        return {"trail": list(headers_list or []), "last_hop": prev_hop}

    # ---------- envío ----------
    def send_direct(self, neighbor_id:str, pkt:dict):
        self.transport.publish_packet(neighbor_id, pkt)

    def broadcast(self, pkt:dict, exclude:Optional[str]=None):
        self.transport.broadcast(self.neighbors, pkt, exclude=exclude)

    # ---------- ciclo de vida ----------
    def start(self):
        log(f"[node] {self.id} neighbors={self.neighbors} metric={self.router.metric}")
        self.transport.start()
        self._seed_direct_routes()  # rutas base a vecinos
        time.sleep(1.0)
        threading.Thread(target=self._hello_loop, name=f"hello-{self.id}", daemon=True).start()
        threading.Thread(target=self._lsp_loop, name=f"lsp-{self.id}", daemon=True).start()
        self._console_loop()

    def _seed_direct_routes(self):
        # Si el router aún no conoce a los vecinos, crea rutas directas (next-hop el propio vecino)
        try:
            self.router.next_hop  # solo para verificar existe
        except Exception:
            # si tu router no tiene estos atributos, crea placeholders
            self.router.next_hop = {}
            self.router.dist = {}
            self.router.paths = {}
        for nb in self.neighbors:
            self.router.next_hop.setdefault(nb, nb)
            self.router.dist.setdefault(nb, 1)
            self.router.paths.setdefault(nb, [self.id, nb])
        log(f"[seed] direct routes installed: { {nb: nb for nb in self.neighbors} }")

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

    # ---------- construir LSP propio ----------
    def _build_lsp_payload(self) -> Dict[str, Any]:
        # Enviamos como dict { vecino: costo }, lo normalizamos antes de entrar al router
        links_dict: Dict[str, int] = {}
        for nb in self.neighbors:
            cost = 1 if self.router.metric == "hop" else (getattr(self.router, "rtt", lambda _ : 1)(nb) or 1)
            try:
                cost = int(cost)
            except Exception:
                cost = 1
            links_dict[nb] = cost
        return {"id": self.id, "links": links_dict, "ts": now_iso()}

    def _originate_lsp(self):
        lsp_payload = self._build_lsp_payload()
        pkt = new_lsp(self.id, lsp_payload, ttl=5)
        self._consume_control(pkt, incoming_last_hop=self.id)  # integra local
        self.broadcast(pkt)  # difunde

    # ---------- recepción ----------
    def _on_packet(self, pkt:dict, _src:str):
        # Normaliza paquetitos “raros” del otro programa
        try:
            pkt = sanitize_incoming(pkt)
        except Exception as e:
            log(f"[drop] sanitize failed: {e}; raw={pretty(pkt)}")
            return

        mid = pkt.get("msg_id")
        if mid and not self.seen.add_if_new(mid):
            return

        ptype = pkt.get("type")
        dest  = pkt.get("to")
        headers_list = pkt.get("headers", [])
        prev_hop = headers_list[-1] if headers_list else None

        if ptype == TYPE_HELLO:
            # asegura ruta directa (por si no estaba)
            frm = pkt.get("from")
            if frm in self.neighbors:
                if getattr(self.router, "next_hop", None) is not None:
                    self.router.next_hop.setdefault(frm, frm)
                    self.router.dist.setdefault(frm, 1)
            return

        if ptype == TYPE_INFO:
            # Interoperabilidad: INFO del otro programa (DV/LSR) -> convertir a LSP canónico
            self._handle_info_as_lsp(pkt, prev_hop)
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
            nh = getattr(self.router, "next_hop", {}).get(dest)
            if not nh:
                log(f"[drop] no route {self.id}->{dest}")
                return
            fwd = forward_transform(pkt, self.id)
            if fwd is None:
                return
            self.send_direct(nh, fwd)
            return

    def _handle_info_as_lsp(self, pkt: dict, prev_hop: Optional[str]):
        """
        Acepta INFO del router externo:
        payload puede venir como str JSON o dict con campos:
          {"origin":"B","neighbors":{"A":1,...},"seq":N,"ts":...}
        Lo convertimos a un LSP: {"id":"B","links":[["A",1],...]}
        y lo pasamos por el mismo flujo que un LSP normal.
        """
        pl = pkt.get("payload")
        info: Dict[str, Any] = {}
        if isinstance(pl, str):
            try:
                info = json.loads(pl)
            except Exception:
                return
        elif isinstance(pl, dict):
            info = pl
        else:
            return

        origin = info.get("origin") or pkt.get("from")
        links_in = info.get("neighbors") or info.get("links") or {}
        links_pairs = self._normalize_links_to_pairs(links_in)

        fake_lsp = {
            "proto": "lsr",
            "type": TYPE_LSP,
            "from": origin,
            "to": "broadcast",
            "ttl": int(pkt.get("ttl", 5)),
            "headers": pkt.get("headers", []),  # lista; _consume_control la convertirá a dict para el router
            "payload": {"id": origin, "links": links_pairs, "ts": info.get("ts", now_iso())},
            "msg_id": pkt.get("msg_id"),
        }
        self._consume_control(fake_lsp, incoming_last_hop=prev_hop)

    def _consume_control(self, pkt:dict, incoming_last_hop:Optional[str]):
        """
        Compatibilidad con tu LSRRouter:
        - Asegura top-level 'id'
        - Convierte 'links' a lista de pares [[vecino, costo], ...]
        - Convierte headers list -> dict {'trail': [...], 'last_hop': ...}
        """
        try:
            if pkt.get("type") == TYPE_LSP:
                pl = pkt.get("payload") or {}
                legacy = dict(pkt)  # copia superficial

                # ID toplevel
                legacy["id"] = legacy.get("id") or pl.get("id") or pkt.get("from") or self.id

                # Fuente de links (payload > toplevel) y normalización a lista de pares
                links_in = pl.get("links", pkt.get("links"))
                links_pairs = self._normalize_links_to_pairs(links_in)

                # headers: tu router espera dict
                headers_dict = self._headers_to_dict(pkt.get("headers", []), incoming_last_hop)
                # (solo para debug limpio)
                sample = dict(list(headers_dict.items())[:2])
                log(f"[debug:before-router] pkt.type={pkt.get('type')} from={pkt.get('from')} to={pkt.get('to')} ttl={pkt.get('ttl')}")
                log(f"[debug:before-router] payload.keys={list(pl.keys())}")
                log(f"[debug:before-router] raw_links_type={'list' if isinstance(links_in, list) else 'dict' if isinstance(links_in, dict) else type(links_in).__name__} "
                    f"sample={links_in if isinstance(links_in, dict) else (links_in[:1] if isinstance(links_in, list) else links_in)}")
                log(f"[debug:before-router] legacy.id={legacy['id']} links_dict.size={len(dict(links_pairs))} sample={list(dict(links_pairs).items())[:1]}")
                log(f"[debug:before-router] headers_dict.keys={list(headers_dict.keys())} sample={sample}")

                # Espeja formato canónico que tu LSRRouter sabe digerir
                legacy["headers"] = headers_dict
                legacy["links"] = links_pairs
                legacy["payload"] = {"id": legacy["id"], "links": links_pairs, "ts": (pl.get("ts") or now_iso())}

                # Al router
                self.router.on_receive(legacy, incoming_last_hop)
            else:
                self.router.on_receive(pkt, incoming_last_hop)

        except Exception as e:
            log(f"[ERROR] router.on_receive raised: {e}\n[ERROR] offending packet = {pretty(pkt)}")

    # ---------- consola ----------
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

        nh = getattr(self.router, "next_hop", {}).get(dest)

        # --- FAIL-SAFE: si no hay ruta pero es vecino directo, usa el vecino ---
        if not nh and dest in self.neighbors:
            nh = dest
            log(f"[fallback] using direct neighbor {dest} as next-hop")

        if not nh:
            log(f"[drop] no route {self.id}->{dest}")
            return

        pkt = new_message(self.id, dest, text, proto=PROTO_LSR, ttl=self.default_ttl)
        self.send_direct(nh, pkt)


    def _print_table(self):
        log("Routing table (next-hop | cost):")
        try:
            for d, nh in sorted(getattr(self.router, "next_hop", {}).items()):
                cost = getattr(self.router, "dist", {}).get(d, "?")
                log(f"  {self.id}->{d} : next-hop={nh} cost={cost}")
        except Exception:
            log("[warn] routing table not available yet")

    def _print_route(self, dest:str):
        try:
            path = getattr(self.router, "paths", {}).get(dest, [])
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
    ap.add_argument("--topo", required=True, help="Neighbors-only JSON (type=topo)")
    ap.add_argument("--names", default=None, help="names-redis.json (opcional)")
    ap.add_argument("--metric", choices=["hop","rtt"], default="hop")
    ap.add_argument("--ttl", type=int, default=8)
    ap.add_argument("--hello", type=float, default=5.0)
    ap.add_argument("--lsp", type=float, default=10.0)
    ap.add_argument("--maxage", type=float, default=60.0)
    ap.add_argument("--redis-host", default="localhost")
    ap.add_argument("--redis-port", type=int, default=6379)
    ap.add_argument("--redis-db", type=int, default=0)
    ap.add_argument("--redis-pass", default=None)
    args = ap.parse_args()

    node = Node(args.id, args.topo, names_path=args.names, metric=args.metric,
                default_ttl=args.ttl, hello_interval=args.hello,
                lsp_interval=args.lsp, lsp_max_age=args.maxage,
                redis_host=args.redis_host, redis_port=args.redis_port,
                redis_db=args.redis_db, redis_pass=args.redis_pass)
    node.start()


if __name__ == "__main__":
    main()
