# Flooding/node.py
import argparse
import threading
import time
import json
from typing import Optional, List
from utils import log, now_iso

from protocols import (
    new_hello, new_message,
    sanitize_incoming, forward_transform, ExpiringSet,
    TYPE_HELLO, TYPE_MESSAGE,
    PROTO_FLOODING,
)

# Transporte existente para modo IDs (publica por neighbor_id)
from transport_redis import RedisTransport

# Cargadores de configuración
from config_loader import (
    load_topology, load_names,            # compatibles con tu flujo actual
    load_names_channels, build_redis_url  # NUEVOS helpers para "channels"
)


# ==== NUEVO: Transporte Redis por "canales" (pub/sub explícito) ====
class ChannelRedisTransport:
    """
    Pub/Sub por 'channel' explícito para interoperar con un servidor remoto.
    Vive dentro de Flooding/ para no mover nada a common/.
    """
    def __init__(
        self,
        my_channel: str,
        on_packet,
        url: Optional[str] = None,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        password: Optional[str] = None,
    ):
        try:
            import redis  # import local para no exigirlo si no se usa este modo
        except ImportError as e:
            raise RuntimeError("Falta la librería 'redis'. Instala con: pip install redis") from e

        self.redis = redis
        if url:
            self.client = self.redis.Redis.from_url(url, decode_responses=True)
        else:
            self.client = self.redis.Redis(
                host=host, port=port, db=db, password=password, decode_responses=True
            )

        self.pubsub = self.client.pubsub(ignore_subscribe_messages=True)
        self.my_channel = my_channel
        self._on_packet = on_packet
        self._t = None
        self._stop = threading.Event()

    def start(self):
        self.pubsub.subscribe(self.my_channel)

        def loop():
            for msg in self.pubsub.listen():
                if self._stop.is_set():
                    break
                if msg.get("type") == "message":
                    raw = msg.get("data")
                    try:
                        pkt = json.loads(raw)
                    except Exception:
                        pkt = {"type": "raw", "payload": raw}
                    # _src: podrías pasar self.my_channel si quieres trazar origen
                    self._on_packet(pkt, self.my_channel)

        self._t = threading.Thread(target=loop, name=f"redis-sub-{self.my_channel}", daemon=True)
        self._t.start()

    def publish_channel(self, channel: str, pkt: dict):
        self.client.publish(channel, json.dumps(pkt))

    def broadcast_channels(self, channels: List[str], pkt: dict, exclude: Optional[str] = None):
        for ch in channels:
            if exclude and ch == exclude:
                continue
            self.publish_channel(ch, pkt)

    def stop(self):
        try:
            self._stop.set()
            self.pubsub.close()
        except:
            pass


class Node:
    def __init__(
        self,
        node_id: str,
        topo_path: str,
        names_path: str,
        default_ttl: int = 6,
        hello_interval: float = 10.0,
        redis_host: str = "localhost",
        redis_port: int = 6379,
        redis_db: int = 0,
        redis_url: Optional[str] = None,   # NUEVO: permite URL (redis/rediss)
    ):
        self.id = node_id

        # Topología: {A:[B,C], ...} o {A:{B:1,...}}
        self.topo = load_topology(topo_path)
        neighs = self.topo.get(self.id, {})
        self.neighbors: List[str] = list(neighs.keys() if isinstance(neighs, dict) else neighs)

        # Compat: mapa IDs -> (host,port) para tu flujo local/sockets
        self.addr_map = load_names(names_path)

        # NUEVO: names con "channels" y meta (host/port/pwd, ssl opcional)
        try:
            self.names_info = load_names_channels(names_path)  # {'meta', 'channels', 'hostports'}
        except Exception:
            # Si el archivo no trae "channels", simplemente quedará vacío
            self.names_info = {"meta": {}, "channels": {}, "hostports": {}}

        self.channel_map: dict = self.names_info.get("channels", {}) or {}
        self.redis_meta: dict = self.names_info.get("meta", {}) or {}

        # Si estamos en modo canales, debo tener mi canal
        self.my_channel: Optional[str] = self.channel_map.get(self.id)

        # URL efectiva (CLI > meta en names > None)
        self.redis_url = redis_url or build_redis_url(self.redis_meta)

        # ---- Selección de transporte ----
        self.channel_mode = bool(self.my_channel)  # True si archivo names trae canales
        if self.channel_mode:
            # Modo canales remotos (pub/sub por canal explícito)
            self.transport = ChannelRedisTransport(
                my_channel=self.my_channel,
                on_packet=self._on_packet,
                url=self.redis_url,
                host=redis_host,
                port=redis_port,
                db=redis_db,
                password=self.redis_meta.get("pwd"),
            )
            # Canales de vecinos (solo los que tengan canal mapeado)
            self.neighbor_channels: List[str] = [
                self.channel_map[n] for n in self.neighbors if n in self.channel_map
            ]
        else:
            # Modo IDs (tu transporte actual, publica por neighbor_id)
            self.transport = RedisTransport(
                node_id=self.id, on_packet=self._on_packet, host=redis_host, port=redis_port, db=redis_db
            )

        self.default_ttl = default_ttl
        self.hello_interval = hello_interval
        self._hello_stop = threading.Event()

        self.seen = ExpiringSet(ttl_seconds=60)

    # --- envío ---
    def send_direct(self, neighbor_id: str, pkt: dict):
        if self.channel_mode:
            ch = self.channel_map.get(neighbor_id)
            if ch:
                self.transport.publish_channel(ch, pkt)
        else:
            self.transport.publish_packet(neighbor_id, pkt)

    def broadcast(self, pkt: dict, exclude: Optional[str] = None):
        if self.channel_mode:
            # 'exclude' es un ID; traducimos a canal si aplica
            exclude_ch = self.channel_map.get(exclude) if exclude else None
            self.transport.broadcast_channels(self.neighbor_channels, pkt, exclude=exclude_ch)
        else:
            self.transport.broadcast(self.neighbors, pkt, exclude=exclude)

    # --- ciclo de vida ---
    def start(self):
        mode_str = "channels" if self.channel_mode else "ids"
        log(f"[node] {self.id} mode={mode_str} neighbors={self.neighbors}")
        if self.channel_mode:
            log(f"[node] my_channel={self.my_channel} redis_url={'provided' if self.redis_url else 'host/port'}")
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
    def _on_packet(self, pkt: dict, _src: str):
        try:
            pkt = sanitize_incoming(pkt)
        except Exception:
            return

        mid = pkt.get("msg_id")
        if mid and not self.seen.add_if_new(mid):
            return

        ptype = pkt.get("type")
        dest = pkt.get("to")
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
                dest = parts[1]
                text = " ".join(parts[2:])
                pkt = new_message(self.id, dest, text, proto=PROTO_FLOODING, ttl=self.default_ttl)
                self.broadcast(pkt)
            elif cmd == "ttl" and len(parts) == 2:
                try:
                    self.default_ttl = int(parts[1])
                    log(f"default TTL set to {self.default_ttl}")
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

    # Modo IDs (legacy/local)
    ap.add_argument("--redis-host", default="localhost")
    ap.add_argument("--redis-port", type=int, default=6379)
    ap.add_argument("--redis-db", type=int, default=0)

    # NUEVO: URL (tiene prioridad si se provee). Útil para remoto (redis/rediss).
    ap.add_argument("--redis-url")

    args = ap.parse_args()

    node = Node(
        args.id, args.topo, args.names, args.ttl, args.hello,
        redis_host=args.redis_host, redis_port=args.redis_port, redis_db=args.redis_db,
        redis_url=args.redis_url
    )
    node.start()


if __name__ == "__main__":
    main()
