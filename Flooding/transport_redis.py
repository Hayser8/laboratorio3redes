"""
Redis transport (Pub/Sub) con:
- canal por nodo vía channel_map (names-redis.json) o net:inbox:<ID> por defecto
- logs claros de suscripción/publicación
- health check (PING) al arrancar
- broadcast con deduplicación por canal (para el caso "todos al mismo canal")
"""

from __future__ import annotations
from typing import Any, Callable, Iterable, Optional, Dict, Set
import json
import os
import threading
import redis

Packet   = dict
OnPacket = Callable[[Packet, str], None]
OnError  = Callable[[Exception, Optional[str]], None]
OnLog    = Callable[[str], None]


class RedisTransport:
    def __init__(
        self,
        node_id: str,
        on_packet: OnPacket,
        *,
        host: Optional[str] = None,
        port: Optional[int] = None,
        db: Optional[int] = None,
        password: Optional[str] = None,
        channel_map: Optional[Dict[str, str]] = None,
        on_error: Optional[OnError] = None,
        on_log: Optional[OnLog] = None,
    ):
        self.node_id = node_id
        self.on_packet = on_packet
        self.on_error = on_error
        self.on_log = on_log

        self.host = host or os.getenv("REDIS_HOST", "localhost")
        self.port = int(port or os.getenv("REDIS_PORT", "6379"))
        self.db   = int(db   or os.getenv("REDIS_DB", "0"))
        self.password = password or os.getenv("REDIS_PASSWORD")
        self.channel_map = dict(channel_map or {})  # id -> canal

        self._r = redis.Redis(
            host=self.host, port=self.port, db=self.db,
            password=self.password, decode_responses=True
        )
        self._ps = self._r.pubsub()
        self._stop_evt = threading.Event()
        self._thread: Optional[threading.Thread] = None

    # -------- canales
    def _default_inbox(self) -> str:
        return f"net:inbox:{self.node_id}"

    @property
    def inbox_channel(self) -> str:
        return self.channel_map.get(self.node_id, self._default_inbox())

    def channel_for(self, node_id: str) -> str:
        return self.channel_map.get(node_id, f"net:inbox:{node_id}")

    # -------- ciclo
    def start(self) -> None:
        # Health check: te fallará acá si el password/host/puerto están mal.
        try:
            self._r.ping()
        except Exception as e:
            self._log(f"[RedisTransport] PING failed: {e}")
            raise

        ch = self.inbox_channel
        self._ps.subscribe(ch)
        self._thread = threading.Thread(target=self._listen_loop, daemon=True)
        self._thread.start()
        self._log(f"[RedisTransport] Subscribed to '{ch}' @ {self.host}:{self.port}/{self.db}")

        # Para depurar: muestra cómo está resuelto el channel_map (primeros pares)
        if self.channel_map:
            preview = list(self.channel_map.items())[:5]
            self._log(f"[RedisTransport] channel_map sample: {preview}")

    def stop(self) -> None:
        self._stop_evt.set()
        try:
            self._ps.close()
        except Exception:
            pass
        self._log("[RedisTransport] Stopped.")

    def _listen_loop(self) -> None:
        for msg in self._ps.listen():
            if self._stop_evt.is_set():
                break
            try:
                if not isinstance(msg, dict):
                    continue
                mtype = msg.get("type")
                if mtype != "message":
                    # Los primeros eventos suelen ser "subscribe"
                    continue
                raw = msg.get("data")
                if not isinstance(raw, str):
                    continue
                try:
                    pkt = json.loads(raw)
                except Exception as je:
                    self._log(f"[RedisTransport] drop: invalid JSON: {je} raw={raw!r}")
                    continue
                src = pkt.get("from") or "?"
                self.on_packet(pkt, src)
            except Exception as e:
                if self.on_error:
                    try:
                        raw = msg.get("data") if isinstance(msg, dict) else None
                    except Exception:
                        raw = None
                    self.on_error(e, raw)
                else:
                    self._log(f"[RedisTransport] on_packet error: {e}")

    # -------- envío
    def publish_packet(self, neighbor_id: str, packet: Packet) -> None:
        ch = self.channel_for(neighbor_id)
        self._r.publish(ch, json.dumps(packet, ensure_ascii=False))
        self._log(f"[RedisTransport] PUBLISH -> '{ch}' msg_id={packet.get('msg_id')} to={packet.get('to')}")

    def broadcast(self, neighbors: Iterable[str], packet: Packet, *, exclude: Optional[str] = None) -> None:
        """Deduplica por canal real (si varios vecinos usan el mismo canal)."""
        channels: Set[str] = set()
        for nb in neighbors:
            if exclude and nb == exclude:
                continue
            ch = self.channel_for(nb)
            if ch in channels:
                continue
            channels.add(ch)
        payload = json.dumps(packet, ensure_ascii=False)
        for ch in channels:
            self._r.publish(ch, payload)
            self._log(f"[RedisTransport] BROADCAST -> '{ch}' msg_id={packet.get('msg_id')} to={packet.get('to')}")

    # Útil para pruebas manuales
    def publish_raw(self, channel: str, packet: Packet) -> None:
        self._r.publish(channel, json.dumps(packet, ensure_ascii=False))
        self._log(f"[RedisTransport] RAW PUBLISH -> '{channel}' id={packet.get('msg_id')}")

    # -------- log
    def _log(self, s: str) -> None:
        if self.on_log:
            self.on_log(s)
