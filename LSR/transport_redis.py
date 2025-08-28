"""
transport_redis.py — Transporte Pub/Sub con Redis.
- Soporta canal por nodo con map (names-redis.json) o default net:inbox:<ID>.
"""

from __future__ import annotations
from typing import Any, Callable, Iterable, Optional, Dict
import json
import os
import threading
import redis

Packet  = dict
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
        channel_map: Optional[Dict[str, str]] = None,  # <-- NUEVO
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
        self.channel_map = channel_map or {}              # id -> canal exacto

        self._r = redis.Redis(host=self.host, port=self.port, db=self.db,
                              password=self.password, decode_responses=True)
        self._ps = self._r.pubsub()
        self._stop_evt = threading.Event()
        self._thread: Optional[threading.Thread] = None

    # --- canales
    def _default_inbox(self) -> str:
        return f"net:inbox:{self.node_id}"

    @property
    def inbox_channel(self) -> str:
        return self.channel_map.get(self.node_id, self._default_inbox())

    def channel_for(self, node_id: str) -> str:
        return self.channel_map.get(node_id, f"net:inbox:{node_id}")

    # --- ciclo
    def start(self) -> None:
        ch = self.inbox_channel
        self._ps.subscribe(ch)
        self._thread = threading.Thread(target=self._listen_loop, daemon=True)
        self._thread.start()
        self._log(f"[RedisTransport] Subscribed to {ch} @ {self.host}:{self.port}/{self.db}")

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
                if msg.get("type") != "message":
                    continue
                raw = msg.get("data")
                pkt = json.loads(raw)
                src = pkt.get("from", "?")
                self.on_packet(pkt, src)
            except Exception as e:
                if self.on_error:
                    self.on_error(e, msg.get("data") if isinstance(msg, dict) else None)

    # --- envío
    def publish_packet(self, neighbor_id: str, packet: Packet) -> None:
        ch = self.channel_for(neighbor_id)
        self._r.publish(ch, json.dumps(packet, ensure_ascii=False))

    def broadcast(self, neighbors: Iterable[str], packet: Packet, *, exclude: Optional[str] = None) -> None:
        for nb in neighbors:
            if exclude and nb == exclude:
                continue
            self.publish_packet(nb, packet)

    # --- log
    def _log(self, s: str) -> None:
        if self.on_log:
            self.on_log(s)
