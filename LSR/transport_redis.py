"""
transport_redis.py — Transporte Pub/Sub con Redis.

- Cada nodo se suscribe a su canal inbox:  net:inbox:<NODE_ID>
- Para enviar a un vecino: publish a      net:inbox:<NEIGHBOR_ID>
- Para broadcast a vecinos: el Nodo hace N publishes (uno por vecino).
- Este módulo NO hace forwarding; sólo entrega paquetes JSON al callback.

Requiere: pip install redis>=5
Vars de entorno opcionales:
  REDIS_HOST (default "localhost")
  REDIS_PORT (default "6379")
  REDIS_DB   (default "0")
"""

from __future__ import annotations
from typing import Any, Callable, Iterable, Optional
import json
import os
import threading
import redis

Packet = dict
OnPacket = Callable[[Packet, str], None]  # (packet, src) -> None
OnError  = Callable[[Exception, Optional[str]], None]  # (exc, raw) -> None
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

        self._r = redis.Redis(host=self.host, port=self.port, db=self.db, decode_responses=True)
        self._ps = self._r.pubsub()
        self._stop_evt = threading.Event()
        self._thread: Optional[threading.Thread] = None

    # --------------- canales ----------------

    @property
    def inbox_channel(self) -> str:
        return f"net:inbox:{self.node_id}"

    @staticmethod
    def channel_for(node_id: str) -> str:
        return f"net:inbox:{node_id}"

    # --------------- ciclo de escucha ----------------

    def start(self) -> None:
        """
        Inicia el hilo receptor y se suscribe a tu inbox.
        """
        self._ps.subscribe(self.inbox_channel)
        self._thread = threading.Thread(target=self._listen_loop, daemon=True)
        self._thread.start()
        self._log(f"[RedisTransport] Subscribed to {self.inbox_channel} @ {self.host}:{self.port}/{self.db}")

    def stop(self) -> None:
        """
        Detiene el hilo receptor y cierra pubsub.
        """
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
                # Entregamos al callback del Nodo (forwarding de tu app)
                self.on_packet(pkt, src)
            except Exception as e:
                if self.on_error:
                    self.on_error(e, msg.get("data") if isinstance(msg, dict) else None)
                else:
                    # fallback silencioso
                    pass

    # --------------- envío ----------------

    def publish_packet(self, neighbor_id: str, packet: Packet) -> None:
        """
        Envía un paquete (dict) a un vecino por su inbox.
        """
        ch = self.channel_for(neighbor_id)
        self._r.publish(ch, json.dumps(packet, ensure_ascii=False))

    def broadcast(self, neighbors: Iterable[str], packet: Packet, *, exclude: Optional[str] = None) -> None:
        """
        Convenience: broadcast manual (N publishes, uno por vecino).
        """
        for nb in neighbors:
            if exclude and nb == exclude:
                continue
            self.publish_packet(nb, packet)

    # --------------- logs ----------------

    def _log(self, s: str) -> None:
        if self.on_log:
            self.on_log(s)
