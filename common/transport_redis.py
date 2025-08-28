# common/transport_redis.py
# Si el server remoto usa TLS, la URL será rediss://… en vez de redis://
import json, threading
from typing import Callable, Optional
import redis

class RedisTransport:
    def __init__(self, *, url: Optional[str], host: str, port: int, password: Optional[str], my_channel: str):
        self.client = redis.Redis.from_url(url, decode_responses=True) if url else \
                      redis.Redis(host=host, port=port, password=password, decode_responses=True)
        self.pubsub = self.client.pubsub(ignore_subscribe_messages=True)
        self.my_channel = my_channel
        self._t = None
        self._on_message: Optional[Callable[[dict], None]] = None

    def start(self, on_message: Callable[[dict], None]):
        self._on_message = on_message
        self.pubsub.subscribe(self.my_channel)

        def loop():
            for msg in self.pubsub.listen():
                if msg.get("type") == "message":
                    raw = msg.get("data")
                    try:
                        data = json.loads(raw)
                    except Exception:
                        data = {"proto":"unknown","type":"raw","payload":raw}
                    on_message(data)
        self._t = threading.Thread(target=loop, daemon=True)
        self._t.start()

    def send(self, dest_channel: str, packet: dict):
        return self.client.publish(dest_channel, json.dumps(packet))

    def stop(self):
        try: self.pubsub.close()
        except: pass
