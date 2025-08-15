import time, uuid, json, sys, threading, datetime

def now_iso():
    return datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()

def gen_id():
    return str(uuid.uuid4())

def pretty(obj):
    return json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True)

class SafePrint:
    _lock = threading.Lock()
    def __call__(self, *args, **kwargs):
        with self._lock:
            print(*args, **kwargs, flush=True)

log = SafePrint()

class ExpiringSet:
    """Seen-message ID cache with TTL eviction."""
    def __init__(self, ttl_seconds:int=120):
        self.ttl = ttl_seconds
        self.data = {}  # id -> expires_at
        self._lock = threading.Lock()

    def add_if_new(self, key:str) -> bool:
        now = time.time()
        with self._lock:
            self._evict(now)
            if key in self.data:
                return False
            self.data[key] = now + self.ttl
            return True

    def _evict(self, now:float):
        expired = [k for k, t in self.data.items() if t <= now]
        for k in expired:
            self.data.pop(k, None)
