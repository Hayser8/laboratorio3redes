import time, uuid, json, threading, datetime

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
