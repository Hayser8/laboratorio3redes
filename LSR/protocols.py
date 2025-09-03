"""
Formato JSON (v1):
{
  "proto":   "dijkstra|flooding|lsr|dvr",
  "type":    "hello|info|message|echo|lsp",
  "from":    "A",
  "to":      "B|broadcast",
  "ttl":     5,
  "headers": ["A","B","C"]  ó  {"trail":[...], "last_hop":"X"},
  "payload": {},
  "msg_id":  "uuid-..."
}
"""

from __future__ import annotations
from typing import Any, Dict, Iterable, List, Optional, Tuple
import uuid
import time

# ---- Constantes de protocolo
PROTO_DIJKSTRA = "dijkstra"
PROTO_FLOODING = "flooding"
PROTO_LSR      = "lsr"
PROTO_DVR      = "dvr"

TYPE_HELLO   = "hello"
TYPE_INFO    = "info"
TYPE_LSP     = "lsp"
TYPE_MESSAGE = "message"
TYPE_ECHO    = "echo"

BROADCAST = "broadcast"
DEFAULT_TTL_INFO     = 5
DEFAULT_TTL_MESSAGE  = 8
HEADERS_MAXLEN       = 3

# ---- Helpers
def make_msg_id() -> str:
    return str(uuid.uuid4())

def normalize_headers(headers: Optional[Iterable[str]]) -> List[str]:
    if headers is None:
        return []
    out = [str(h) for h in headers if h is not None]
    return out[-HEADERS_MAXLEN:]

def _extract_trail_from_headers_maybe_dict(hval: Any) -> List[str]:
    """
    Acepta:
      - list -> se normaliza
      - dict -> usa 'trail' si existe y es list, si no -> []
      - None/otros -> []
    """
    if isinstance(hval, list):
        return normalize_headers(hval)
    if isinstance(hval, dict):
        trail = hval.get("trail")
        path  = hval.get("path")
        if isinstance(trail, list):
            return normalize_headers(trail)
        if isinstance(path, list):
            return normalize_headers(path)
        return []
    return []

def rotate_headers(headers: Optional[Iterable[str]], self_id: str,
                   maxlen: int = HEADERS_MAXLEN) -> List[str]:
    hs = list(headers or [])
    if hs:
        hs = hs[1:]
    hs.append(self_id)
    return hs[-maxlen:]

def should_drop_for_cycle(self_id: str, headers: Optional[Iterable[str]]) -> bool:
    return self_id in set(headers or [])

def decrement_ttl(ttl: Optional[int]) -> int:
    if ttl is None:
        return 0
    return max(int(ttl) - 1, 0)

# ---- Builders (siempre dejan headers como LISTA en “wire”)
def build_packet(
    *, proto: str, ptype: str, from_id: str, to: str, ttl: int,
    headers: Optional[Iterable[str]] = None, payload: Any = None,
    msg_id: Optional[str] = None,
) -> Dict[str, Any]:
    return {
        "proto":   str(proto),
        "type":    str(ptype),
        "from":    str(from_id),
        "to":      str(to),
        "ttl":     int(ttl),
        "headers": normalize_headers(headers),
        "payload": payload if payload is not None else {},
        "msg_id":  msg_id or make_msg_id(),
    }

def new_hello(self_id: str, *, proto: str = PROTO_LSR, ttl: int = DEFAULT_TTL_INFO) -> Dict[str, Any]:
    return build_packet(proto=proto, ptype=TYPE_HELLO, from_id=self_id, to=BROADCAST,
                        ttl=ttl, headers=[self_id], payload={})

def new_info(self_id: str, payload: Dict[str, Any], *,
             proto: str = PROTO_LSR, ttl: int = DEFAULT_TTL_INFO,
             headers: Optional[Iterable[str]] = None) -> Dict[str, Any]:
    return build_packet(proto=proto, ptype=TYPE_INFO, from_id=self_id, to=BROADCAST,
                        ttl=ttl, headers=(headers or [self_id]), payload=payload)

def new_lsp(self_id: str, lsp: Dict[str, Any], *,
            ttl: int = DEFAULT_TTL_INFO,
            headers: Optional[Iterable[str]] = None) -> Dict[str, Any]:
    return build_packet(proto=PROTO_LSR, ptype=TYPE_LSP, from_id=self_id, to=BROADCAST,
                        ttl=ttl, headers=(headers or [self_id]), payload=lsp)

def new_message(self_id: str, dest_id: str, data: Any, *,
                proto: str = PROTO_LSR, ttl: int = DEFAULT_TTL_MESSAGE,
                headers: Optional[Iterable[str]] = None) -> Dict[str, Any]:
    return build_packet(proto=proto, ptype=TYPE_MESSAGE, from_id=self_id, to=dest_id,
                        ttl=ttl, headers=(headers or [self_id]), payload=data)

# ---- Validación / saneamiento de entrada
REQUIRED_FIELDS = ("proto", "type", "from", "to", "ttl", "headers", "payload", "msg_id")

def is_valid_packet(pkt: Dict[str, Any]) -> Tuple[bool, str]:
    if not isinstance(pkt, dict):
        return False, "packet-not-dict"
    for k in REQUIRED_FIELDS:
        if k not in pkt:
            return False, f"missing-field:{k}"
    if not isinstance(pkt.get("headers"), (list, dict)):
        return False, "headers-bad-type"
    try:
        int(pkt["ttl"])
    except Exception:
        return False, "ttl-not-int"
    return True, "ok"

def sanitize_incoming(pkt: Dict[str, Any]) -> Dict[str, Any]:
    """
    Acepta paquetes “ensuciados” y los normaliza a nuestro wire-format (headers LIST).
    No hace verificaciones de semántica, sólo estructura básica.
    """
    if not isinstance(pkt, dict):
        raise ValueError("invalid-packet-structure")

    # Normaliza tipos básicos
    if "proto" in pkt:   pkt["proto"] = str(pkt["proto"])
    if "type"  in pkt:   pkt["type"]  = str(pkt["type"])
    if "from"  in pkt:   pkt["from"]  = str(pkt["from"])
    if "to"    in pkt:   pkt["to"]    = str(pkt["to"])
    if "ttl"   in pkt:   pkt["ttl"]   = int(pkt["ttl"])

    # headers: permitir list o dict -> convertir a LIST
    h = pkt.get("headers")
    pkt["headers"] = _extract_trail_from_headers_maybe_dict(h)

    # msg_id: si falta, generar (o copiar de headers.dict si venía allí)
    if "msg_id" not in pkt or not pkt.get("msg_id"):
        hid = None
        if isinstance(h, dict):
            hid = h.get("msg_id")
        pkt["msg_id"] = str(hid) if hid else make_msg_id()

    # payload por defecto
    if "payload" not in pkt:
        pkt["payload"] = {}

    # Defaults por si venían ausentes
    pkt.setdefault("proto", PROTO_LSR)
    pkt.setdefault("type",  TYPE_MESSAGE)
    pkt.setdefault("from",  "?")
    pkt.setdefault("to",    BROADCAST)
    pkt["ttl"] = int(pkt.get("ttl", DEFAULT_TTL_INFO))

    # Validación final
    for k in REQUIRED_FIELDS:
        if k not in pkt:
            raise ValueError("invalid-packet-structure")
    return pkt

# ---- Anti-duplicados simple
class ExpiringSet:
    def __init__(self, ttl_seconds: int = 60):
        self.ttl = int(ttl_seconds)
        self._data: Dict[str, float] = {}
    def _now(self) -> float:
        return time.monotonic()
    def _purge(self) -> None:
        now = self._now()
        for k in [k for k, ts in self._data.items() if now - ts > self.ttl]:
            del self._data[k]
    def add_if_new(self, key: str) -> bool:
        self._purge()
        if key in self._data:
            return False
        self._data[key] = self._now()
        return True
    def __contains__(self, key: str) -> bool:
        self._purge()
        return key in self._data

# ---- SHIM de compatibilidad (código legado que usaba build_message)
def build_message(proto, ptype, from_id, to, ttl=5, payload=None, headers=None, msg_id=None):
    if isinstance(headers, list):
        headers_list = normalize_headers(headers)
    elif isinstance(headers, dict):
        headers_list = _extract_trail_from_headers_maybe_dict(headers)
    else:
        headers_list = [str(from_id)]
    return build_packet(proto=proto, ptype=ptype, from_id=from_id, to=to,
                        ttl=ttl, headers=headers_list, payload=payload,
                        msg_id=msg_id or make_msg_id())
