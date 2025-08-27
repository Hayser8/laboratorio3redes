"""
protocols.py — Estándar JSON de mensajes + utilidades comunes.

Formato JSON obligatorio (v1):
{
  "proto":   "dijkstra|flooding|lsr|dvr",
  "type":    "hello|info|message|echo|lsp",
  "from":    "A",
  "to":      "B|broadcast",
  "ttl":     5,
  "headers": ["A","B","C"],   # últimos hops para detectar ciclos (máx. 3)
  "payload": {},              # tabla/LSP/datos de usuario según type
  "msg_id":  "uuid-..."       # para suprimir duplicados
}

Notas:
- HELLO: broadcast a vecinos directos; NO se retransmite al recibir.
- INFO/LSP: broadcast con retransmisión; al reenviar hacer ttl-- y rotar headers.
- MESSAGE (DATA): unicast hop-by-hop usando tu tabla de ruteo.
"""

from __future__ import annotations
from typing import Any, Dict, Iterable, List, Optional, Tuple
import uuid
import time

# =========================
# Constantes de protocolo
# =========================

# proto
PROTO_DIJKSTRA = "dijkstra"
PROTO_FLOODING = "flooding"
PROTO_LSR      = "lsr"
PROTO_DVR      = "dvr"

# type
TYPE_HELLO   = "hello"
TYPE_INFO    = "info"     # puede usarse para DV o info general
TYPE_LSP     = "lsp"      # típico de LSR (Link State Packet)
TYPE_MESSAGE = "message"  # datos de usuario
TYPE_ECHO    = "echo"

# otros
BROADCAST = "broadcast"

# parámetros por defecto
DEFAULT_TTL_INFO     = 5
DEFAULT_TTL_MESSAGE  = 8
HEADERS_MAXLEN       = 3


# =========================
# Helpers generales
# =========================

def make_msg_id() -> str:
    return str(uuid.uuid4())


def normalize_headers(headers: Optional[Iterable[str]]) -> List[str]:
    if headers is None:
        return []
    # filtra nulos y castea a str
    out = [str(h) for h in headers if h is not None]
    return out[-HEADERS_MAXLEN:]


def rotate_headers(headers: Optional[Iterable[str]], self_id: str,
                   maxlen: int = HEADERS_MAXLEN) -> List[str]:
    """
    Regla del enunciado:
    - al reenviar, remover el primer elemento
    - agregar self_id al final
    - mantener últimos 3
    """
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


# =========================
# Mensajes (builders)
# =========================

def build_packet(
    *,
    proto: str,
    ptype: str,
    from_id: str,
    to: str,
    ttl: int,
    headers: Optional[Iterable[str]] = None,
    payload: Any = None,
    msg_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Crea un paquete JSON conforme al estándar.
    """
    pkt = {
        "proto":   str(proto),
        "type":    str(ptype),
        "from":    str(from_id),
        "to":      str(to),
        "ttl":     int(ttl),
        "headers": normalize_headers(headers),
        "payload": payload if payload is not None else {},
        "msg_id":  msg_id or make_msg_id(),
    }
    return pkt


def new_hello(self_id: str, *, proto: str = PROTO_LSR, ttl: int = DEFAULT_TTL_INFO) -> Dict[str, Any]:
    """
    HELLO: broadcast a vecinos; NO se retransmite al recibir.
    """
    return build_packet(
        proto=proto, ptype=TYPE_HELLO, from_id=self_id, to=BROADCAST,
        ttl=ttl, headers=[self_id], payload={}
    )


def new_info(
    self_id: str,
    payload: Dict[str, Any],
    *,
    proto: str = PROTO_DVR,  # o PROTO_LSR si usas INFO para LSR también
    ttl: int = DEFAULT_TTL_INFO,
    headers: Optional[Iterable[str]] = None,
) -> Dict[str, Any]:
    """
    INFO: broadcast con retransmisión (ttl-- y rotación de headers al reenviar).
    Para DV: payload puede ser tu vector de distancias.
    Para LSR si no usas TYPE_LSP: payload puede contener la tabla/estado que difundes.
    """
    return build_packet(
        proto=proto, ptype=TYPE_INFO, from_id=self_id, to=BROADCAST,
        ttl=ttl, headers=(headers or [self_id]), payload=payload
    )


def new_lsp(
    self_id: str,
    lsp: Dict[str, Any],
    *,
    ttl: int = DEFAULT_TTL_INFO,
    headers: Optional[Iterable[str]] = None,
) -> Dict[str, Any]:
    """
    LSP específico para LSR (Link State Packet).
    """
    return build_packet(
        proto=PROTO_LSR, ptype=TYPE_LSP, from_id=self_id, to=BROADCAST,
        ttl=ttl, headers=(headers or [self_id]), payload=lsp
    )


def new_message(
    self_id: str,
    dest_id: str,
    data: Any,
    *,
    proto: str = PROTO_LSR,  # o el que quieras según tu modo
    ttl: int = DEFAULT_TTL_MESSAGE,
    headers: Optional[Iterable[str]] = None,
) -> Dict[str, Any]:
    """
    MESSAGE: datos de usuario (unicast hop-by-hop).
    """
    return build_packet(
        proto=proto, ptype=TYPE_MESSAGE, from_id=self_id, to=dest_id,
        ttl=ttl, headers=(headers or [self_id]), payload=data
    )


# =========================
# Validación / Sanitización
# =========================

REQUIRED_FIELDS = ("proto", "type", "from", "to", "ttl", "headers", "payload", "msg_id")

def is_valid_packet(pkt: Dict[str, Any]) -> Tuple[bool, str]:
    if not isinstance(pkt, dict):
        return False, "packet-not-dict"
    for k in REQUIRED_FIELDS:
        if k not in pkt:
            return False, f"missing-field:{k}"
    if not isinstance(pkt["headers"], list):
        return False, "headers-not-list"
    try:
        int(pkt["ttl"])
    except Exception:
        return False, "ttl-not-int"
    return True, "ok"


def sanitize_incoming(pkt: Dict[str, Any]) -> Dict[str, Any]:
    """
    Asegura tipos básicos y longitudes. No rota headers ni modifica TTL aquí.
    Aplica tras deserializar JSON.
    """
    ok, _ = is_valid_packet(pkt)
    if not ok:
        raise ValueError("invalid-packet-structure")

    pkt["proto"]   = str(pkt["proto"])
    pkt["type"]    = str(pkt["type"])
    pkt["from"]    = str(pkt["from"])
    pkt["to"]      = str(pkt["to"])
    pkt["ttl"]     = int(pkt["ttl"])
    pkt["headers"] = normalize_headers(pkt.get("headers"))
    # payload y msg_id se dejan como vienen (payload puede ser cualquier JSON)
    return pkt


def forward_transform(pkt: Dict[str, Any], self_id: str) -> Optional[Dict[str, Any]]:
    """
    Aplica reglas de forwarding para INFO/LSP/MESSAGE:
    - Drop si ciclo (self en headers).
    - Drop si ttl <= 0 tras decremento.
    - Rota headers.
    Devuelve un NUEVO dict; no muta el original.
    """
    if should_drop_for_cycle(self_id, pkt.get("headers")):
        return None
    new_ttl = decrement_ttl(pkt.get("ttl"))
    if new_ttl <= 0:
        return None
    new_headers = rotate_headers(pkt.get("headers"), self_id, HEADERS_MAXLEN)
    new_pkt = dict(pkt)
    new_pkt["ttl"] = new_ttl
    new_pkt["headers"] = new_headers
    return new_pkt


# =========================
# Anti-duplicados (opcional)
# =========================

class ExpiringSet:
    """
    Set con expiración para suprimir duplicados de msg_id.
    Uso:
        seen = ExpiringSet(ttl_seconds=60)
        if seen.add_if_new(pkt["msg_id"]):  # True -> primera vez
            ... procesar ...
    """
    def __init__(self, ttl_seconds: int = 60):
        self.ttl = int(ttl_seconds)
        self._data: Dict[str, float] = {}

    def _now(self) -> float:
        return time.monotonic()

    def _purge(self) -> None:
        now = self._now()
        to_del = [k for k, ts in self._data.items() if now - ts > self.ttl]
        for k in to_del:
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
