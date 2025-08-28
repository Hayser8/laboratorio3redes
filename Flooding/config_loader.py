# Flooding/config_loader.py
import json
from typing import Dict, List, Tuple, Optional
from utils import log


def load_topology(path: str) -> Dict[str, List[str]]:
    """
    Carga un archivo de topología con formato:
      { "type": "topo", "config": { "A": ["B","C"], "B": ["A"], ... } }
    También tolera listas o dicts de pesos, normalizando a lista de vecinos.
    """
    with open(path, "r", encoding="utf-8") as f:
        obj = json.load(f)
    assert obj.get("type") == "topo", "topology file must have type=topo"
    config = obj["config"]

    topo: Dict[str, List[str]] = {}
    for k, v in config.items():
        if isinstance(v, dict):
            topo[k] = list(v.keys())
        else:
            topo[k] = list(v)
    return topo


def load_names(path: str) -> Dict[str, Tuple[str, int]]:
    """
    Carga un archivo 'names' en el formato clásico:
      { "type": "names", "config": { "A": "127.0.0.1:5001", "B": "127.0.0.1:5002" } }

    TOLERANTE: Si encuentra entradas del tipo {"channel": "..."} (formato por canales),
    las ignora en este mapeo (para no romper compatibilidad). Úsese con sockets/IDs.
    """
    with open(path, "r", encoding="utf-8") as f:
        obj = json.load(f)
    assert obj.get("type") == "names", "names file must have type=names"

    mapping: Dict[str, Tuple[str, int]] = {}
    cfg = obj["config"]

    for node_id, val in cfg.items():
        # Formato clásico "host:port"
        if isinstance(val, str) and ":" in val:
            host, port_s = val.split(":", 1)
            try:
                port = int(port_s.strip())
            except ValueError:
                raise ValueError(f"Invalid port for node {node_id}: {port_s!r}")
            mapping[node_id] = (host.strip(), port)
        # Formato por canales -> se ignora aquí (lo maneja load_names_channels)
        elif isinstance(val, dict) and "channel" in val:
            continue
        else:
            # No válido para este loader; lo dejamos pasar silenciosamente
            # para no romper, pero avisamos en logs.
            try:
                log(f"[warn] load_names: skipping unsupported entry for {node_id}: {val}")
            except Exception:
                pass
    return mapping


# ===================== NUEVO: soporte para formato por CANALES =====================

def load_names_channels(path: str) -> Dict[str, dict]:
    """
    Carga un archivo 'names' flexible que puede incluir:
      {
        "type": "names",
        "host": "lab3.redesuvg.cloud",     # opcional (meta)
        "port": 6379,                      # opcional (meta)
        "pwd": "UVGRedis2025",             # opcional (meta)
        "ssl": false,                      # opcional (meta; si true -> rediss://)
        "config": {
          "A": {"channel": "sec20.topologia1.node5.test1"},
          "B": {"channel": "sec20.topologia1.node5.test2"},
          "C": "127.0.0.1:5003"  # también tolera host:port en el mismo archivo
        }
      }

    Retorna:
      {
        "meta": {"host":..., "port":..., "pwd":..., "ssl": bool},
        "channels": { node_id: channel_str },
        "hostports": { node_id: (host, port) }
      }
    """
    with open(path, "r", encoding="utf-8") as f:
        obj = json.load(f)
    assert obj.get("type") == "names", "names file must have type=names"

    # Meta para construir URL de Redis si se desea
    meta = {
        "host": obj.get("host"),
        "port": obj.get("port"),
        "pwd":  obj.get("pwd"),
        "ssl":  bool(obj.get("ssl", False)),
        # Si el archivo algún día incluyera "url", lo respetamos en build_redis_url
        "url":  obj.get("url"),
    }

    channels: Dict[str, str] = {}
    hostports: Dict[str, Tuple[str, int]] = {}

    cfg = obj["config"]
    for node_id, val in cfg.items():
        if isinstance(val, dict) and "channel" in val:
            ch = str(val["channel"]).strip()
            if not ch:
                raise ValueError(f"Empty channel for node {node_id}")
            channels[node_id] = ch
        elif isinstance(val, str) and ":" in val:
            host, port_s = val.split(":", 1)
            try:
                port = int(port_s.strip())
            except ValueError:
                raise ValueError(f"Invalid port for node {node_id}: {port_s!r}")
            hostports[node_id] = (host.strip(), port)
        else:
            try:
                log(f"[warn] load_names_channels: unsupported entry for {node_id}: {val}")
            except Exception:
                pass

    return {"meta": meta, "channels": channels, "hostports": hostports}


def get_channel(names_info: Dict[str, dict], node_id: str) -> str:
    """
    Helper para extraer el canal de un node_id (lanza KeyError si no existe).
    """
    return names_info["channels"][node_id]


def build_redis_url(meta: Dict[str, object]) -> Optional[str]:
    """
    Construye una URL redis/rediss a partir de meta:
      - Si meta['url'] existe, se devuelve tal cual.
      - Si hay host/port (y opcional pwd, ssl), arma:
          redis://[:pwd]@host:port/0
          rediss://[:pwd]@host:port/0  (si ssl=True)
      - Si falta host o port, retorna None.
    """
    # Prioridad: si el JSON ya trae una URL explícita, úsala
    if meta.get("url"):
        return str(meta["url"])

    host = meta.get("host")
    port = meta.get("port")
    pwd  = meta.get("pwd")
    ssl  = bool(meta.get("ssl", False))

    if not host or not port:
        return None

    try:
        port_i = int(port)  # tolera port como string o int
    except (TypeError, ValueError):
        raise ValueError(f"Invalid redis meta.port: {port!r}")

    scheme = "rediss" if ssl else "redis"
    if pwd:
        return f"{scheme}://:{pwd}@{host}:{port_i}/0"
    return f"{scheme}://{host}:{port_i}/0"
