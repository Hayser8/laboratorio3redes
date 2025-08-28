# Flooding/config_loader.py
# Carga solo la lista de vecinos del nodo desde topo-compat.json (o formatos similares).

from __future__ import annotations
import json
from typing import List

def load_neighbors_only(topo_path: str, self_id: str) -> List[str]:
    """
    Acepta:
    - {"type":"topo","config":{"A":["B","C"], "B":["A"] ...}}
    - {"A":["B","C"], "B":["A"], ...}  (sin "type")
    - {"type":"topo","config":{"A":{"neighbors":["B","C"]}, ...}}
    """
    with open(topo_path, "r", encoding="utf-8") as f:
        obj = json.load(f)

    if isinstance(obj, dict) and obj.get("type") == "topo":
        cfg = obj.get("config", {})
    else:
        cfg = obj  # mapa simple

    if self_id not in cfg:
        raise KeyError(f"node '{self_id}' not found in topology '{topo_path}'")

    entry = cfg[self_id]
    if isinstance(entry, dict):
        neighbors = entry.get("neighbors") or entry.get("links") or entry.get("adj") or []
    else:
        neighbors = entry

    if not isinstance(neighbors, list):
        raise ValueError("neighbors must be a list")
    return [str(n) for n in neighbors]
