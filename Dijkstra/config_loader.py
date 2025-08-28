# config_loader.py
from __future__ import annotations
from typing import Dict, List, Any
import json

def _coerce_edges(value: Any) -> Dict[str, int]:
    """
    Normaliza formatos posibles de aristas a { vecino: costo(int) }.
    Acepta:
      - {"B":1,"C":2}
      - [{"to":"B","cost":1}, ...]
      - [["B",1], ["C",2]]
      - ["B","C"]  (costo=1)
    """
    edges: Dict[str, int] = {}
    if value is None:
        return edges
    if isinstance(value, dict):
        for k, v in value.items():
            try:
                edges[str(k)] = int(v)
            except Exception:
                edges[str(k)] = 1
        return edges
    if isinstance(value, list):
        for item in value:
            if isinstance(item, str):
                edges[item] = 1
            elif isinstance(item, (list, tuple)) and len(item) >= 2:
                nb, w = item[0], item[1]
                try:
                    edges[str(nb)] = int(w)
                except Exception:
                    edges[str(nb)] = 1
            elif isinstance(item, dict) and "to" in item:
                nb = str(item["to"])
                w = item.get("cost", 1)
                try:
                    edges[nb] = int(w)
                except Exception:
                    edges[nb] = 1
    return edges

def _ensure_undirected(G: Dict[str, Dict[str, int]]) -> Dict[str, Dict[str, int]]:
    # añade aristas inversas si faltan (mismo costo)
    for u, nbrs in list(G.items()):
        for v, w in nbrs.items():
            G.setdefault(v, {})
            G[v].setdefault(u, w)
    return G

def load_graph(topo_path: str) -> Dict[str, Dict[str, int]]:
    """Lee el grafo completo en forma { nodo: { vecino: costo, ... }, ... }"""
    with open(topo_path, "r", encoding="utf-8") as f:
        obj = json.load(f)
    assert obj.get("type") == "topo", "topology file must have type=topo"
    cfg = obj.get("config") or {}
    G: Dict[str, Dict[str, int]] = {}
    for node, edges in cfg.items():
        G[str(node)] = _coerce_edges(edges)
    return _ensure_undirected(G)

def load_neighbors_only(topo_path: str, node_id: str) -> List[str]:
    """Lista simple de vecinos directos del nodo dado (costo ignorado)."""
    G = load_graph(topo_path)
    return sorted(list(G.get(str(node_id), {}).keys()))
