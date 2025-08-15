from __future__ import annotations
import heapq
from typing import Dict, Tuple, List, Optional

INF = float("inf")

def shortest_paths(graph:Dict[str, Dict[str, float]], source:str) -> Tuple[Dict[str,float], Dict[str, Optional[str]]]:
    """Classic Dijkstra. Returns (dist, prev) from source."""
    dist: Dict[str, float] = {v: INF for v in graph.keys()}
    prev: Dict[str, Optional[str]] = {v: None for v in graph.keys()}
    dist[source] = 0.0
    pq: List[Tuple[float, str]] = [(0.0, source)]
    visited = set()

    while pq:
        d, u = heapq.heappop(pq)
        if u in visited:
            continue
        visited.add(u)
        for v, w in graph[u].items():
            nd = d + float(w)
            if nd < dist[v]:
                dist[v] = nd
                prev[v] = u
                heapq.heappush(pq, (nd, v))
    return dist, prev

def reconstruct_path(prev:Dict[str, Optional[str]], source:str, dest:str) -> List[str]:
    """Return path as [source, ..., dest] or [] if unreachable."""
    if source == dest:
        return [source]
    path: List[str] = []
    cur = dest
    while cur is not None:
        path.append(cur)
        if cur == source:
            break
        cur = prev.get(cur)
    path.reverse()
    if not path or path[0] != source:
        return []
    return path

def build_next_hop_table(graph:Dict[str, Dict[str, float]], source:str) -> Tuple[Dict[str, str], Dict[str, float], Dict[str, List[str]]]:
    """Compute next-hop per destination, distances, and full paths."""
    dist, prev = shortest_paths(graph, source)
    next_hop: Dict[str, str] = {}
    full_paths: Dict[str, List[str]] = {}
    for dest in graph.keys():
        if dest == source:
            continue
        path = reconstruct_path(prev, source, dest)
        full_paths[dest] = path
        if len(path) >= 2:
            next_hop[dest] = path[1]
    return next_hop, dist, full_paths
