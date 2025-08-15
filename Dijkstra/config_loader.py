import json

def load_topology(path:str) -> dict[str, dict[str, float]]:
    """Load topology into weighted adjacency: node -> {neighbor: weight}.    Accepts list form (weight=1) or dict form (explicit weights)."""
    with open(path, "r", encoding="utf-8") as f:
        obj = json.load(f)
    assert obj.get("type") == "topo", "topology file must have type=topo"
    cfg = obj["config"]
    graph: dict[str, dict[str, float]] = {}
    # normalize
    for node, neigh in cfg.items():
        if isinstance(neigh, dict):
            graph[node] = {str(k): float(v) for k, v in neigh.items()}
        else:
            graph[node] = {str(k): 1.0 for k in neigh}
    # ensure undirected edges exist (if missing)
    for u, nbrs in list(graph.items()):
        for v, w in nbrs.items():
            graph.setdefault(v, {})
            graph[v].setdefault(u, w)
    return graph

def load_names(path:str) -> dict[str, tuple[str,int]]:
    with open(path, "r", encoding="utf-8") as f:
        obj = json.load(f)
    assert obj.get("type") == "names", "names file must have type=names"
    mapping = {}
    for node_id, hostport in obj["config"].items():
        host, port_s = hostport.split(":")
        mapping[node_id] = (host, int(port_s))
    return mapping
