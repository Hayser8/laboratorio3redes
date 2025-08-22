import json

def load_neighbors_only(path:str, self_id:str) -> list[str]:
    with open(path, "r", encoding="utf-8") as f:
        obj = json.load(f)
    assert obj.get("type") == "topo", "topology file must have type=topo"
    cfg = obj["config"]
    neigh = cfg.get(self_id, [])
    if isinstance(neigh, dict):
        return list(neigh.keys())
    return list(neigh)

def load_names(path:str) -> dict[str, tuple[str,int]]:
    with open(path, "r", encoding="utf-8") as f:
        obj = json.load(f)
    assert obj.get("type") == "names", "names file must have type=names"
    mapping = {}
    for node_id, hostport in obj["config"].items():
        host, port_s = hostport.split(":")
        mapping[node_id] = (host, int(port_s))
    return mapping
