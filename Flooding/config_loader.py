import json
from utils import log

def load_topology(path:str) -> dict[str, list[str]]:
    with open(path, "r", encoding="utf-8") as f:
        obj = json.load(f)
    assert obj.get("type") == "topo", "topology file must have type=topo"
    config = obj["config"]
    # Normalize: ensure lists
    topo = {k: list(v) for k, v in config.items()}
    return topo

def load_names(path:str) -> dict[str, tuple[str,int]]:
    with open(path, "r", encoding="utf-8") as f:
        obj = json.load(f)
    assert obj.get("type") == "names", "names file must have type=names"
    mapping = {}
    for node_id, hostport in obj["config"].items():
        host, port_s = hostport.split(":")
        mapping[node_id] = (host, int(port_s))
    return mapping
