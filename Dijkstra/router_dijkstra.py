from typing import Optional
from utils import log
from protocols import TYPE_MESSAGE, TYPE_HELLO, TYPE_ECHO
from dijkstra import build_next_hop_table

class RouterStrategy:
    def on_receive(self, node, msg:dict, incoming_neighbor:Optional[str]): ...
    def on_tick(self, node): ...

class DijkstraRouter(RouterStrategy):
    def __init__(self, graph, source_id: Optional[str] = None):
        self.graph = graph
        self.next_hop = {}
        self.dist = {}
        self.paths = {}
        if source_id is not None:
            self.refresh_for(source_id)

    def refresh_for(self, node_id:str):
        self.next_hop, self.dist, self.paths = build_next_hop_table(self.graph, node_id)

    def on_receive(self, node, msg:dict, incoming_neighbor:Optional[str]):
        mtype = msg.get("type")
        ttl = msg.get("ttl", 0)

        if mtype == TYPE_MESSAGE:
            if msg.get("to") == node.id:
                log(f"[DATA] from {msg['from']} to {node.id}: {msg.get('payload')} (cost={self.dist.get(node.id,'?')})")
                return
            if ttl <= 0:
                return
            nh = self.next_hop.get(msg["to"])
            if not nh:
                log(f"[drop] no route from {node.id} to {msg['to']}")
                return
            # Prepare forward
            fwd = dict(msg)
            fwd["ttl"] = ttl - 1
            headers = dict(fwd.get("headers") or {})
            headers["last_hop"] = node.id
            fwd["headers"] = headers
            node.send_direct(nh, fwd)

        elif mtype == TYPE_HELLO:
            # Reply with echo
            reply = dict(msg)
            reply["type"] = TYPE_ECHO
            reply["to"] = msg["from"]
            reply["from"] = node.id
            node.send_direct(msg["from"], reply)

        elif mtype == TYPE_ECHO:
            node.on_echo(msg)

    def on_tick(self, node):
        pass

# Small indirection to avoid needing node before init
def node_id_placeholder():
    return "__SOURCE_PLACEHOLDER__"
