import time
from typing import Optional
from utils import log, ExpiringSet, now_iso
from protocols import PROTO_FLOODING, TYPE_MESSAGE, TYPE_HELLO, TYPE_ECHO, TYPE_INFO, add_header

class RouterStrategy:
    def on_receive(self, node, msg:dict, incoming_neighbor:Optional[str]): ...
    def on_tick(self, node): ...

class FloodingRouter(RouterStrategy):
    def __init__(self, duplicate_ttl:int=120):
        self.seen = ExpiringSet(duplicate_ttl)

    def on_receive(self, node, msg:dict, incoming_neighbor:Optional[str]):
        # Duplicate / TTL checks
        mid = msg.get("id")
        if not self.seen.add_if_new(mid):
            return  # already processed

        ttl = msg.get("ttl", 0)
        if ttl <= 0:
            return  # expired

        mtype = msg.get("type")

        if mtype == TYPE_MESSAGE:
            # Deliver if for me
            if msg.get("to") == node.id:
                log(f"[DATA] from {msg['from']} to {node.id}: {msg.get('payload')} (id={mid})")
                return
            # else: forward to neighbors (except where it came from)
            self._forward_to_neighbors(node, msg, incoming_neighbor)

        elif mtype == TYPE_HELLO:
            # Reply with ECHO
            reply = {
                **msg,
                "type": TYPE_ECHO,
                "to": msg["from"],
                "from": node.id,
                "headers": {**msg.get("headers", {}), "echo_ts": now_iso()}
            }
            node.send_direct(msg["from"], reply)

        elif mtype == TYPE_ECHO:
            # RTT measurement handled at node level; just notify
            node.on_echo(msg)

        elif mtype == TYPE_INFO:
            # Not used heavily in flooding; could propagate if needed
            self._forward_to_neighbors(node, msg, incoming_neighbor)

    def _forward_to_neighbors(self, node, msg:dict, incoming_neighbor:Optional[str]):
        # decrement TTL and add last_hop
        msg = dict(msg)
        msg["ttl"] = msg.get("ttl", 0) - 1
        headers = dict(msg.get("headers") or {})
        headers["last_hop"] = node.id
        msg["headers"] = headers

        for nb in node.neighbors:
            if nb == incoming_neighbor:
                continue
            node.send_direct(nb, msg)

    def on_tick(self, node):
        # No periodic flooding behavior required.
        pass
