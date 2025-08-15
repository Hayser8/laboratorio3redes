from utils import gen_id

PROTO_DIJKSTRA = "dijkstra"

TYPE_MESSAGE = "message"  # user data
TYPE_HELLO   = "hello"    # ping request
TYPE_ECHO    = "echo"     # ping response
TYPE_INFO    = "info"     # optional info packets

def build_message(proto:str, type_:str, from_id:str, to_id:str, ttl:int, payload, headers:dict|None=None, msg_id:str|None=None):
    return {
        "proto": proto,
        "type": type_,
        "from": from_id,
        "to": to_id,
        "ttl": ttl,
        "headers": headers or {},
        "payload": payload,
        "id": msg_id or gen_id(),
    }
