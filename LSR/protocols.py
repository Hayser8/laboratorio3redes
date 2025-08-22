from utils import gen_id

PROTO_LSR = "lsr"

TYPE_MESSAGE = "message"
TYPE_HELLO   = "hello"
TYPE_ECHO    = "echo"
TYPE_LSP     = "lsp"
TYPE_INFO    = "info"

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
