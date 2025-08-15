import argparse, threading, time
from typing import Optional
from utils import log, now_iso
from protocols import build_message, PROTO_FLOODING, TYPE_MESSAGE, TYPE_HELLO, TYPE_ECHO, TYPE_INFO
from config_loader import load_topology, load_names
from transport import JsonLineServer, send_json_line
from flooding import FloodingRouter

class Node:
    def __init__(self, node_id:str, topo_path:str, names_path:str, default_ttl:int=6, hello_interval:float=10.0):
        self.id = node_id
        self.topo = load_topology(topo_path)
        self.addr_map = load_names(names_path)
        self.host, self.port = self.addr_map[self.id]

        # neighbors knowledge only
        self.neighbors = list(self.topo.get(self.id, []))

        # Transport server
        self.server = JsonLineServer(self.host, self.port, handler=self._on_raw_message, name=f"server-{self.id}")

        # Routing strategy (Flooding only for now)
        self.router = FloodingRouter()

        # Default TTL for new outgoing messages
        self.default_ttl = default_ttl

        # Hello/Ping
        self.hello_interval = hello_interval
        self._hello_stop = threading.Event()
        self._hello_rtts = {}  # neighbor -> last RTT in ms
        self._pending_pings = {}  # id -> send_time, neighbor

    # === Transport helpers ===
    def send_direct(self, neighbor_id:str, msg:dict):
        if neighbor_id not in self.addr_map:
            log(f"[warn] unknown neighbor {neighbor_id}")
            return
        host, port = self.addr_map[neighbor_id]
        try:
            send_json_line(host, port, msg)
            # log(f"[tx] {self.id} -> {neighbor_id} type={msg['type']} ttl={msg.get('ttl')} id={msg.get('id')}")
        except Exception as e:
            log(f"[err] failed to send to {neighbor_id} at {host}:{port}: {e}")

    def broadcast(self, msg:dict, exclude:Optional[str]=None):
        for nb in self.neighbors:
            if nb == exclude:
                continue
            self.send_direct(nb, msg)

    # === Inbound handling ===
    def _on_raw_message(self, obj:dict, addr):
        incoming_last_hop = obj.get("headers", {}).get("last_hop")
        # If incoming_last_hop not given, best guess from addr? We keep it optional.
        self.router.on_receive(self, obj, incoming_last_hop)

    # Called by router when ECHO arrives
    def on_echo(self, msg:dict):
        mid = msg.get("id")
        if mid in self._pending_pings:
            t0, nb = self._pending_pings.pop(mid)
            rtt_ms = int((time.time() - t0) * 1000)
            self._hello_rtts[nb] = rtt_ms
            log(f"[RTT] {self.id} <-> {nb}: {rtt_ms} ms")

    # === Threads ===
    def start(self):
        log(f"[node] {self.id} starting at {self.host}:{self.port} with neighbors={self.neighbors}")
        self.server.start()
        threading.Thread(target=self._hello_loop, name=f"hello-{self.id}", daemon=True).start()
        self._console_loop()

    def _hello_loop(self):
        while not self._hello_stop.is_set():
            for nb in self.neighbors:
                self._send_hello(nb)
                time.sleep(0.05)
            self._hello_stop.wait(self.hello_interval)

    def _send_hello(self, neighbor_id:str):
        msg = build_message(PROTO_FLOODING, TYPE_HELLO, self.id, neighbor_id, ttl=2, payload="", headers={"ts": now_iso()})
        self._pending_pings[msg["id"]] = (time.time(), neighbor_id)
        self.send_direct(neighbor_id, msg)

    # === Console ===
    def _console_loop(self):
        help_text = (
            "Commands:\n"
            "  send <DEST> <TEXT>   - send DATA to DEST via flooding\n"
            "  ping <NEIGHBOR>      - hello/echo to neighbor\n"
            "  peers                - list neighbors and last RTT\n"
            "  ttl <N>              - set default TTL for outgoing DATA\n"
            "  help                 - show help\n"
            "  quit                 - exit\n"
        )
        log(help_text)
        while True:
            try:
                raw = input(f"[{self.id}]> ").strip()
            except (EOFError, KeyboardInterrupt):
                break
            if not raw:
                continue
            parts = raw.split()
            cmd = parts[0].lower()
            if cmd == "send" and len(parts) >= 3:
                dest = parts[1]
                text = " ".join(parts[2:])
                msg = build_message(PROTO_FLOODING, TYPE_MESSAGE, self.id, dest, ttl=self.default_ttl, payload=text, headers={"ts": now_iso()})
                # initial send — broadcast to all neighbors
                self.broadcast(msg)
            elif cmd == "ping" and len(parts) == 2:
                nb = parts[1]
                if nb not in self.neighbors:
                    log(f"[warn] {nb} is not a neighbor of {self.id}")
                else:
                    self._send_hello(nb)
            elif cmd == "peers":
                log(f"Neighbors of {self.id}: {self.neighbors}")
                for nb, rtt in self._hello_rtts.items():
                    log(f"  {nb}: {rtt} ms")
            elif cmd == "ttl" and len(parts) == 2:
                try:
                    self.default_ttl = int(parts[1])
                    log(f"default TTL set to {self.default_ttl}")
                except ValueError:
                    log("ttl must be an integer")
            elif cmd == "help":
                log(help_text)
            elif cmd == "quit":
                break
            else:
                log("Unknown command. Type 'help'.")
        self._shutdown()

    def _shutdown(self):
        log(f"[node] {self.id} shutting down...")
        self._hello_stop.set()
        try:
            self.server.stop()
        except Exception:
            pass

def main():
    ap = argparse.ArgumentParser(description="Flooding Node (Local Sockets)")
    ap.add_argument("--id", required=True, help="Node ID (e.g., A)")
    ap.add_argument("--topo", required=True, help="Path to topology JSON")
    ap.add_argument("--names", required=True, help="Path to names JSON (host:port per node)")
    ap.add_argument("--ttl", type=int, default=6, help="Default TTL for outgoing DATA")
    ap.add_argument("--hello", type=float, default=10.0, help="Hello interval seconds")
    args = ap.parse_args()

    node = Node(args.id, args.topo, args.names, default_ttl=args.ttl, hello_interval=args.hello)
    node.start()

if __name__ == "__main__":
    main()
