import argparse, threading, time
from typing import Optional
from utils import log, now_iso, pretty
from protocols import build_message, PROTO_DIJKSTRA, TYPE_MESSAGE, TYPE_HELLO
from config_loader import load_topology, load_names
from transport import JsonLineServer, send_json_line
from router_dijkstra import DijkstraRouter

class Node:
    def __init__(self, node_id:str, topo_path:str, names_path:str, default_ttl:int=8, hello_interval:float=10.0):
        self.id = node_id
        self.graph = load_topology(topo_path)   # weighted adjacency
        self.addr_map = load_names(names_path)
        self.host, self.port = self.addr_map[self.id]
        self.neighbors = list(self.graph.get(self.id, {}).keys())

        # Transport server
        self.server = JsonLineServer(self.host, self.port, handler=self._on_raw_message, name=f"server-{self.id}")

        # Routing strategy (Dijkstra)
        self.router = DijkstraRouter(self.graph)
        self.router.refresh_for(self.id)

        # Default TTL
        self.default_ttl = default_ttl

        # Hello/Ping (informativo)
        self.hello_interval = hello_interval
        self._hello_stop = threading.Event()
        self._hello_rtts = {}
        self._pending_pings = {}

    # Transport helpers
    def send_direct(self, neighbor_id:str, msg:dict):
        if neighbor_id not in self.addr_map:
            log(f"[warn] unknown neighbor {neighbor_id}")
            return
        host, port = self.addr_map[neighbor_id]
        try:
            send_json_line(host, port, msg)
        except Exception as e:
            log(f"[err] failed to send to {neighbor_id} at {host}:{port}: {e}")

    # Inbound
    def _on_raw_message(self, obj:dict, addr):
        incoming_last_hop = obj.get("headers", {}).get("last_hop")
        self.router.on_receive(self, obj, incoming_last_hop)

    def on_echo(self, msg:dict):
        mid = msg.get("id")
        if mid in self._pending_pings:
            t0, nb = self._pending_pings.pop(mid)
            rtt_ms = int((time.time() - t0) * 1000)
            self._hello_rtts[nb] = rtt_ms
            log(f"[RTT] {self.id} <-> {nb}: {rtt_ms} ms")

    # Threads
    def start(self):
        log(f"[node] {self.id} starting at {self.host}:{self.port} with neighbors={self.neighbors}")
        self.server.start()
        threading.Thread(target=self._hello_loop, name=f"hello-{self.id}", daemon=True).start()
        self._console_loop()

    def _hello_loop(self):
        # pequeño delay para que todos levanten
        time.sleep(1.0)
        while not self._hello_stop.is_set():
            for nb in self.neighbors:
                self._send_hello(nb)
                time.sleep(0.05)
            self._hello_stop.wait(self.hello_interval)

    def _send_hello(self, neighbor_id:str):
        msg = build_message(PROTO_DIJKSTRA, TYPE_HELLO, self.id, neighbor_id, ttl=2, payload="", headers={"ts": now_iso()})
        self._pending_pings[msg["id"]] = (time.time(), neighbor_id)
        self.send_direct(neighbor_id, msg)

    # Console
    def _console_loop(self):
        help_text = (
            "Commands:\n"
            "  send <DEST> <TEXT>   - send DATA via Dijkstra (next-hop)\n"
            "  table                - print routing table (next-hop, cost)\n"
            "  route <DEST>         - show full path to DEST\n"
            "  ping <NEIGHBOR>      - hello/echo\n"
            "  peers                - list neighbors and last RTT\n"
            "  ttl <N>              - set default TTL\n"
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
                self._send_data(dest, text)
            elif cmd == "table":
                self._print_table()
            elif cmd == "route" and len(parts) == 2:
                dest = parts[1]
                self._print_route(dest)
            elif cmd == "ping" and len(parts) == 2:
                nb = parts[1]
                self._send_hello(nb)
            elif cmd == "peers":
                self._print_peers()
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

    def _send_data(self, dest:str, text:str):
        if dest == self.id:
            log("[info] destination is self; printing locally: " + text)
            return
        nh = self.router.next_hop.get(dest)
        if not nh:
            log(f"[drop] no route from {self.id} to {dest}")
            return
        msg = build_message(PROTO_DIJKSTRA, TYPE_MESSAGE, self.id, dest, ttl=self.default_ttl, payload=text, headers={"ts": now_iso(), "last_hop": self.id})
        self.send_direct(nh, msg)

    def _print_table(self):
        log("Routing table (next-hop | cost):")
        for dest, nh in sorted(self.router.next_hop.items()):
            cost = self.router.dist.get(dest, "?")
            log(f"  {self.id} -> {dest} : next-hop={nh} cost={cost}")

    def _print_route(self, dest:str):
        path = self.router.paths.get(dest, [])
        if path:
            log(" -> ".join(path))
        else:
            log(f"[no-path] {self.id} to {dest}")

    def _print_peers(self):
        log(f"Neighbors of {self.id}: {self.neighbors}")
        for nb, rtt in self._hello_rtts.items():
            log(f"  {nb}: {rtt} ms")

    def _shutdown(self):
        log(f"[node] {self.id} shutting down...")
        self._hello_stop.set()
        try:
            self.server.stop()
        except Exception:
            pass

def main():
    import argparse
    ap = argparse.ArgumentParser(description="Dijkstra Node (Local Sockets)")
    ap.add_argument("--id", required=True, help="Node ID (e.g., A)")
    ap.add_argument("--topo", required=True, help="Path to topology JSON")
    ap.add_argument("--names", required=True, help="Path to names JSON (host:port per node)")
    ap.add_argument("--ttl", type=int, default=8, help="Default TTL for outgoing DATA")
    ap.add_argument("--hello", type=float, default=10.0, help="Hello interval seconds")
    args = ap.parse_args()

    node = Node(args.id, args.topo, args.names, default_ttl=args.ttl, hello_interval=args.hello)
    node.start()

if __name__ == "__main__":
    main()
