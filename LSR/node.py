import argparse, threading, time
from utils import log, now_iso, pretty
from protocols import build_message, PROTO_LSR, TYPE_MESSAGE, TYPE_HELLO
from config_loader import load_neighbors_only, load_names
from transport import JsonLineServer, send_json_line
from lsr import LSRRouter

class Node:
    def __init__(self, node_id:str, topo_path:str, names_path:str, metric:str='hop', default_ttl:int=8, hello_interval:float=5.0, lsp_interval:float=10.0, lsp_max_age:float=60.0):
        self.id = node_id
        self.addr_map = load_names(names_path)
        self.host, self.port = self.addr_map[self.id]
        self.neighbors = load_neighbors_only(topo_path, self.id)

        self.server = JsonLineServer(self.host, self.port, handler=self._on_raw_message, name=f"server-{self.id}")
        self.router = LSRRouter(self, metric=metric, lsp_interval=lsp_interval, max_age=lsp_max_age)

        self.default_ttl = default_ttl
        self.hello_interval = hello_interval
        self.lsp_interval = lsp_interval

        self._hello_stop = threading.Event()
        self._lsp_stop = threading.Event()
        self._hello_rtts = {}
        self._pending_pings = {}

    def send_direct(self, neighbor_id:str, msg:dict):
        if neighbor_id not in self.addr_map:
            log(f"[warn] unknown neighbor {neighbor_id}")
            return
        host, port = self.addr_map[neighbor_id]
        try:
            send_json_line(host, port, msg)
        except Exception as e:
            log(f"[err] failed to send to {neighbor_id} at {host}:{port}: {e}")

    def _on_raw_message(self, obj:dict, addr):
        incoming_last_hop = obj.get("headers", {}).get("last_hop")
        self.router.on_receive(obj, incoming_last_hop)

    def on_echo(self, msg:dict):
        mid = msg.get("id")
        if mid in self._pending_pings:
            t0, nb = self._pending_pings.pop(mid)
            rtt_ms = int((time.time() - t0) * 1000)
            self._hello_rtts[nb] = rtt_ms

    def last_rtt(self, neighbor_id:str):
        return self._hello_rtts.get(neighbor_id)

    def start(self):
        log(f"[node] {self.id} starting at {self.host}:{self.port} neighbors={self.neighbors}")
        self.server.start()
        time.sleep(1.0)
        threading.Thread(target=self._hello_loop, name=f"hello-{self.id}", daemon=True).start()
        threading.Thread(target=self._lsp_loop, name=f"lsp-{self.id}", daemon=True).start()
        self._console_loop()

    def _hello_loop(self):
        while not self._hello_stop.is_set():
            for nb in self.neighbors:
                self._send_hello(nb)
                time.sleep(0.05)
            self._hello_stop.wait(self.hello_interval)

    def _lsp_loop(self):
        time.sleep(1.0)
        self.router.originate_lsp()
        while not self._lsp_stop.is_set():
            self._lsp_stop.wait(self.lsp_interval)
            if self._lsp_stop.is_set():
                break
            self.router.originate_lsp()

    def _send_hello(self, neighbor_id:str):
        msg = build_message(PROTO_LSR, TYPE_HELLO, self.id, neighbor_id, ttl=2, payload="", headers={"ts": now_iso()})
        self._pending_pings[msg["id"]] = (time.time(), neighbor_id)
        self.send_direct(neighbor_id, msg)

    def _console_loop(self):
        help_text = (
            "Commands:\n"
            "  send <DEST> <TEXT>   - send DATA via LSR (next-hop)\n"
            "  table                - print routing table (next-hop, cost)\n"
            "  route <DEST>         - show full path to DEST\n"
            "  peers                - list neighbors and last RTT\n"
            "  ttl <N>              - set default TTL\n"
            "  lsdb                 - print LSDB\n"
            "  lsp                  - originate LSP now\n"
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
                self._print_route(parts[1])
            elif cmd == "peers":
                self._print_peers()
            elif cmd == "ttl" and len(parts) == 2:
                try:
                    self.default_ttl = int(parts[1])
                    log(f"default TTL set to {self.default_ttl}")
                except ValueError:
                    log("ttl must be an integer")
            elif cmd == "lsdb":
                self._print_lsdb()
            elif cmd == "lsp":
                self.router.originate_lsp()
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
        msg = build_message(PROTO_LSR, TYPE_MESSAGE, self.id, dest, ttl=self.default_ttl, payload=text, headers={"ts": now_iso(), "last_hop": self.id})
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

    def _print_lsdb(self):
        from utils import pretty
        log(pretty(self.router.lsdb.as_dict()))

    def _shutdown(self):
        log(f"[node] {self.id} shutting down...")
        self._hello_stop.set()
        self._lsp_stop.set()
        try:
            self.server.stop()
        except Exception:
            pass

def main():
    ap = argparse.ArgumentParser(description="LSR Node (Local Sockets)")
    ap.add_argument("--id", required=True, help="Node ID (e.g., A)")
    ap.add_argument("--topo", required=True, help="Path to topology JSON (neighbors only)")
    ap.add_argument("--names", required=True, help="Path to names JSON (host:port per node)")
    ap.add_argument("--metric", choices=["hop","rtt"], default="hop", help="Cost metric")
    ap.add_argument("--ttl", type=int, default=8, help="Default TTL for outgoing DATA")
    ap.add_argument("--hello", type=float, default=5.0, help="Hello interval seconds")
    ap.add_argument("--lsp", type=float, default=10.0, help="LSP origination interval seconds")
    ap.add_argument("--maxage", type=float, default=60.0, help="Max age seconds for LSPs in LSDB")
    args = ap.parse_args()

    node = Node(args.id, args.topo, args.names, metric=args.metric, default_ttl=args.ttl, hello_interval=args.hello, lsp_interval=args.lsp, lsp_max_age=args.maxage)
    node.start()

if __name__ == "__main__":
    main()
