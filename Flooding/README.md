# Flooding Routing (Local Sockets Demo)

Minimal, modular implementation of **Flooding** routing using local TCP sockets (one process per node).  
Designed to match your lab's JSON protocol and thread split (Forwarding vs Routing).

## Features
- JSON message format: `proto`, `type`, `from`, `to`, `ttl`, `headers`, `payload`, `id`
- Two concurrent services:
  - **Forwarding**: accepts inbound packets, decrements TTL, suppresses duplicates, prints DATA for self, forwards to neighbors.
  - **Routing**: periodic `hello` pings (RTT measurement) and optional info logs.
- Duplicate suppression by `id` with time-based expiry.
- Config-driven neighbors and ports (no global topology knowledge beyond neighbors).
- Clean separation: `node.py` (process), `flooding.py` (algorithm), `protocols.py` (message schema), `transport.py`, `config_loader.py`, `utils.py`.

## Requirements
- Python 3.10+ (standard library only). No external dependencies.

## Files
- `node.py` — entrypoint to run a node
- `flooding.py` — Flooding algorithm logic (duplicate suppression, TTL handling)
- `protocols.py` — message schema and builders
- `transport.py` — TCP server/client helpers (JSON Lines framing)
- `config_loader.py` — parse topology and names configs
- `utils.py` — logging, timing, ID helpers
- `topo-sample.json` — sample neighbor map
- `names-sample.json` — sample address map (127.0.0.1:port per node)

## JSON Protocol (example)
```json
{
  "proto":   "flooding",
  "type":    "message|hello|echo|info",
  "from":    "A",
  "to":      "C",
  "ttl":     6,
  "headers": {"last_hop":"A","ts":"2025-08-14T20:00:00Z"},
  "payload": "Hola mundo (payload libre)",
  "id":      "b8b0bb41-9a0d-4f9c-ae12-135f8a0b6c88"
}
```

## Quick Start (3 terminals)
1) **Terminal 1**
```bash
cd flooding-routing
python3 node.py --id A --topo topo-sample.json --names names-sample.json
py node.py --id A --topo topo-sample.json --names names-sample.json

```
2) **Terminal 2**
```bash
cd flooding-routing
python3 node.py --id B --topo topo-sample.json --names names-sample.json
py node.py --id B --topo topo-sample.json --names names-sample.json

```
3) **Terminal 3**
```bash
cd flooding-routing
python3 node.py --id C --topo topo-sample.json --names names-sample.json
py node.py --id C --topo topo-sample.json --names names-sample.json

- Clase
.\.venv\Scripts\Activate.ps1
python .\node.py --id C --topo .\topo-compat.json --names .\names-redis.json
```

### Send a message (from any node's interactive prompt)
```
> send C Hola C, te llega?
```
You should see the DATA printed at node C. Intermediate nodes forward (flood) to all neighbors except the incoming one, TTL is decremented, and duplicates are suppressed.

### Other commands
```
> ping B           # hello/echo RTT to neighbor B
> peers            # list known neighbors and last RTT
> ttl 8            # change default TTL for *new* outgoing DATA messages
> help             # list commands
> quit             # exit node
```

## Notes for the Lab
- **Neighbors-only knowledge**: Each node reads only its immediate neighbors from `topo-*.json`.
- **Two threads**: Forwarding server thread + Routing (hello/ping loop). Console runs in main thread.
- **Flooding only**: The router is pluggable (`RouterStrategy` interface). You can add Dijkstra/LSR/DVR later and keep the node skeleton intact.
- **No XMPP yet**: This version uses local TCP sockets for quick, offline testing. Later you can replace `transport.py` with an XMPP transport layer; upper layers remain the same.

---

## Topology & Names format
`topo-sample.json`
```json
{
  "type": "topo",
  "config": {
    "A": ["B","C"],
    "B": ["A","C"],
    "C": ["A","B"]
  }
}
```
`names-sample.json`
```json
{
  "type": "names",
  "config": {
    "A": "127.0.0.1:5001",
    "B": "127.0.0.1:5002",
    "C": "127.0.0.1:5003"
  }
}
```

---

## Implementation Notes
- Messages are framed as JSON Lines over TCP (one JSON object per line).
- Duplicate suppression uses a set with time-based eviction (default 120 seconds) keyed by `id`.
- A `last_hop` header is added by the forwarder to help neighbors avoid immediate bounce-back.
- For flooding, forwarding rule is: forward to `all_neighbors - {incoming_neighbor}` if `ttl > 0` and msg not seen.
- Hello/Echo: routing thread periodically sends `hello` to neighbors; the receiver replies `echo` with same `id`, sender computes RTT.
- The node prints **DATA** when `msg["to"] == self.id` (destination reached).

Happy testing!
