# Link State Routing (LSR) — Local Sockets Demo

Implementación **modular** de Link-State Routing (OSPF-like) sobre **sockets TCP locales**,
siguiendo el formato JSON del laboratorio. Incluye:

- **LSDB** (Link-State Database) basada en **LSPs** (Link-State Packets)
- **Flooding** de LSPs con `seq` por origen y supresión de duplicados
- **Dijkstra (SPF)** para construir tabla de enrutamiento (**next-hop**)
- Hilos separados:
  - **Forwarding**: recibe paquetes y reenvía (DATA por next-hop; LSP por flooding)
  - **Routing**: *hello/echo* (RTT), *origination* periódica de LSP, y recomputo SPF
- Métrica seleccionable: **hop** (peso=1) o **rtt** (último RTT ms hacia vecino)

## Ejecutar (3 terminales)
1) Terminal A
```bash
cd lsr-routing
python3 node.py --id A --topo topo-sample.json --names names-sample.json --metric hop
```
2) Terminal B
```bash
cd lsr-routing
python3 node.py --id B --topo topo-sample.json --names names-sample.json --metric hop
```
3) Terminal C
```bash
cd lsr-routing
python3 node.py --id C --topo topo-sample.json --names names-sample.json --metric hop
```

- Clase
```bash
python node.py --id A --topo topo.json --names names-redis.json --debug

```
### Comandos
```
send <DEST> <TEXTO>
table
route <DEST>
peers
ttl <N>
lsdb
lsp
help
quit
```
