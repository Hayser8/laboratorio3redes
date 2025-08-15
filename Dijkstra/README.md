# Dijkstra Routing (Local Sockets Demo)

Implementación **modular** de **Dijkstra** (camino más corto) sobre **sockets TCP locales**.
Usa el mismo protocolo JSON que el demo de Flooding y mantiene la separación en hilos:
- **Forwarding**: recibe paquetes, decrementa TTL, reenvía al **siguiente salto** según la tabla.
- **Routing (LS init)**: calcula la **tabla de ruteo** con Dijkstra a partir de la topología (pesos opcionales).

> **Nota del lab:** Para *Dijkstra puro* se permite construir la topología desde archivo y calcular rutas estáticas. Este demo **no** propaga LSAs ni recalcula dinámicamente (eso sería LSR).

## Protocolo JSON (igual al anterior)
```json
{
  "proto":   "dijkstra",
  "type":    "message|hello|echo|info",
  "from":    "A",
  "to":      "C",
  "ttl":     8,
  "headers": {"last_hop":"A","ts":"2025-08-14T20:00:00Z"},
  "payload": "Hola mundo",
  "id":      "uuid-..."
}
```

## Topología y pesos
- `topo-*.json` soporta **dos formatos**:
  1) **Listas** (peso 1 por defecto)
  ```json
  { "type": "topo", "config": { "A": ["B","C"], "B": ["A"], "C": ["A"] } }
  ```
  2) **Diccionarios** con pesos (enteros o flotantes)
  ```json
  { "type": "topo", "config": { "A": {"B":1, "C":3.5}, "B": {"A":1}, "C": {"A":3.5} } }
  ```
- El grafo se trata como **no dirigido**. Si hay asimetrías, se usa el peso tal cual por cada dirección.

## Archivos
- `node.py` — proceso de nodo (servidor TCP + consola)
- `dijkstra.py` — cálculo del SPT y **tabla next-hop**
- `router_dijkstra.py` — estrategia `RouterStrategy` para forwarding con next-hop
- `protocols.py`, `transport.py`, `config_loader.py`, `utils.py` — utilitarios
- `topo-sample.json`, `names-sample.json` — ejemplos

## Requisitos
Python 3.10+ (stdlib).

## Cómo correr (3 terminales)
1) Terminal 1
```bash
cd dijkstra-routing
python3 node.py --id A --topo topo-sample.json --names names-sample.json
py node.py --id A --topo topo-sample.json --names names-sample.json
```
2) Terminal 2
```bash
cd dijkstra-routing
python3 node.py --id B --topo topo-sample.json --names names-sample.json
py node.py --id B --topo topo-sample.json --names names-sample.json

```
3) Terminal 3
```bash
cd dijkstra-routing
python3 node.py --id C --topo topo-sample.json --names names-sample.json
py node.py --id C --topo topo-sample.json --names names-sample.json

```

### Probar envío
En cualquiera:
```
> send C Hola C, ¿me copias?
```
El nodo destino imprime el **DATA**. Los intermedios **reenvían solo al siguiente salto**.

### Comandos
```
> table          # ver tabla de enrutamiento (siguiente salto y costo)
> route C        # ver la ruta completa A->...->C
> send <D> <msg> # envía DATA
> ping <N>       # hello/echo (mide RTT, no afecta pesos)
> peers          # muestra vecinos y último RTT
> ttl <N>        # TTL por defecto para nuevos mensajes
> help, quit
```

## Ejemplo de topología y puertos
`topo-sample.json`
```json
{
  "type": "topo",
  "config": {
    "A": ["B", "C"],
    "B": ["A", "C"],
    "C": ["A", "B"]
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

## Detalles de implementación
- Se construye un grafo `dict[str, dict[str, float]]` a partir del `topo`.
- `dijkstra.shortest_paths(source)` devuelve distancias y predecesores.
- `build_next_hop_table(source)` usa predecesores para obtener el **primer salto** por destino.
- Forwarding: si `msg["to"] != self.id`, se reenvía a `next_hop[to]` (si existe) y se **decrementa TTL**. Si no hay ruta, se descarta.
- Se añade `headers.last_hop` para trazabilidad.
- Hello/Echo corre en paralelo (informativo).

¡Listo para que luego cambies `transport.py` por XMPP y mantengas las capas superiores!
