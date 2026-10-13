# Costo y latencia según Phoenix (proyecto `pe7-api-agente`)

- Leído de la API de Phoenix el 2026-10-12T21:55:13. Trazas: 7.
- Costo total: US$ 0.021361 · tokens: 52045
- Costo promedio por ejecución (job = corrida + reanudación): US$ 0.004272
- Latencia de las trazas: p50 186.3 s · **p95 286.7 s** · p99 290.2 s

## Por traza

| inicio (UTC) | traza | job | fase | latencia (s) | tokens in/out | costo (US$) |
|---|---|---|---|---|---|---|
| 00:36:57 | `0947eb8b` | `2256c2d6` | ejecutar | 186.33 | 6253/711 | 0.003273 |
| 00:36:57 | `9f52db5a` | `722c0eee` | ejecutar | 155.31 | 3675/322 | 0.001852 |
| 00:36:57 | `30535456` | `45e67ab9` | ejecutar | 235.02 | 9483/755 | 0.004364 |
| 00:36:57 | `b455ea69` | `4dfe60c4` | ejecutar | 291.05 | 6979/501 | 0.003176 |
| 00:36:57 | `fdb45079` | `8c42b114` | ejecutar | 276.42 | 22523/843 | 0.008696 |
| 00:40:07 | `ead77275` | `2256c2d6` | reanudar | 0.03 | 0/0 | 0.000000 |
| 00:40:55 | `563b194d` | `45e67ab9` | reanudar | 0.02 | 0/0 | 0.000000 |

## Por nodo del grafo (dónde se concentra la latencia y qué consume tokens)

| nodo | ejecuciones | latencia total del nodo (s) | llamadas LLM | tokens in/out | costo (US$) |
|---|---|---|---|---|---|
| researcher | 6 | 752.2 | 18 | 34326/647 | 0.011915 |
| supervisor | 14 | 271.5 | 14 | 8920/1625 | 0.006738 |
| synthesizer | 5 | 36.5 | 5 | 2532/598 | 0.001530 |
| analyst | 2 | 62.7 | 4 | 3135/262 | 0.001177 |
| evaluar_criticidad | 5 | 0.0 | 0 | 0/0 | 0.000000 |
| aprobacion_humana | 4 | 0.0 | 0 | 0/0 | 0.000000 |
| publicar_reporte | 2 | 0.0 | 0 | 0/0 | 0.000000 |
