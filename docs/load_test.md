# Prueba de carga: 5 peticiones concurrentes

- Fecha: 2026-10-12T21:41:49 · API: `http://localhost:8000` · duración total: 291.3 s
- Estados finales: {'DONE': 5} · pasaron por aprobación humana: 2
- POST /tasks (respuesta 202): p50 20.1 ms · p95 20.4 ms · máx 20.5 ms
- Latencia total por job (POST -> DONE, incluye la espera de aprobación): p50 238.4 s · p95 288.5 s
- Tiempo de cómputo por job (sin la espera humana): p50 236.4 s · p95 288.5 s

| # | job_id | crítica | estados vistos | total (s) | cómputo (s) | ruta del supervisor | publicado |
|---|---|---|---|---|---|---|---|
| 1 | `722c0eee` | no | PENDING → RUNNING → DONE | 155.83 | 155.83 | researcher → FINISH | - |
| 2 | `8c42b114` | no | PENDING → RUNNING → DONE | 277.24 | 277.24 | researcher → researcher → FINISH | - |
| 3 | `45e67ab9` | sí | PENDING → RUNNING → WAITING_APPROVAL → DONE | 238.38 | 236.37 | researcher → analyst → FINISH | pe7:reporte:45e67ab9a9614e92a8522fa594064f4b |
| 4 | `4dfe60c4` | no | PENDING → RUNNING → DONE | 291.34 | 291.34 | researcher → FINISH | - |
| 5 | `2256c2d6` | sí | PENDING → RUNNING → WAITING_APPROVAL → DONE | 190.41 | 188.38 | researcher → analyst → FINISH | pe7:reporte:2256c2d6666a43538c91df6d2e941cd0 |

Percentiles con interpolación lineal; con 5 muestras el p95 queda cerca del máximo.
