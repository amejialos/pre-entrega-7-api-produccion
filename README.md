# Pre-entrega 7: API del orquestador multi-agente en producción

API REST con **FastAPI** que expone el orquestador multi-agente del Módulo 6 (supervisor +
investigador + analista + síntesis, de la Pre-entrega 6) con:

- **Endpoints asíncronos**: `POST /tasks` encola y devuelve `202 Accepted` con un `job_id` en ~20 ms;
  el grafo corre en segundo plano y el cliente consulta `GET /tasks/{id}`.
- **Estado en Redis**: el estado de cada job (`PENDING → RUNNING → WAITING_APPROVAL → DONE | FAILED`),
  la cola del worker y los **checkpoints de LangGraph** (`AsyncRedisSaver`, `thread_id = job_id`).
- **Observabilidad**: cada corrida del grafo es una traza en **Arize Phoenix** vía OpenInference, con
  tokens, **costo por ejecución** y **latencia p95**.
- **Human-in-the-loop**: las tareas críticas (las que publican el reporte, una acción con efectos
  secundarios) se **pausan** con `interrupt()` hasta que alguien llama a `POST /tasks/{id}/approve`.
- **Prueba de carga**: 5 peticiones concurrentes reales contra Gemini + Pinecone, con capturas de
  Phoenix en [`screenshots/`](screenshots/).

> Resultado de la corrida real (12/10/2026): **5/5 jobs DONE**, 2 pasaron por aprobación humana y
> publicaron; costo promedio **US$ 0,004272 por ejecución**; latencia **p95 286,7 s** según Phoenix.
> El `POST /tasks` respondió en 20 ms (p95) mientras el grafo tardaba minutos.

---

## Índice

1. [Arquitectura](#1-arquitectura)
2. [Estructura del repo](#2-estructura-del-repo)
3. [Cómo levantarlo](#3-cómo-levantarlo)
4. [Cómo usar la API](#4-cómo-usar-la-api)
5. [Human-in-the-loop: qué es una tarea crítica](#5-human-in-the-loop-qué-es-una-tarea-crítica)
6. [Persistencia en Redis](#6-persistencia-en-redis)
7. [Observabilidad con Phoenix](#7-observabilidad-con-phoenix)
8. [Prueba de carga: 5 concurrentes, costo y p95](#8-prueba-de-carga-5-concurrentes-costo-y-p95)
9. [Tests](#9-tests)
10. [Decisiones, límites y próximos pasos](#10-decisiones-límites-y-próximos-pasos)
11. [Criterio → dónde se cumple](#11-criterio--dónde-se-cumple)

---

## 1. Arquitectura

```
 cliente                    API (FastAPI, async)                        Redis (redis:8)
 ───────                    ────────────────────                        ───────────────
 POST /tasks ───────────▶  valida body (Pydantic) ── HSET job PENDING ─▶ pe7:job:{id}
            ◀── 202 job_id  LPUSH ────────────────────────────────────▶ pe7:cola
                                                                          │ BLMOVE
                            Worker (5 tareas asyncio) ◀───────────────────┘
                              │ RUNNING
                              │ graph.ainvoke(..., thread_id=job_id) ───▶ checkpoint:{id}:*  (AsyncRedisSaver)
                              │   └─ trazas OpenInference ─────────────▶ Phoenix (localhost:6006)
                              ├─ terminó ............ DONE + resultado
                              ├─ interrupt() ........ WAITING_APPROVAL + qué aprobar
                              └─ excepción .......... FAILED + error
 GET /tasks/{id} ────────▶  HGETALL (no espera al grafo)
 POST /tasks/{id}/approve ▶ WAITING_APPROVAL → PENDING (atómico) + LPUSH reanudar
                              Worker: graph.ainvoke(Command(resume=decisión)) desde el checkpoint
```

El grafo es el de la PE6 con tres nodos nuevos al final ([`docs/grafo.mmd`](docs/grafo.mmd)):

```
START → supervisor ⇄ researcher / analyst → synthesizer → evaluar_criticidad
evaluar_criticidad ─(no crítica)→ END
                   ─(crítica)───→ aprobacion_humana ── interrupt() ── PAUSA hasta /approve
aprobacion_humana ─(aprobado)──→ publicar_reporte → END
                  ─(rechazado)─→ END  (la respuesta explica el rechazo)
```

**Por qué así:**

- **Nada sincrónico en los endpoints.** Solo hablan con Redis por el cliente `redis.asyncio` y encolan.
  La búsqueda de Pinecone/BM25 (sincrónica) corre en `asyncio.to_thread`, y el `RAGSystem` también se
  arma en un hilo al arrancar. Con 5 jobs corriendo, `/health` y los `GET` siguen respondiendo.
- **La cola vive en Redis, no en memoria.** `BLMOVE` pasa cada mensaje a `pe7:procesando` y se borra al
  terminar; si el proceso muere, al arrancar `recuperar()` lo devuelve a la cola, y como hay checkpoint
  el grafo **continúa** desde el último paso en vez de empezar de cero. Se pueden sumar workers en otros
  procesos (`uv run python -m app.worker` con `WORKER_EMBEBIDO=0` en la API).
- **Ningún error deja al cliente en polling infinito.** Todo `Worker.procesar` está envuelto en
  `try/except`: cualquier excepción deja el job en `FAILED` con `"Tipo: mensaje"`.
- **Inyección de dependencias** como en la PE6: `create_app(fabrica)` recibe Redis, grafo y worker;
  los tests le pasan `fakeredis`, `InMemorySaver` y LLM fake, sin red.

## 2. Estructura del repo

```
├── app/
│   ├── main.py            # FastAPI: POST /tasks (202 + job_id) · GET /tasks/{id} · GET /tasks · /health
│   ├── graph.py           # orquestador del M6 + nodos HITL + AsyncRedisSaver (redis_checkpointer)
│   ├── worker.py          # cola en Redis + consumidores asyncio: PENDING/RUNNING/WAITING_APPROVAL/DONE/FAILED
│   ├── observability.py   # phoenix.otel.register + LangChainInstrumentor (OpenInference)
│   ├── hitl.py            # criterio de tarea crítica, nodo con interrupt(), publicador y POST /tasks/{id}/approve
│   ├── jobs.py            # JobStore: hash del job, historial de eventos y transición atómica de aprobación
│   ├── state.py           # estado del orquestador (+ campos de HITL)
│   ├── providers.py       # modelo por rol, thought signatures de Gemini, limitador de RPM por modelo
│   ├── agents/            # supervisor (rúbrica Pydantic), investigador (RAG híbrido), analista (AST), síntesis
│   ├── rag_system.py, corpus.py, config.py   # RAG híbrido Pinecone + BM25 de la PE4
├── data/                  # documentación de la librería ficticia "Caudal" (corpus del RAG)
├── scripts/
│   ├── load_test.py       # 5 POST concurrentes + polling + aprobación automática + p50/p95
│   └── phoenix_report.py  # costo por ejecución, p95 y tokens por nodo leídos de la API de Phoenix
├── tests/                 # 60 tests sin red (fakeredis, InMemorySaver, LLM y buscador fake)
├── docs/                  # salidas de la carga, reporte de Phoenix, evidencia de Redis, demo de reinicio
├── screenshots/           # capturas del dashboard de Phoenix
├── docker-compose.yml     # redis:8 + Phoenix (+ la API con --profile app)
├── Dockerfile             # imagen de la API (uv, Python 3.12)
├── requirements.txt       # exportado de uv.lock (incluye pytest y fakeredis para correr los tests)
├── pyproject.toml / uv.lock
└── .env.example           # REDIS_URL, PHOENIX_COLLECTOR_ENDPOINT y claves (placeholders)
```

## 3. Cómo levantarlo

Requisitos: Docker (Docker Desktop), [uv](https://docs.astral.sh/uv/) y Python 3.12. Para correr el grafo
real: una key de Gemini (o de OpenAI) y el índice de Pinecone de la Pre-entrega 4.

```bash
# 1. Dependencias
uv sync                                  # o: pip install -r requirements.txt

# 2. Configuración
cp .env.example .env                     # completá PINECONE_API_KEY y OPENAI_API_KEY (+ OPENAI_BASE_URL si es Gemini)

# 3. Redis + Phoenix
docker compose up -d                     # redis:8 en :6379 y Phoenix en http://localhost:6006
docker compose ps                        # redis "healthy"

# 4. La API (con el worker embebido: 5 jobs a la vez)
uv run uvicorn app.main:app --port 8000
# Swagger: http://localhost:8000/docs   ·   Phoenix: http://localhost:6006
```

Alternativa todo en contenedores: `docker compose --profile app up -d --build` (la API toma el `.env`
y apunta sola a `redis` y `phoenix` dentro de la red del compose).

Variables nuevas respecto de la PE6 (todas en [`.env.example`](.env.example)): `REDIS_URL`,
`WORKER_CONCURRENCY`, `WORKER_EMBEBIDO`, `PHOENIX_COLLECTOR_ENDPOINT`, `PHOENIX_PROJECT_NAME`,
`TRACING_ENABLED`. `LLM_RPM` ahora limita **por modelo** (la cuota de Gemini se cuenta por modelo).

Para apagar: `docker compose down` (los volúmenes `redis-data` y `phoenix-data` quedan: los jobs, los
checkpoints y las trazas sobreviven; `down -v` los borra).

## 4. Cómo usar la API

```bash
# Crear una tarea (responde al instante con el job_id)
curl -s -X POST localhost:8000/tasks -H 'content-type: application/json' \
  -d '{"pregunta": "¿Cuál es el tamano_lote por defecto según la documentación de Caudal?"}'
# {"job_id":"722c0eee...","status":"PENDING","links":{"estado":"/tasks/722c0eee..."}}   (HTTP 202)

# Consultar el estado (polling)
curl -s localhost:8000/tasks/<job_id>
# {"status":"RUNNING", ...}  →  {"status":"DONE","resultado":{"respuesta": "...", "contribuciones": [...]}, "eventos":[...]}

# Tarea crítica: pide publicar el reporte → se pausa
curl -s -X POST localhost:8000/tasks -H 'content-type: application/json' \
  -d '{"pregunta": "¿Qué tamaño de lote se recomienda para ingesta masiva?", "publicar": true}'
curl -s localhost:8000/tasks/<job_id>
# {"status":"WAITING_APPROVAL","aprobacion_pendiente":{"accion":"publicar_reporte","motivos":[...],"borrador":"..."}}

# Aprobar (o rechazar con "aprobado": false)
curl -s -X POST localhost:8000/tasks/<job_id>/approve -H 'content-type: application/json' \
  -d '{"aprobado": true, "comentario": "ok", "revisor": "ana"}'
# 202 → el worker reanuda desde el checkpoint → DONE con "reporte_publicado": "pe7:reporte:<job_id>"
```

| Código | Cuándo |
|---|---|
| 202 | `POST /tasks` y `POST /tasks/{id}/approve` aceptados (el trabajo sigue en segundo plano) |
| 404 | job inexistente |
| 409 | `approve` sobre un job que no está en `WAITING_APPROVAL` (o doble aprobación simultánea) |
| 422 | body inválido: falta `pregunta`, menos de 10 o más de 2000 caracteres, solo espacios, `publicar` no booleano, `aprobado` faltante |

## 5. Human-in-the-loop: qué es una tarea crítica

**Criterio** ([`app/hitl.py`](app/hitl.py), `motivos_criticidad`): una tarea es crítica cuando implica
una acción con **efectos secundarios fuera del sistema**: publicar el reporte (`publicar_reporte` lo deja
en `pe7:reporte:{id}` y lo anuncia en `pe7:reportes_publicados`, de donde lo tomarían otros sistemas).
Se detecta de dos formas, y los motivos quedan en el estado, en la traza y en la respuesta de la API:

1. el cliente lo pide explícitamente: `"publicar": true`;
2. la pregunta lo pide en lenguaje natural: "publicá", "enviá", "notificá", "exportá" (y sus formas
   "publicar", "envialo"...). "Publicación" o "envío" como sustantivos **no** cuentan.

**Cómo pausa:** después de la síntesis, `evaluar_criticidad` decide la ruta. Si es crítica, el nodo
`aprobacion_humana` llama a `interrupt({...})`: LangGraph guarda el checkpoint en Redis y corta la
corrida; el worker lo detecta (`snapshot.tasks[*].interrupts`) y deja el job en `WAITING_APPROVAL` con el
borrador y los motivos. Nada avanza solo. `POST /tasks/{id}/approve` hace la transición atómica
`WAITING_APPROVAL → PENDING` (WATCH/MULTI en Redis: dos aprobaciones simultáneas no reanudan dos veces) y
encola `Command(resume={aprobado, comentario, revisor})`.

**Al reanudar**, LangGraph re-ejecuta `aprobacion_humana` desde el principio y esta vez `interrupt()`
devuelve la decisión. Por eso el nodo no tiene efectos antes del `interrupt()`: el efecto secundario
está en otro nodo (`publicar_reporte`) al que solo se llega aprobado. Si se rechaza, el flujo termina y la
respuesta empieza con *"La acción 'publicar_reporte' fue RECHAZADA por revisión humana (comentario: …).
El reporte NO se publicó"*.

## 6. Persistencia en Redis

| Clave | Qué guarda |
|---|---|
| `pe7:job:{id}` (hash) | `status`, `pregunta`, `publicar`, `created_at/started_at/updated_at/finished_at`, `resultado` o `error`, `aprobacion_pendiente`, `aprobacion` |
| `pe7:job:{id}:eventos` (lista) | cada transición de estado con su hora (`PENDING → RUNNING → WAITING_APPROVAL → PENDING → RUNNING → DONE`) |
| `pe7:jobs` (sorted set) | índice por fecha para `GET /tasks` |
| `pe7:cola`, `pe7:procesando` | la cola del worker (al menos una vez) |
| `checkpoint:{id}:*`, `checkpoint_write:*` | **checkpoints de LangGraph** (`AsyncRedisSaver`, módulos JSON + Search de `redis:8`; `asetup()` crea los índices) |
| `pe7:reporte:{id}`, `pe7:reportes_publicados` | el efecto secundario aprobado |

Los jobs vencen a la semana (TTL). Redis corre con AOF (`--appendonly yes`) y un volumen: los datos
sobreviven a reinicios del contenedor.

Evidencia real: [`docs/redis_evidencia.md`](docs/redis_evidencia.md) (111 claves de checkpoint después de
la carga, 23 de un solo hilo, índices `checkpoint` y `checkpoint_write`, el historial de eventos de un job
crítico) y [`docs/demo_reinicio.md`](docs/demo_reinicio.md): un job quedó en `WAITING_APPROVAL`, se apagó
la API **y se reinició el contenedor de Redis**; una API nueva lo vio pausado, se aprobó y terminó `DONE`
publicando, **con 0 llamadas al LLM** en el proceso nuevo: el estado salió del checkpoint.

## 7. Observabilidad con Phoenix

[`app/observability.py`](app/observability.py) registra un `TracerProvider` con `phoenix.otel.register`
(exportación en batch al colector OTLP de Phoenix) e instrumenta LangChain/LangGraph con
`LangChainInstrumentor` de OpenInference. Cada `ainvoke` del grafo es una traza con un span por nodo, por
llamada al LLM (modelo, tokens de entrada/salida, costo) y por herramienta (`buscar_documentacion`,
`calcular`, los retrievers de Pinecone y BM25).

- La traza se llama `orquestador job=<id>` y lleva `job_id` y `fase` (`ejecutar` / `reanudar`) en la
  metadata. OpenInference usa el `thread_id` como `session.id`: en la pestaña *Sessions* de Phoenix
  cada job es una sesión con su corrida inicial y su reanudación ([captura 07](screenshots/07_sesiones_por_job.png)).
- La pausa queda visible: el span `aprobacion_humana` tiene un evento `Interrupt(value={'accion':
  'publicar_reporte', ...})` y la traza termina ahí ([captura 03](screenshots/03_hitl_pausa_interrupt.png)).
- FastAPI 0.143 trae telemetría HTTP nativa que usa el `TracerProvider` global: **se apagó**
  (`telemetry={"tracing": False, ...}`) porque cada `GET` de polling era una traza más (en la primera
  prueba, un solo job generó 39 trazas HTTP además de las 2 del grafo) y distorsionaba la latencia p95 del proyecto.
- **Costo**: Phoenix trae precios para los modelos de Gemini usados (`gemini-flash-lite-latest`,
  `gemini-3.5-flash-lite`, `gemini-3.1-flash-lite`) en su tabla de modelos, así que no hizo falta cargar
  precios a mano: el costo sale de los tokens × precio y **no es 0**.

## 8. Prueba de carga: 5 concurrentes, costo y p95

```bash
docker compose up -d && uv run uvicorn app.main:app --port 8000     # en otra terminal
uv run python scripts/load_test.py                                   # 5 POST concurrentes contra :8000
uv run python scripts/phoenix_report.py                              # costo y p95 desde la API de Phoenix
```

`load_test.py` lanza los 5 `POST /tasks` a la vez (`asyncio.gather` con `httpx.AsyncClient`), hace polling
de cada job, **aprueba** por la API los que quedan en `WAITING_APPROVAL` y guarda
[`docs/load_test.md`](docs/load_test.md) / `.json`. Dos de las cinco tareas son críticas (una por
`publicar: true`, otra porque la pregunta dice "publicá el reporte").

Modelos (free tier de Gemini, repartidos por cuota): supervisor `gemini-flash-lite-latest`, investigador
`gemini-3.5-flash-lite`, analista y síntesis `gemini-3.1-flash-lite`; `LLM_RPM=4` por modelo.

**Resultados de la corrida real** (12/10/2026, 21:37):

| | valor |
|---|---|
| Jobs | **5/5 DONE**, 0 FAILED · 2 pasaron por `WAITING_APPROVAL` y se aprobaron vía endpoint |
| `POST /tasks` (202) | p50 20,1 ms · **p95 20,4 ms** (no bloquea) |
| Latencia por job, cliente (POST → DONE) | p50 238,4 s · p95 288,5 s |
| **Latencia de las trazas, Phoenix** | p50 186,3 s · **p95 286,66 s** · p99 290,2 s ([captura 06](screenshots/06_latencia_p95_phoenix.png)) |
| **Costo por ejecución, Phoenix** | promedio **US$ 0,004272** (entre 0,001852 y 0,008696) · total US$ 0,021361 · 52.045 tokens ([captura 05](screenshots/05_costo_por_ejecucion.png)) |

Detalle por traza y por nodo en [`docs/phoenix_report.md`](docs/phoenix_report.md). Phoenix cuenta 7 trazas:
las 5 corridas iniciales y las 2 reanudaciones tras aprobar (que duran ~0,03 s porque solo publican);
por eso su p50 es más bajo que el del cliente.

**Lectura del dashboard: dónde se concentra la latencia y qué consume tokens**

| nodo | llamadas LLM | tokens entrada / salida | costo | latencia total |
|---|---|---|---|---|
| researcher | 18 | 34.326 / 647 | US$ 0,011915 (56 %) | 752 s |
| supervisor | 14 | 8.920 / 1.625 | US$ 0,006738 | 272 s |
| synthesizer | 5 | 2.532 / 598 | US$ 0,001530 | 37 s |
| analyst | 4 | 3.135 / 262 | US$ 0,001177 | 63 s |

- **El investigador es el que más tokens consume** (70 % de los tokens de entrada): cada búsqueda le devuelve hasta
  5 fragmentos de ~900 caracteres que vuelven a entrar al modelo en cada vuelta del ReAct. El job del
  error CAU-417 hizo dos rondas de investigación y costó 4,7 veces lo que la consulta más simple.
  Recortar fragmentos o pasar a k=3 es la palanca de costo más directa.
- **La latencia no está en el modelo sino en la cuota.** Una llamada a `gemini-flash-lite-latest` tarda
  ~1 s, pero en la traza el span LLM del supervisor dura 31 s: con `LLM_RPM=4` cada modelo admite un pedido
  cada 15 s, y los 5 jobs comparten el limitador de cada modelo, así que hacen cola. Es el precio de no
  pasarse del free tier (sin el limitador aparecen 429). Con una cuota paga, bajar ese límite reduce el
  p95 casi linealmente.
- **Un error real quedó registrado y se recuperó solo**: en el job `8c42b114` Gemini devolvió un 500
  (`Internal error encountered`) en el supervisor; la `RetryPolicy` del nodo reintentó y el job terminó
  bien. En Phoenix se ve el span `supervisor` en rojo seguido del reintento exitoso
  ([captura 09](screenshots/09_traza_reintento_error_500.png)); es la traza marcada como *error* en el gráfico de volumen.

**Capturas** (Phoenix 20.21.1 en `localhost:6006`, tomadas con un Chromium headless propio vía Playwright):

| archivo | qué muestra |
|---|---|
| [01_trazas_lista.png](screenshots/01_trazas_lista.png) | las 7 trazas de la carga con latencia, tokens y costo por traza |
| [02_traza_abierta_nodos.png](screenshots/02_traza_abierta_nodos.png) | una traza abierta: supervisor → researcher → supervisor → analyst → supervisor → synthesizer → evaluar_criticidad → aprobacion_humana |
| [03_hitl_pausa_interrupt.png](screenshots/03_hitl_pausa_interrupt.png) | el evento `Interrupt` del nodo `aprobacion_humana` (la pausa) |
| [04_costo_metricas_phoenix.png](screenshots/04_costo_metricas_phoenix.png) | pestaña *Metrics*: costo estimado (prompt/completion), top de modelos por costo y por tokens, percentiles de latencia |
| [05_costo_por_ejecucion.png](screenshots/05_costo_por_ejecucion.png) | costo exacto por ejecución y p95 leídos de la API de Phoenix (la UI redondea a centavos y muestra "<$0.01") |
| [06_latencia_p95_phoenix.png](screenshots/06_latencia_p95_phoenix.png) | *Trace latency* con el tooltip de percentiles: **p95 286,66 s** |
| [07_sesiones_por_job.png](screenshots/07_sesiones_por_job.png) | 5 sesiones (una por job), 1,4 trazas por sesión |
| [08_span_llm_modelo_tokens.png](screenshots/08_span_llm_modelo_tokens.png) | un span LLM: modelo, tokens, costo, prompt y salida estructurada del supervisor |
| [09_traza_reintento_error_500.png](screenshots/09_traza_reintento_error_500.png) | el 500 de Gemini reintentado dentro de la traza |
| [10_api_swagger.png](screenshots/10_api_swagger.png) | los endpoints de la API en `/docs` |

## 9. Tests

```bash
uv run pytest -q          # 60 passed, sin red ni keys
```

| archivo | qué prueba |
|---|---|
| `tests/test_api.py` | 202 + `job_id` en <0,5 s con el grafo todavía corriendo; `PENDING → RUNNING → DONE` visto por polling; excepción en el worker → `FAILED` con `"ConnectionError: ..."` y el worker sigue vivo; 5 jobs en `RUNNING` a la vez mientras `/health` responde; 422 para 6 bodies inválidos; 404; el estado vive en Redis; mensajes huérfanos se reencolan |
| `tests/test_hitl.py` | el criterio de criticidad (8 casos); crítica → `WAITING_APPROVAL` sin publicar, sigue pausada sola, approve → `DONE` y publica una vez; reject → `DONE` sin publicar y con la explicación; no crítica no pausa; 409 por doble aprobación; 422 del body de aprobación; **otro worker con otra instancia del grafo reanuda desde el checkpoint sin llamar a ningún LLM** |
| `tests/test_redis_checkpoint.py` | lo mismo contra un **Redis real** con `AsyncRedisSaver` (se saltea si no hay Redis en `localhost:6379`) |
| `tests/test_graph.py`, `test_tools.py`, `test_providers.py` | los de la PE6 (rutas, rúbrica, tope de pasos, aislamiento de contexto, calculadora AST, thought signatures), adaptados |

Los tests usan `fakeredis` (estado y cola), `InMemorySaver` (checkpoints) y modelos fake; la app se arma
con `create_app(fabrica)` y se le habla por `httpx.ASGITransport` corriendo su `lifespan`.

## 10. Decisiones, límites y próximos pasos

- **Worker propio sobre Redis en vez de Celery/Arq**: el grafo ya es async; una cola en Redis con
  `BLMOVE` + lista de procesamiento da persistencia, reintento tras caída y escala horizontal
  (`python -m app.worker`) sin otra dependencia. Límite: no hay reintentos automáticos de un job
  `FAILED` (se reintenta creando otro) ni prioridad de colas.
- **Un job = un hilo** (`thread_id = job_id`): permite reanudar, auditar y borrar el hilo de un job.
- **Criterio de criticidad determinista** (flag + verbos), no decidido por el LLM: es testeable y no se
  puede "convencer" al modelo de saltear la aprobación.
- **Costo en la UI**: Phoenix muestra el costo redondeado a centavos ("<$0.01"); los valores exactos salen
  de su propia API GraphQL (`costSummary`) con `scripts/phoenix_report.py`.
- **Cuota gratis de Gemini**: ~20 pedidos por día por modelo; la carga gastó ~41 pedidos repartidos en
  3 modelos. Para la entrega final conviene una key paga o más modelos por rol.
- **Seguridad pendiente para producción**: la API no tiene autenticación (cualquiera podría aprobar);
  el endpoint de aprobación debería exigir un rol de revisor.

## 11. Criterio → dónde se cumple

| Criterio | Dónde se cumple (archivo:línea) | Evidencia |
|---|---|---|
| **Orquestación asíncrona y API (30 %)**: POST encola y devuelve `job_id` sin bloquear | `app/main.py:165-173` (`POST /tasks` → 202 + `Location`), `app/main.py:90` (body Pydantic), `app/worker.py:82` (encola en Redis), `app/worker.py:112` (consumidores asyncio), `app/worker.py:132-169` (`procesar`) | `tests/test_api.py:14`, `:50`; [docs/load_test.md](docs/load_test.md) (POST p95 20,4 ms); [10_api_swagger.png](screenshots/10_api_swagger.png) |
| GET refleja el estado sin bloquear | `app/main.py:175-181`, `app/jobs.py:35` (estados), `app/jobs.py:96` (transiciones con historial) | `tests/test_api.py:14` (`PENDING → RUNNING → DONE`) |
| Excepción en background → FAILED | `app/worker.py:166-169` | `tests/test_api.py:37` |
| **Persistencia con Redis (25 %)**: checkpoints de LangGraph | `app/graph.py:121-130` (`AsyncRedisSaver` + `asetup()`), `app/graph.py:96` (`compile(checkpointer=...)`), `app/graph.py:103-117` (`thread_id = job_id`), `app/main.py:64` | [docs/redis_evidencia.md](docs/redis_evidencia.md), [docs/demo_reinicio.md](docs/demo_reinicio.md), `tests/test_redis_checkpoint.py:36` |
| Estado de jobs en Redis | `app/jobs.py:65` (crear), `app/jobs.py:96` (actualizar), `app/jobs.py:119` (aprobación atómica), `docker-compose.yml:14-18` (`redis:8` + AOF) | `tests/test_api.py:90` |
| **Observabilidad (25 %)**: trazas en Phoenix | `app/observability.py:43-62` (`register` + `LangChainInstrumentor`), `app/main.py:58`, `app/graph.py:115-116` (nombre y metadata de la traza), `app/main.py:162` (sin trazas HTTP) | [01](screenshots/01_trazas_lista.png), [02](screenshots/02_traza_abierta_nodos.png), [07](screenshots/07_sesiones_por_job.png), [08](screenshots/08_span_llm_modelo_tokens.png), [09](screenshots/09_traza_reintento_error_500.png) |
| Costo por ejecución y p95 de las 5 concurrentes | `scripts/load_test.py`, `scripts/phoenix_report.py` | [04](screenshots/04_costo_metricas_phoenix.png), [05](screenshots/05_costo_por_ejecucion.png) (US$ 0,004272/ejecución), [06](screenshots/06_latencia_p95_phoenix.png) (p95 286,66 s), [docs/phoenix_report.md](docs/phoenix_report.md) |
| **Human-in-the-loop (15 %)**: nodo que detiene el flujo hasta una señal externa | `app/hitl.py:60` (criterio), `app/hitl.py:85-119` (`interrupt()` y `Command(goto=...)`), `app/hitl.py:157-173` (`POST /tasks/{id}/approve`), `app/graph.py:89-93` (nodos y arista), `app/worker.py:145` (`Command(resume=...)`), `app/worker.py:158` (`WAITING_APPROVAL`) | `tests/test_hitl.py:84`, `:119`, `:72`, `:167`; [03](screenshots/03_hitl_pausa_interrupt.png); 2 aprobaciones reales en [docs/load_test.md](docs/load_test.md) |
| **Documentación y estructura (5 %)** | este README (cómo levantar Redis y la API, cómo lanzar las 5 concurrentes), `requirements.txt`, `pyproject.toml`/`uv.lock`, `.env.example`, `docker-compose.yml`, `Dockerfile` | `uv run pytest -q` → 60 passed; `docker compose --profile app up -d --build` levanta la API en contenedor |
