"""La API: 202 + job_id sin esperar al grafo, polling PENDING -> RUNNING -> DONE, FAILED ante
una excepción, validación del body y concurrencia."""

import asyncio
import json
import time

import pytest

from app.jobs import Estado
from tests.conftest import grafo_simple, levantar_api


async def test_post_devuelve_202_y_job_id_sin_esperar_al_grafo():
    compuerta = asyncio.Event()  # el grafo no termina hasta que la abramos
    async with levantar_api(grafo_simple(compuerta)) as api:
        inicio = time.monotonic()
        r = await api.client.post("/tasks", json={"pregunta": "¿Cuál es el tamano_lote por defecto?"})
        demora = time.monotonic() - inicio

        assert r.status_code == 202
        cuerpo = r.json()
        assert len(cuerpo["job_id"]) == 32 and cuerpo["status"] == "PENDING"
        assert r.headers["location"] == f"/tasks/{cuerpo['job_id']}"
        assert demora < 0.5  # respondió con el grafo todavía corriendo

        job = await api.esperar(cuerpo["job_id"], "RUNNING")
        assert job["started_at"] and job["resultado"] is None

        compuerta.set()
        job = await api.esperar(cuerpo["job_id"], "DONE")
        assert job["resultado"]["respuesta"] == "respuesta a: ¿Cuál es el tamano_lote por defecto?"
        assert [e["status"] for e in job["eventos"]] == ["PENDING", "RUNNING", "DONE"]
        assert job["finished_at"] >= job["started_at"] >= job["created_at"]


async def test_excepcion_en_el_worker_deja_el_job_failed_con_el_error():
    async with levantar_api(grafo_simple(falla=ConnectionError("Pinecone no responde"))) as api:
        job_id = await api.crear()
        job = await api.esperar(job_id, "FAILED", "DONE")

        assert job["status"] == "FAILED"
        assert job["error"] == "ConnectionError: Pinecone no responde"
        assert job["finished_at"]
        assert [e["status"] for e in job["eventos"]] == ["PENDING", "RUNNING", "FAILED"]
        # El worker sigue vivo después del error: el siguiente job se procesa normalmente.
        assert api.worker._tareas and not any(t.done() for t in api.worker._tareas)


async def test_cinco_jobs_corren_en_paralelo_sin_bloquear_la_api():
    compuerta = asyncio.Event()
    async with levantar_api(grafo_simple(compuerta), concurrencia=5) as api:
        ids = await asyncio.gather(*(api.crear(f"Pregunta número {i} sobre Caudal") for i in range(5)))
        # Los 5 llegan a RUNNING a la vez (ninguno espera a que termine otro)...
        await asyncio.gather(*(api.esperar(i, "RUNNING") for i in ids))
        # ...y mientras tanto la API sigue respondiendo.
        assert (await api.client.get("/health")).status_code == 200
        compuerta.set()
        jobs = await asyncio.gather(*(api.esperar(i, "DONE") for i in ids))
        assert {j["resultado"]["respuesta"] for j in jobs} == {f"respuesta a: Pregunta número {i} sobre Caudal" for i in range(5)}
        listado = (await api.client.get("/tasks", params={"limite": 10})).json()
        assert {j["job_id"] for j in listado} == set(ids)


@pytest.mark.parametrize(
    "body",
    [
        {},  # falta la pregunta
        {"pregunta": "corta"},  # menos de 10 caracteres
        {"pregunta": " " * 20},  # solo espacios
        {"pregunta": "x" * 2001},  # demasiado larga
        {"pregunta": "¿Cuál es el tamano_lote?", "publicar": "quizás"},  # no es booleano
        {"pregunta": 123456789012},  # no es texto
    ],
)
async def test_body_invalido_responde_422_y_no_crea_jobs(body):
    async with levantar_api(grafo_simple()) as api:
        r = await api.client.post("/tasks", json=body)
        assert r.status_code == 422
        assert await api.store.listar() == []


async def test_job_inexistente_responde_404():
    async with levantar_api(grafo_simple()) as api:
        assert (await api.client.get("/tasks/no-existe")).status_code == 404
        r = await api.client.post("/tasks/no-existe/approve", json={"aprobado": True})
        assert r.status_code == 404


async def test_el_estado_del_job_vive_en_redis():
    compuerta = asyncio.Event()
    async with levantar_api(grafo_simple(compuerta)) as api:
        job_id = await api.crear()
        await api.esperar(job_id, "RUNNING")
        crudo = await api.redis.hgetall(f"pe7:job:{job_id}")
        assert crudo["status"] == Estado.RUNNING and crudo["pregunta"].startswith("¿Cuál")
        compuerta.set()
        await api.esperar(job_id, "DONE")
        assert json.loads(await api.redis.hget(f"pe7:job:{job_id}", "resultado"))["task_completed"] is True


async def test_recuperar_reencola_mensajes_que_quedaron_a_medio_procesar():
    async with levantar_api(grafo_simple()) as api:
        await api.worker.detener()  # simulamos que el proceso murió con un mensaje tomado
        job = await api.store.crear("¿Cuál es el tamano_lote por defecto?")
        await api.redis.lpush("pe7:procesando", json.dumps({"tipo": "ejecutar", "job_id": job["job_id"]}))

        await api.worker.iniciar()  # al arrancar, recuperar() lo devuelve a la cola

        terminado = await api.esperar(job["job_id"], "DONE")
        assert terminado["resultado"]["task_completed"] is True
        assert await api.redis.llen("pe7:procesando") == 0
