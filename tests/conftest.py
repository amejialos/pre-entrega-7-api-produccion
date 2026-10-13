"""Fixtures de la API sin red: fakeredis (estado de jobs y cola), `InMemorySaver` (checkpoints),
LLM y buscador fake. La app se arma con `create_app(fabrica)` y se le habla con httpx por ASGI."""

import asyncio
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import httpx
import pytest
from fakeredis import FakeAsyncRedis
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph

from app.jobs import JobStore
from app.main import Recursos, create_app
from app.state import OrchestratorState
from app.worker import Worker


class PublicadorFake:
    """Registra cada publicación en vez de tener efectos reales."""

    def __init__(self) -> None:
        self.publicados: list[tuple[str, dict[str, Any]]] = []

    async def __call__(self, job_id: str, reporte: dict[str, Any]) -> str:
        self.publicados.append((job_id, reporte))
        return f"fake:reporte:{job_id}"


def grafo_simple(compuerta: asyncio.Event | None = None, falla: Exception | None = None):
    """Grafo de un nodo: espera la `compuerta` (simula una corrida larga) o levanta `falla`."""

    async def trabajo(state: OrchestratorState) -> dict[str, Any]:
        if compuerta is not None:
            await compuerta.wait()
        if falla is not None:
            raise falla
        return {"final_answer": f"respuesta a: {state['pregunta']}", "task_completed": True}

    builder = StateGraph(OrchestratorState)
    builder.add_node("trabajo", trabajo)
    builder.add_edge(START, "trabajo")
    builder.add_edge("trabajo", END)
    return builder.compile(checkpointer=InMemorySaver())


class Entorno:
    def __init__(self, client: httpx.AsyncClient, store: JobStore, worker: Worker, redis: FakeAsyncRedis) -> None:
        self.client = client
        self.store = store
        self.worker = worker
        self.redis = redis

    async def crear(self, pregunta: str = "¿Cuál es el tamano_lote por defecto?", **extra: Any) -> str:
        r = await self.client.post("/tasks", json={"pregunta": pregunta, **extra})
        assert r.status_code == 202, r.text
        return r.json()["job_id"]

    async def esperar(self, job_id: str, *estados: str, timeout: float = 5.0) -> dict[str, Any]:
        """Polling como haría un cliente real, hasta que el job llegue a uno de `estados`."""
        limite = time.monotonic() + timeout
        while True:
            job = (await self.client.get(f"/tasks/{job_id}")).json()
            if job["status"] in estados:
                return job
            if time.monotonic() > limite:
                raise AssertionError(f"El job {job_id} quedó en {job['status']}, esperaba {estados}")
            await asyncio.sleep(0.02)


@asynccontextmanager
async def levantar_api(graph, concurrencia: int = 5) -> AsyncIterator[Entorno]:
    redis = FakeAsyncRedis(decode_responses=True)
    store = JobStore(redis)
    worker = Worker(redis, store, graph, concurrencia=concurrencia, espera_cola_s=0.05)

    @asynccontextmanager
    async def fabrica() -> AsyncIterator[Recursos]:
        await worker.iniciar()
        try:
            yield Recursos(store=store, worker=worker, redis=redis, modelos={"todos": "fake"})
        finally:
            await worker.detener()

    app = create_app(fabrica)
    async with app.router.lifespan_context(app):
        transporte = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transporte, base_url="http://test") as client:
            yield Entorno(client, store, worker, redis)
    await redis.aclose()


@pytest.fixture
def publicador() -> PublicadorFake:
    return PublicadorFake()
