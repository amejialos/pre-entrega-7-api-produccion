"""API REST del orquestador multi-agente (FastAPI, asíncrona).

    POST /tasks                 -> crea el job (PENDING), lo encola y responde 202 + job_id al instante
    GET  /tasks/{job_id}        -> estado del job: PENDING | RUNNING | WAITING_APPROVAL | DONE | FAILED
    GET  /tasks                 -> últimos jobs
    POST /tasks/{job_id}/approve-> decisión humana para una tarea crítica pausada (ver app/hitl.py)
    GET  /health                -> Redis responde

Ningún endpoint espera al grafo: solo leen o escriben Redis (cliente async) y encolan. El
trabajo pesado lo hace `app.worker.Worker` en segundo plano. Por eso un POST tarda
milisegundos aunque la corrida del grafo tarde minutos.

Levantar:  uv run uvicorn app.main:app --port 8000
"""

import logging
import os
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass, field
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request, Response, status
from pydantic import BaseModel, Field, field_validator
from redis.asyncio import Redis

from app.hitl import publicador_redis
from app.hitl import router as hitl_router
from app.jobs import Estado, JobStore
from app.worker import Worker

logger = logging.getLogger("app.main")

DEFAULT_REDIS_URL = "redis://localhost:6379/0"


# --- Recursos compartidos (Redis, checkpointer, grafo, worker) ---


@dataclass
class Recursos:
    store: JobStore
    worker: Worker
    redis: Redis
    modelos: dict[str, str] = field(default_factory=dict)

    @classmethod
    @asynccontextmanager
    async def desde_entorno(cls, iniciar_worker: bool = True) -> AsyncIterator["Recursos"]:
        """Producción: Redis real, `AsyncRedisSaver`, Gemini/OpenAI + Pinecone y trazas a Phoenix."""
        from dotenv import load_dotenv

        from app.graph import build_from_env, redis_checkpointer
        from app.observability import init_tracing, shutdown_tracing

        load_dotenv()
        provider = init_tracing()  # antes de construir el grafo, para que todo quede instrumentado
        redis_url = os.environ.get("REDIS_URL", "").strip() or DEFAULT_REDIS_URL
        redis = Redis.from_url(redis_url, decode_responses=True)
        try:
            await redis.ping()
            store = JobStore(redis)
            async with redis_checkpointer(redis_url) as checkpointer:
                graph, modelos = await build_from_env(checkpointer, publicador_redis(redis))
                worker = Worker(redis, store, graph, concurrencia=int(os.environ.get("WORKER_CONCURRENCY", "5")))
                if iniciar_worker:
                    await worker.iniciar()
                try:
                    yield cls(store=store, worker=worker, redis=redis, modelos=modelos)
                finally:
                    await worker.detener()
        finally:
            await redis.aclose()
            shutdown_tracing(provider)


FabricaRecursos = Callable[[], AbstractAsyncContextManager[Recursos]]


def _fabrica_por_defecto() -> AbstractAsyncContextManager[Recursos]:
    # WORKER_EMBEBIDO=0: la API solo encola y los jobs los corren procesos `python -m app.worker`.
    embebido = os.environ.get("WORKER_EMBEBIDO", "1").strip() not in {"0", "false", "no"}
    return Recursos.desde_entorno(iniciar_worker=embebido)


# --- Esquemas de la API ---


class TareaIn(BaseModel):
    """Body de POST /tasks."""

    pregunta: str = Field(
        min_length=10,
        max_length=2000,
        description="Consulta para el orquestador (documentación de Caudal + cálculos).",
        examples=["¿Cuál es el tamano_lote por defecto y cuántos lotes son 2.000.000 de eventos?"],
    )
    publicar: bool = Field(
        default=False,
        description="Publicar el reporte al terminar. Es una acción con efectos secundarios: "
        "la tarea pasa a ser CRÍTICA y espera aprobación humana.",
    )

    @field_validator("pregunta")
    @classmethod
    def _no_vacia(cls, valor: str) -> str:
        if not valor.strip():
            raise ValueError("la pregunta no puede ser solo espacios")
        return valor.strip()


class TareaAceptada(BaseModel):
    job_id: str
    status: Estado
    links: dict[str, str]


class Job(BaseModel):
    """Estado de un job tal como está en Redis."""

    job_id: str
    status: Estado
    pregunta: str
    publicar: bool = False
    created_at: str
    updated_at: str
    started_at: str | None = None
    finished_at: str | None = None
    resultado: dict[str, Any] | None = None
    error: str | None = None
    aprobacion_pendiente: dict[str, Any] | None = Field(
        default=None, description="Qué tiene que revisar el humano (solo en WAITING_APPROVAL)."
    )
    aprobacion: dict[str, Any] | None = None
    eventos: list[dict[str, Any]] = Field(default_factory=list, description="Historial de estados con su hora.")


# --- Aplicación ---


def create_app(fabrica: FabricaRecursos = _fabrica_por_defecto) -> FastAPI:
    """Fábrica de la app. Los tests le pasan recursos con fakes (fakeredis, LLM fake, InMemorySaver)."""

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        async with fabrica() as recursos:
            app.state.store = recursos.store
            app.state.worker = recursos.worker
            app.state.redis = recursos.redis
            app.state.modelos = recursos.modelos
            yield

    app = FastAPI(
        title="API del orquestador multi-agente",
        version="0.1.0",
        description="Pre-entrega 7: tareas asíncronas con estado en Redis, trazas en Phoenix y aprobación humana.",
        lifespan=lifespan,
    )

    @app.post("/tasks", status_code=status.HTTP_202_ACCEPTED, response_model=TareaAceptada, tags=["tareas"])
    async def crear_tarea(tarea: TareaIn, request: Request, response: Response) -> TareaAceptada:
        """Encola la tarea y devuelve el job_id sin esperar al grafo."""
        job = await request.app.state.store.crear(tarea.pregunta, tarea.publicar)
        await request.app.state.worker.encolar_ejecucion(job["job_id"])
        response.headers["Location"] = f"/tasks/{job['job_id']}"
        return TareaAceptada(
            job_id=job["job_id"], status=job["status"], links={"estado": f"/tasks/{job['job_id']}"}
        )

    @app.get("/tasks/{job_id}", response_model=Job, tags=["tareas"])
    async def estado_tarea(job_id: str, request: Request) -> dict[str, Any]:
        """Estado actual del job (lectura de Redis: no bloquea ni espera)."""
        job = await request.app.state.store.get(job_id)
        if job is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, f"No existe el job {job_id}.")
        return job

    @app.get("/tasks", response_model=list[Job], tags=["tareas"])
    async def listar_tareas(request: Request, limite: int = Query(20, ge=1, le=100)) -> list[dict[str, Any]]:
        return await request.app.state.store.listar(limite)

    @app.get("/health", tags=["sistema"])
    async def health(request: Request) -> dict[str, Any]:
        await request.app.state.redis.ping()
        return {"status": "ok", "redis": "ok", "modelos": request.app.state.modelos}

    app.include_router(hitl_router)
    return app


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
app = create_app()
