"""Estado de los trabajos (jobs) en Redis.

Cada job es un hash `pe7:job:{job_id}` con:
    status            PENDING | RUNNING | WAITING_APPROVAL | DONE | FAILED
    pregunta, publicar
    created_at, started_at, updated_at, finished_at   (ISO 8601, UTC)
    resultado         JSON con la respuesta final, contribuciones y decisión humana (DONE)
    error             tipo y mensaje de la excepción (FAILED)
    aprobacion_pendiente  JSON con lo que tiene que revisar el humano (WAITING_APPROVAL)

Además, `pe7:job:{job_id}:eventos` es una lista con cada transición de estado y su hora
(sirve de evidencia de PENDING -> RUNNING -> ... -> DONE), y `pe7:jobs` un sorted set por
fecha de creación para listar los últimos jobs.

El estado del job (para el cliente que hace polling) y el CHECKPOINT del grafo (para
LangGraph) son cosas distintas y viven en claves distintas del mismo Redis: el job dice
"en qué anda el trabajo", el checkpoint guarda el estado completo del grafo para reanudarlo.
El `job_id` es el `thread_id` del checkpoint: con uno se encuentra el otro.
"""

import json
import uuid
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from redis.asyncio import Redis
from redis.exceptions import WatchError

PREFIJO = "pe7"
TTL_SEGUNDOS = 7 * 24 * 3600  # los jobs se borran solos a la semana
_CAMPOS_JSON = ("resultado", "aprobacion_pendiente", "aprobacion")


class Estado(StrEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    WAITING_APPROVAL = "WAITING_APPROVAL"
    DONE = "DONE"
    FAILED = "FAILED"


FINALES = {Estado.DONE, Estado.FAILED}


def ahora() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds")


class JobStore:
    """Lectura y escritura del estado de los jobs. Todas las operaciones son async."""

    def __init__(self, redis: Redis, prefijo: str = PREFIJO, ttl: int = TTL_SEGUNDOS) -> None:
        # El cliente tiene que crearse con decode_responses=True (str, no bytes).
        self.redis = redis
        self.prefijo = prefijo
        self.ttl = ttl

    def clave(self, job_id: str) -> str:
        return f"{self.prefijo}:job:{job_id}"

    def _eventos(self, job_id: str) -> str:
        return f"{self.clave(job_id)}:eventos"

    async def crear(self, pregunta: str, publicar: bool = False) -> dict[str, Any]:
        job_id = uuid.uuid4().hex
        momento = ahora()
        campos = {
            "job_id": job_id,
            "status": Estado.PENDING,
            "pregunta": pregunta,
            "publicar": int(publicar),
            "created_at": momento,
            "updated_at": momento,
        }
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.hset(self.clave(job_id), mapping=campos)
            pipe.expire(self.clave(job_id), self.ttl)
            pipe.rpush(self._eventos(job_id), json.dumps({"status": Estado.PENDING, "t": momento}))
            pipe.expire(self._eventos(job_id), self.ttl)
            pipe.zadd(f"{self.prefijo}:jobs", {job_id: datetime.now(UTC).timestamp()})
            await pipe.execute()
        return await self.get(job_id)  # type: ignore[return-value]

    async def get(self, job_id: str) -> dict[str, Any] | None:
        datos = await self.redis.hgetall(self.clave(job_id))
        if not datos:
            return None
        for campo in _CAMPOS_JSON:
            if campo in datos:
                datos[campo] = json.loads(datos[campo])
        datos["publicar"] = datos.get("publicar") == "1"
        datos["eventos"] = [json.loads(e) for e in await self.redis.lrange(self._eventos(job_id), 0, -1)]
        return datos

    async def actualizar(self, job_id: str, status: Estado | None = None, **campos: Any) -> None:
        """Cambia el estado y/o campos. Los dict/list se guardan como JSON; None borra el campo."""
        momento = ahora()
        valores: dict[str, Any] = {"updated_at": momento}
        borrar = [k for k, v in campos.items() if v is None]
        for k, v in campos.items():
            if v is not None:
                valores[k] = json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else v
        if status is not None:
            valores["status"] = status
            if status in FINALES:
                valores["finished_at"] = momento
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.hset(self.clave(job_id), mapping=valores)
            if borrar:
                pipe.hdel(self.clave(job_id), *borrar)
            if status is not None:
                pipe.rpush(self._eventos(job_id), json.dumps({"status": status, "t": momento}))
            await pipe.execute()
        if status == Estado.RUNNING:
            # started_at es la PRIMERA vez que corrió: no se pisa al reanudar.
            await self.redis.hsetnx(self.clave(job_id), "started_at", momento)

    async def tomar_aprobacion(self, job_id: str, decision: dict[str, Any]) -> bool:
        """WAITING_APPROVAL -> PENDING de forma atómica (WATCH/MULTI). False si no estaba esperando."""
        clave = self.clave(job_id)
        async with self.redis.pipeline(transaction=True) as pipe:
            try:
                await pipe.watch(clave)
                if await pipe.hget(clave, "status") != Estado.WAITING_APPROVAL:
                    return False
                momento = ahora()
                pipe.multi()
                pipe.hset(
                    clave,
                    mapping={
                        "status": Estado.PENDING,
                        "updated_at": momento,
                        "aprobacion": json.dumps(decision, ensure_ascii=False),
                    },
                )
                pipe.rpush(self._eventos(job_id), json.dumps({"status": Estado.PENDING, "t": momento, "por": "approve"}))
                await pipe.execute()
                return True
            except WatchError:  # otro cliente cambió el job entre el WATCH y el EXEC
                return False

    async def listar(self, limite: int = 20) -> list[dict[str, Any]]:
        ids = await self.redis.zrevrange(f"{self.prefijo}:jobs", 0, limite - 1)
        jobs = [await self.get(job_id) for job_id in ids]
        return [j for j in jobs if j is not None]
