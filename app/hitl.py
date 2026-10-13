"""Human-in-the-loop: qué tarea es crítica, el nodo que pausa y el endpoint que la reanuda.

CRITERIO DE TAREA CRÍTICA (explícito y testeable, ver `motivos_criticidad`):
una tarea es crítica cuando implica una acción con EFECTOS SECUNDARIOS fuera del sistema:
publicar el reporte final (`publicar_reporte` deja el reporte publicado en Redis, donde lo
leen otros sistemas). Se detecta de dos formas:
1. el cliente lo pide explícitamente: `"publicar": true` en el body de `POST /tasks`;
2. la pregunta lo pide en lenguaje natural ("publicá", "enviá", "notificá", "exportá"...).

El resto de las tareas (solo preguntar y calcular) no tiene efectos fuera del grafo y
termina sin pausa.

Flujo en el grafo:

    synthesizer -> evaluar_criticidad --(no crítica)--> END
                                     --(crítica)----> aprobacion_humana --interrupt()-- PAUSA
    POST /tasks/{id}/approve  -> Command(resume={aprobado, comentario})
                                     aprobacion_humana --(aprobado)--> publicar_reporte -> END
                                                       --(rechazado)-> END (respuesta explica el rechazo)

`interrupt()` guarda el estado en el checkpoint (Redis) y corta la ejecución. Al reanudar,
LangGraph vuelve a ejecutar el nodo `aprobacion_humana` DESDE EL PRINCIPIO y esta vez
`interrupt()` devuelve el valor de `Command(resume=...)`. Por eso el nodo no tiene efectos
antes del `interrupt()`: el efecto secundario vive en otro nodo, que solo corre aprobado.
"""

import json
import re
import unicodedata
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Request, status
from langchain_core.messages import AIMessage
from langchain_core.runnables import RunnableConfig
from langgraph.graph import END
from langgraph.types import Command, interrupt
from pydantic import BaseModel, Field
from redis.asyncio import Redis

from app.state import OrchestratorState

ACCION_CRITICA = "publicar_reporte"
MAX_BORRADOR = 3000  # lo que ve el revisor del borrador (el resto queda en el checkpoint)

# Verbos que piden una acción hacia afuera. Se compara sin tildes y en minúsculas:
# "publicá" -> "publica". "publicación" NO matchea (no es un pedido de acción).
_PEDIDO_DE_ACCION = re.compile(r"\b(publica|envia|notifica|exporta)(r|lo|la|los|las)?\b")

# Firma del publicador inyectado: (job_id, reporte) -> destino donde quedó publicado.
Publicador = Callable[[str, dict[str, Any]], Awaitable[str]]


def _sin_tildes(texto: str) -> str:
    normalizado = unicodedata.normalize("NFKD", texto.lower())
    return "".join(ch for ch in normalizado if not unicodedata.combining(ch))


def motivos_criticidad(pregunta: str, publicar: bool) -> list[str]:
    """Por qué la tarea es crítica. Lista vacía = no es crítica (no se pausa)."""
    motivos = []
    if publicar:
        motivos.append(f"El cliente pidió '{ACCION_CRITICA}' (publicar=true): acción con efectos secundarios.")
    pedido = _PEDIDO_DE_ACCION.search(_sin_tildes(pregunta))
    if pedido:
        motivos.append(
            f"La pregunta pide una acción hacia afuera ('{pedido.group(0)}'): se ejecutaría '{ACCION_CRITICA}'."
        )
    return motivos


# --- Nodos del grafo ---


async def evaluar_criticidad(state: OrchestratorState) -> dict[str, Any]:
    """Aplica el criterio y deja los motivos en el estado (quedan en la traza y en el checkpoint)."""
    return {"motivos_criticidad": motivos_criticidad(state["pregunta"], state.get("publicar", False))}


def route_criticidad(state: OrchestratorState) -> Literal["aprobacion_humana", "__end__"]:
    return "aprobacion_humana" if state.get("motivos_criticidad") else END


async def aprobacion_humana(state: OrchestratorState) -> Command[Literal["publicar_reporte", "__end__"]]:
    """PAUSA obligatoria: nada se publica sin una aprobación externa.

    El payload del `interrupt` es lo que el revisor ve en `GET /tasks/{id}` (campo `aprobacion_pendiente`).
    """
    decision = interrupt(
        {
            "accion": ACCION_CRITICA,
            "motivos": state.get("motivos_criticidad", []),
            "borrador": state.get("final_answer", "")[:MAX_BORRADOR],
            "como_responder": "POST /tasks/{job_id}/approve con {\"aprobado\": true|false, \"comentario\": \"...\"}",
        }
    )
    registro = {
        "aprobado": bool(decision.get("aprobado")),
        "comentario": str(decision.get("comentario") or ""),
        "revisor": str(decision.get("revisor") or "anónimo"),
        "fecha": datetime.now(UTC).isoformat(timespec="seconds"),
    }
    if registro["aprobado"]:
        return Command(goto=ACCION_CRITICA, update={"aprobacion": registro})

    explicacion = (
        f"La acción '{ACCION_CRITICA}' fue RECHAZADA por revisión humana"
        + (f" (comentario: {registro['comentario']})" if registro["comentario"] else "")
        + ". El reporte NO se publicó; la respuesta de abajo queda solo como borrador."
    )
    return Command(
        goto=END,
        update={
            "aprobacion": registro,
            "final_answer": f"{explicacion}\n\n{state.get('final_answer', '')}",
            "messages": [AIMessage(content=explicacion, name="aprobacion_humana")],
        },
    )


def make_publicar_node(publicador: Publicador):
    """Nodo con el efecto secundario. Solo se llega acá con aprobación humana."""

    async def publicar_reporte(state: OrchestratorState, config: RunnableConfig) -> dict[str, Any]:
        job_id = config["configurable"]["thread_id"]
        reporte = {
            "job_id": job_id,
            "pregunta": state["pregunta"],
            "respuesta": state["final_answer"],
            "fuentes": sorted({f for c in state.get("contributions", []) for f in c["fuentes"]}),
            "aprobacion": state.get("aprobacion"),
        }
        destino = await publicador(job_id, reporte)
        return {
            "reporte_publicado": destino,
            "messages": [AIMessage(content=f"Reporte publicado en {destino}.", name=ACCION_CRITICA)],
        }

    return publicar_reporte


# --- Endpoint de aprobación ---


class Aprobacion(BaseModel):
    """La señal externa que reanuda una tarea pausada."""

    aprobado: bool = Field(description="true reanuda y ejecuta la acción crítica; false la cancela.")
    comentario: str = Field(default="", max_length=1000)
    revisor: str = Field(default="anónimo", max_length=100)


router = APIRouter(tags=["human-in-the-loop"])


@router.post("/tasks/{job_id}/approve", status_code=status.HTTP_202_ACCEPTED)
async def aprobar(job_id: str, aprobacion: Aprobacion, request: Request) -> dict[str, Any]:
    """Registra la decisión humana y encola la reanudación (no espera a que termine)."""
    store = request.app.state.store
    worker = request.app.state.worker
    if await store.get(job_id) is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"No existe el job {job_id}.")
    # Transición atómica WAITING_APPROVAL -> PENDING: dos aprobaciones simultáneas no
    # pueden reanudar dos veces el mismo hilo (la segunda recibe 409).
    if not await store.tomar_aprobacion(job_id, aprobacion.model_dump()):
        actual = await store.get(job_id)
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            f"El job {job_id} no está esperando aprobación (estado: {actual['status']}).",
        )
    await worker.encolar_reanudacion(job_id, aprobacion.model_dump())
    return {"job_id": job_id, "status": "PENDING", "aprobado": aprobacion.aprobado, "links": {"estado": f"/tasks/{job_id}"}}


def publicador_redis(redis: Redis, prefijo: str = "pe7") -> Publicador:
    """El efecto secundario real: deja el reporte en `pe7:reporte:{job_id}` y lo anuncia en la
    lista `pe7:reportes_publicados`, de donde lo tomarían otros sistemas (un mail, un Slack...)."""

    async def publicar(job_id: str, reporte: dict[str, Any]) -> str:
        clave = f"{prefijo}:reporte:{job_id}"
        await redis.set(clave, json.dumps(reporte, ensure_ascii=False))
        await redis.lpush(f"{prefijo}:reportes_publicados", job_id)
        return clave

    return publicar
