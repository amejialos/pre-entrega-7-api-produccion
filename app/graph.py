"""El orquestador multi-agente del Módulo 6 + el checkpointer de Redis + el human-in-the-loop.

    START -> supervisor --route_from_supervisor--> researcher  -> supervisor
                                              |--> analyst     -> supervisor
                                              |--> synthesizer -> evaluar_criticidad
    evaluar_criticidad --(no crítica)--> END
                       --(crítica)----> aprobacion_humana --interrupt()--> [PAUSA hasta POST /approve]
    aprobacion_humana --(aprobado)--> publicar_reporte -> END
                      --(rechazado)-> END

- Checkpointer: `AsyncRedisSaver` (langgraph-checkpoint-redis). Cada super-step del grafo
  queda guardado en Redis bajo el `thread_id`, que es el `job_id` de la API. Eso es lo que
  permite pausar en `aprobacion_humana` y reanudar desde OTRO proceso o después de un
  reinicio: el estado no vive en memoria.

- El supervisor es el único que decide el flujo (topología jerárquica): los especialistas
  siempre le devuelven el control, nunca se llaman entre sí.
- `route_from_supervisor` es la arista condicional; su retorno `Literal[...]` le dice a
  LangGraph cuáles son los destinos posibles (y así aparecen en el diagrama).
- Dos frenos contra el bucle infinito: `max_steps` en el supervisor (fuerza la síntesis)
  y `recursion_limit` en la invocación (red de seguridad de LangGraph).
"""

import asyncio
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from langchain_core.language_models import BaseChatModel
from langchain_core.runnables import RunnableConfig
from langchain_core.tools import BaseTool
from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.graph import END, START, StateGraph
from langgraph.graph.state import CompiledStateGraph

from app.agents.analyst_agent import build_analyst_agent, make_analyst_node
from app.agents.common import RETRY_MODELO
from app.agents.research_agent import build_research_agent, make_research_node
from app.agents.supervisor import MAX_STEPS, build_decider, make_supervisor_node, route_from_supervisor
from app.agents.synthesizer import make_synthesizer_node
from app.hitl import (
    ACCION_CRITICA,
    Publicador,
    aprobacion_humana,
    evaluar_criticidad,
    make_publicar_node,
    route_criticidad,
)
from app.state import OrchestratorState

# Cada turno supervisor + especialista son 2 pasos del grafo: 6 turnos = 12, más el turno
# que fuerza el cierre y la síntesis = 14, más criticidad, aprobación y publicación = 17.
# Con 20 sobra margen y un bug igual se corta.
RECURSION_LIMIT = 20


def build_graph(
    supervisor_model: BaseChatModel,
    research_model: BaseChatModel,
    analyst_model: BaseChatModel,
    synth_model: BaseChatModel,
    search_tool: BaseTool,
    max_steps: int = MAX_STEPS,
    structured_method: str = "json_schema",
    retry_policy=RETRY_MODELO,
    checkpointer: BaseCheckpointSaver | None = None,
    publicador: Publicador | None = None,
) -> CompiledStateGraph:
    """Arma y compila el orquestador. Modelos, buscador, checkpointer y publicador se
    inyectan: en producción Gemini + Pinecone + Redis, en los tests fakes y `InMemorySaver`."""
    research_agent = build_research_agent(research_model, search_tool)
    analyst_agent = build_analyst_agent(analyst_model)

    builder = StateGraph(OrchestratorState)
    builder.add_node(
        "supervisor",
        make_supervisor_node(build_decider(supervisor_model, structured_method), max_steps),
        retry_policy=retry_policy,
    )
    builder.add_node("researcher", make_research_node(research_agent), retry_policy=retry_policy)
    builder.add_node("analyst", make_analyst_node(analyst_agent), retry_policy=retry_policy)
    builder.add_node("synthesizer", make_synthesizer_node(synth_model), retry_policy=retry_policy)

    builder.add_edge(START, "supervisor")
    builder.add_conditional_edges("supervisor", route_from_supervisor)
    builder.add_edge("researcher", "supervisor")
    builder.add_edge("analyst", "supervisor")
    builder.add_node("evaluar_criticidad", evaluar_criticidad)
    builder.add_node("aprobacion_humana", aprobacion_humana)
    builder.add_node(ACCION_CRITICA, make_publicar_node(publicador or _publicador_no_configurado))

    builder.add_edge("synthesizer", "evaluar_criticidad")
    builder.add_conditional_edges("evaluar_criticidad", route_criticidad)
    # aprobacion_humana decide su destino con Command(goto=...) (lo declara su tipo de retorno).
    builder.add_edge(ACCION_CRITICA, END)
    return builder.compile(name="orquestador", checkpointer=checkpointer)


async def _publicador_no_configurado(job_id: str, reporte: dict) -> str:
    raise RuntimeError("No hay publicador configurado: build_graph(publicador=...).")


def run_config(
    thread_id: str | None = None, recursion_limit: int = RECURSION_LIMIT, **metadata: str
) -> RunnableConfig:
    """Config de una corrida. `thread_id` identifica el hilo en el checkpointer (= job_id).

    `run_name` y `metadata` viajan a las trazas: en Phoenix cada traza queda nombrada con el
    job, y OpenInference usa el `thread_id` de la metadata como `session.id`, así la corrida
    inicial y la reanudación tras la aprobación quedan agrupadas en la misma sesión.
    """
    config: RunnableConfig = {"recursion_limit": recursion_limit}
    if thread_id:
        config["configurable"] = {"thread_id": thread_id}
        config["run_name"] = f"orquestador job={thread_id[:8]}"
        config["metadata"] = {"job_id": thread_id, **metadata}
    return config


@asynccontextmanager
async def redis_checkpointer(redis_url: str) -> AsyncIterator[BaseCheckpointSaver]:
    """`AsyncRedisSaver` listo para usar: `asetup()` crea los índices de RediSearch.

    Necesita Redis con los módulos JSON y Search (la imagen `redis:8` ya los trae).
    """
    from langgraph.checkpoint.redis.aio import AsyncRedisSaver

    async with AsyncRedisSaver.from_conn_string(redis_url) as saver:
        await saver.asetup()
        yield saver


async def build_from_env(checkpointer: BaseCheckpointSaver, publicador: Publicador) -> tuple[CompiledStateGraph, dict[str, str]]:
    """Grafo real: un modelo por rol (Gemini u OpenAI) y el RAG híbrido de la PE4 sobre Pinecone.

    `RAGSystem.from_env()` es síncrono (Pinecone + BM25 + tiktoken): se arma en un hilo
    para no bloquear el event loop durante el arranque de la API.
    """
    from app.agents.research_agent import crear_herramienta_busqueda
    from app.providers import build_model, model_name_for
    from app.rag_system import RAGSystem

    roles = ("supervisor", "research", "analyst", "synth")
    modelos = {rol: model_name_for(rol) for rol in roles}
    rag = await asyncio.to_thread(RAGSystem.from_env)
    graph = build_graph(
        supervisor_model=build_model("supervisor"),
        research_model=build_model("research"),
        analyst_model=build_model("analyst"),
        synth_model=build_model("synth"),
        search_tool=crear_herramienta_busqueda(rag),
        structured_method=os.environ.get("SUPERVISOR_STRUCTURED_METHOD", "").strip() or "json_schema",
        checkpointer=checkpointer,
        publicador=publicador,
    )
    return graph, modelos
