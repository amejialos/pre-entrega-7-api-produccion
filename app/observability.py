"""Observabilidad: trazas OpenInference -> Arize Phoenix.

`init_tracing()` registra un TracerProvider de OpenTelemetry que exporta a Phoenix
(`phoenix.otel.register`) e instrumenta LangChain/LangGraph con OpenInference
(`LangChainInstrumentor`). Desde ese momento cada corrida del grafo genera una traza:

    orquestador job=xxxx (CHAIN)              <- la corrida completa (latencia total del job)
    ├── supervisor (CHAIN)
    │   └── ChatOpenAICompatible (LLM)        <- modelo, tokens de entrada/salida -> costo
    ├── researcher (CHAIN)
    │   └── researcher (agente ReAct) -> LLM + buscar_documentacion (TOOL)
    ├── ...
    └── aprobacion_humana (CHAIN)             <- la pausa queda registrada en la traza

Phoenix calcula el COSTO de cada span LLM con los tokens (`llm.token_count.*`) y su tabla de
precios por modelo (`llm.model_name`); la traza suma el costo de sus spans. La LATENCIA de
cada traza es la del span raíz, y el proyecto muestra sus percentiles (p50/p95/p99).

La reanudación tras la aprobación es OTRA traza (otro `ainvoke`), pero comparte la sesión:
OpenInference usa el `thread_id` de la metadata como `session.id` (ver `graph.run_config`).

Variables:
- PHOENIX_COLLECTOR_ENDPOINT: dónde mandar los spans (default http://localhost:6006/v1/traces).
- PHOENIX_PROJECT_NAME: proyecto en Phoenix (default "pe7-api-agente").
- TRACING_ENABLED=0: apaga la instrumentación (los tests corren sin esto).
"""

import logging
import os

from opentelemetry.sdk.trace import TracerProvider

logger = logging.getLogger("app.observability")

DEFAULT_ENDPOINT = "http://localhost:6006/v1/traces"
DEFAULT_PROJECT = "pe7-api-agente"


def tracing_habilitado() -> bool:
    return os.environ.get("TRACING_ENABLED", "1").strip().lower() not in {"0", "false", "no"}


def init_tracing() -> TracerProvider | None:
    """Registra el exportador a Phoenix e instrumenta LangChain. Devuelve el provider (o None)."""
    if not tracing_habilitado():
        logger.info("Trazas desactivadas (TRACING_ENABLED=0)")
        return None
    from openinference.instrumentation.langchain import LangChainInstrumentor
    from phoenix.otel import register

    endpoint = os.environ.get("PHOENIX_COLLECTOR_ENDPOINT", "").strip() or DEFAULT_ENDPOINT
    proyecto = os.environ.get("PHOENIX_PROJECT_NAME", "").strip() or DEFAULT_PROJECT
    provider = register(
        endpoint=endpoint,
        project_name=proyecto,
        # Batch: los spans se exportan en segundo plano, sin sumar latencia a cada paso.
        batch=True,
        set_global_tracer_provider=True,
        verbose=False,
    )
    LangChainInstrumentor().instrument(tracer_provider=provider)
    logger.info("Trazas OpenInference -> Phoenix (%s, proyecto %s)", endpoint, proyecto)
    return provider


def shutdown_tracing(provider: TracerProvider | None) -> None:
    """Exporta los spans pendientes y desinstrumenta (al apagar la API)."""
    if provider is None:
        return
    from openinference.instrumentation.langchain import LangChainInstrumentor

    provider.force_flush()
    LangChainInstrumentor().uninstrument()
    provider.shutdown()
