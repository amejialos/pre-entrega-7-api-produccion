"""Integración con un Redis REAL (`AsyncRedisSaver`): se saltea si no hay Redis.

    docker compose up -d redis && uv run pytest -q tests/test_redis_checkpoint.py

Pausa el grafo en `aprobacion_humana` con un grafo, y lo reanuda con OTRA instancia del grafo
y OTRO checkpointer conectados al mismo Redis: el estado salió de Redis, no de memoria.
Los LLM siguen siendo fakes (sin red hacia proveedores).
"""

import os
import uuid

import pytest
from langgraph.types import Command
from redis.asyncio import Redis

from app.graph import redis_checkpointer, run_config
from app.state import estado_inicial
from tests.test_hitl import PREGUNTA, RESPUESTA, grafo_m6

REDIS_URL = os.environ.get("REDIS_TEST_URL", "redis://localhost:6379/0")


async def _redis_disponible() -> bool:
    cliente = Redis.from_url(REDIS_URL, socket_connect_timeout=0.5, decode_responses=True)
    try:
        await cliente.ping()
        modulos = {m["name"] for m in await cliente.module_list()}
        return {"search", "ReJSON"} <= modulos
    except Exception:
        return False
    finally:
        await cliente.aclose()


async def test_checkpoint_en_redis_sobrevive_entre_instancias(publicador):
    if not await _redis_disponible():
        pytest.skip(f"No hay Redis con JSON + Search en {REDIS_URL}")
    thread_id = f"test-{uuid.uuid4().hex}"
    config = run_config(thread_id)

    async with redis_checkpointer(REDIS_URL) as saver:
        grafo = grafo_m6(publicador, checkpointer=saver)
        await grafo.ainvoke(estado_inicial(PREGUNTA, publicar=True), config)
        pausa = await grafo.aget_state(config)
        assert pausa.next == ("aprobacion_humana",)
        assert pausa.tasks[0].interrupts[0].value["accion"] == "publicar_reporte"

    cliente = Redis.from_url(REDIS_URL, decode_responses=True)
    try:
        claves = [k async for k in cliente.scan_iter(match=f"checkpoint:{thread_id}:*")]
        assert claves, "el checkpoint tendría que estar guardado en Redis"

        # "Otro proceso": checkpointer y grafo nuevos (sin LLM), mismo Redis.
        async with redis_checkpointer(REDIS_URL) as saver:
            grafo = grafo_m6(publicador, checkpointer=saver, sin_llm=True)
            final = await grafo.ainvoke(Command(resume={"aprobado": True, "comentario": "ok"}), config)
            assert final["final_answer"] == RESPUESTA
            assert final["reporte_publicado"] == f"fake:reporte:{thread_id}"
            await saver.adelete_thread(thread_id)  # no dejar basura de los tests en Redis
    finally:
        await cliente.aclose()
