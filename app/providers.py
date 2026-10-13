"""Construcción de los modelos de chat (uno por rol) a partir de variables de entorno.

Es el único módulo que lee la configuración del proveedor: el grafo recibe los modelos
por parámetro (inyección de dependencias), así los tests le pasan modelos fake sin red.

Cada rol (supervisor, investigador, analista, sintetizador) puede usar un modelo distinto
con `SUPERVISOR_MODEL`, `RESEARCH_MODEL`, `ANALYST_MODEL` y `SYNTH_MODEL`; si no están,
se usa `OPENAI_MODEL`. Sirve para repartir la cuota del free tier de Gemini, que se
cuenta por modelo (por día y por minuto). Esta solución de la thought signature viene de la Pre-entrega 5.

Se usa `ChatOpenAI` contra cualquier endpoint compatible con la API de OpenAI: con una
key de OpenAI anda tal cual, y con `OPENAI_BASE_URL` apuntando al endpoint compatible
de Gemini se puede probar gratis. Ambos soportan tool calling, que es lo único que
necesita el agente.

Detalle de Gemini: los modelos con razonamiento devuelven en cada tool call un campo
extra, `extra_content.google.thought_signature`, y exigen recibirlo de vuelta en el
historial del turno siguiente (si falta, responden 400 "Function call is missing a
thought_signature"). `ChatOpenAI` descarta ese campo al convertir la respuesta, así que
`ChatOpenAICompatible` lo guarda en el AIMessage y lo reinyecta al armar el pedido.
Con OpenAI el campo no existe y la subclase no cambia nada.
"""

import os
from typing import Any, Literal

import openai
from langchain_core.language_models import LanguageModelInput
from langchain_core.messages import AIMessage
from langchain_core.outputs import ChatResult
from langchain_core.rate_limiters import InMemoryRateLimiter
from langchain_openai import ChatOpenAI

DEFAULT_OPENAI_MODEL = "gpt-4o-mini"
# Clave de additional_kwargs donde se guarda {tool_call_id: extra_content}.
EXTRA_CONTENT_KEY = "tool_call_extra_content"


class ChatOpenAICompatible(ChatOpenAI):
    """ChatOpenAI que preserva el `extra_content` de las tool calls (thought signatures de Gemini)."""

    def _create_chat_result(
        self,
        response: dict | openai.BaseModel,
        generation_info: dict | None = None,
    ) -> ChatResult:
        result = super()._create_chat_result(response, generation_info)
        data = response if isinstance(response, dict) else response.model_dump()
        for choice, generation in zip(data.get("choices") or [], result.generations, strict=False):
            extras = {
                tc["id"]: tc["extra_content"]
                for tc in (choice.get("message") or {}).get("tool_calls") or []
                if tc.get("extra_content") and tc.get("id")
            }
            if extras and isinstance(generation.message, AIMessage):
                generation.message.additional_kwargs[EXTRA_CONTENT_KEY] = extras
        return result

    def _get_request_payload(
        self,
        input_: LanguageModelInput,
        *,
        stop: list[str] | None = None,
        **kwargs: Any,
    ) -> dict:
        payload = super()._get_request_payload(input_, stop=stop, **kwargs)
        messages = self._convert_input(input_).to_messages()
        # En chat/completions hay un dict por mensaje, en el mismo orden.
        for message, message_dict in zip(messages, payload.get("messages") or [], strict=False):
            extras = message.additional_kwargs.get(EXTRA_CONTENT_KEY) if isinstance(message, AIMessage) else None
            for tool_call in message_dict.get("tool_calls") or [] if extras else []:
                if tool_call.get("id") in extras:
                    tool_call["extra_content"] = extras[tool_call["id"]]
        return payload


Rol = Literal["supervisor", "research", "analyst", "synth"]
_VARIABLE_POR_ROL: dict[str, str] = {
    "supervisor": "SUPERVISOR_MODEL",
    "research": "RESEARCH_MODEL",
    "analyst": "ANALYST_MODEL",
    "synth": "SYNTH_MODEL",
}
# Un limitador POR MODELO: la cuota por minuto del free tier de Gemini se cuenta por modelo,
# y con varios jobs concurrentes los roles que comparten modelo tienen que compartir límite.
# (En la PE6 era uno solo para todos los roles: con una sola corrida a la vez alcanzaba.)
_LIMITADORES: dict[str, InMemoryRateLimiter] = {}


def _require_env(var: str) -> str:
    value = os.environ.get(var, "").strip()
    if not value:
        raise ValueError(f"Falta la variable de entorno {var}. Copiá .env.example a .env y completala.")
    return value


def _rate_limiter(modelo: str) -> InMemoryRateLimiter | None:
    """Limitador opcional de pedidos por minuto POR MODELO (LLM_RPM). El free tier de
    Gemini permite ~5 pedidos por minuto por modelo: sin esto aparece el 429."""
    rpm = os.environ.get("LLM_RPM", "").strip()
    if not rpm:
        return None
    rps = float(rpm) / 60
    limitador = _LIMITADORES.get(modelo)
    if limitador is None or limitador.requests_per_second != rps:
        limitador = InMemoryRateLimiter(requests_per_second=rps, check_every_n_seconds=0.5, max_bucket_size=1)
        _LIMITADORES[modelo] = limitador
    return limitador


def model_name_for(rol: Rol) -> str:
    """Modelo del rol: `<ROL>_MODEL` si está definido, si no `OPENAI_MODEL`."""
    return (
        os.environ.get(_VARIABLE_POR_ROL[rol], "").strip()
        or os.environ.get("OPENAI_MODEL", "").strip()
        or DEFAULT_OPENAI_MODEL
    )


def build_model(rol: Rol = "supervisor") -> ChatOpenAICompatible:
    """Modelo del rol, configurado con OPENAI_API_KEY, OPENAI_BASE_URL y LLM_RPM."""
    modelo = model_name_for(rol)
    return ChatOpenAICompatible(
        model=modelo,
        api_key=_require_env("OPENAI_API_KEY"),
        base_url=os.environ.get("OPENAI_BASE_URL") or None,
        temperature=0,  # orquestación y cálculo: lo más determinista posible
        # Sin reintentos del SDK: los hace el nodo del grafo (RetryPolicy) con esperas
        # largas. Los reintentos rápidos del SDK contra un 429 solo gastan más cuota.
        max_retries=0,
        timeout=90,
        rate_limiter=_rate_limiter(modelo),
        disable_streaming=True,  # el rescate de extra_content está en la ruta sin streaming
    )
