"""Worker: corre el grafo en segundo plano y mantiene el estado del job en Redis.

Patrón "Worker" con una cola en Redis:

    POST /tasks ──LPUSH──> pe7:cola ──BLMOVE──> pe7:procesando ──> Worker.procesar()
                                                                    │ RUNNING
                                                                    ├─ grafo terminó ............ DONE
                                                                    ├─ grafo pausó (interrupt) .. WAITING_APPROVAL
                                                                    └─ excepción ................ FAILED (+ error)

- La cola vive en Redis, no en memoria: los mensajes sobreviven a un reinicio de la API y
  se pueden levantar workers en otro proceso (`uv run python -m app.worker`).
- `BLMOVE` pasa cada mensaje de la cola a una lista `procesando` en un solo paso atómico, y
  se borra de ahí recién al terminar. Si el proceso muere a mitad de camino, al arrancar
  `recuperar()` devuelve esos mensajes a la cola (entrega "al menos una vez").
- Al re-ejecutar un job que ya tenía checkpoint, el grafo CONTINÚA desde el último paso
  guardado en Redis (`ainvoke(None, ...)`) en vez de empezar de cero.
- `concurrencia` consumidores (tareas asyncio) comparten el event loop: el grafo es async
  de punta a punta (llamadas HTTP al LLM, Redis y la búsqueda en un hilo), así que 5 jobs
  avanzan en paralelo sin bloquear la API.
- NINGUNA excepción se escapa del worker sin quedar registrada: el job pasa a FAILED con el
  tipo y el mensaje del error, así el cliente que hace polling no queda esperando para siempre.
"""

import asyncio
import json
import logging
from typing import Any

from langgraph.graph.state import CompiledStateGraph
from langgraph.types import Command
from redis.asyncio import Redis

from app.graph import run_config
from app.jobs import Estado, JobStore
from app.state import estado_inicial

logger = logging.getLogger("app.worker")

MAX_ERROR = 1000  # largo máximo del mensaje de error guardado en el job


def resumen_resultado(valores: dict[str, Any]) -> dict[str, Any]:
    """Lo que ve el cliente en `resultado`: la respuesta y su trazabilidad, sin mensajes internos."""
    return {
        "respuesta": valores.get("final_answer", ""),
        "task_completed": valores.get("task_completed", False),
        "cierre_forzado": valores.get("cierre_forzado", False),
        "ruta_supervisor": [d["next"] for d in valores.get("decisiones", [])],
        "contribuciones": [
            {"agente": c["agente"], "paso": c["paso"], "estado": c["estado"], "fuentes": c["fuentes"],
             "herramientas": c["herramientas"], "resultado": c["resultado"]}
            for c in valores.get("contributions", [])
        ],
        "motivos_criticidad": valores.get("motivos_criticidad", []),
        "aprobacion": valores.get("aprobacion"),
        "reporte_publicado": valores.get("reporte_publicado"),
    }


class Worker:
    def __init__(
        self,
        redis: Redis,
        store: JobStore,
        graph: CompiledStateGraph,
        concurrencia: int = 5,
        espera_cola_s: float = 1.0,
    ) -> None:
        self.redis = redis
        self.store = store
        self.graph = graph
        self.concurrencia = concurrencia
        self.espera_cola_s = espera_cola_s
        self.cola = f"{store.prefijo}:cola"
        self.procesando = f"{store.prefijo}:procesando"
        self._tareas: list[asyncio.Task] = []
        self._activo = False

    # --- Productor (lo usan los endpoints) ---

    async def encolar_ejecucion(self, job_id: str) -> None:
        await self.redis.lpush(self.cola, json.dumps({"tipo": "ejecutar", "job_id": job_id}))

    async def encolar_reanudacion(self, job_id: str, decision: dict[str, Any]) -> None:
        await self.redis.lpush(self.cola, json.dumps({"tipo": "reanudar", "job_id": job_id, "decision": decision}))

    # --- Ciclo de vida ---

    async def iniciar(self) -> None:
        await self.recuperar()
        self._activo = True
        self._tareas = [asyncio.create_task(self._consumir(i), name=f"worker-{i}") for i in range(self.concurrencia)]
        logger.info("Worker iniciado con %d consumidores", self.concurrencia)

    async def detener(self) -> None:
        self._activo = False
        for tarea in self._tareas:
            tarea.cancel()
        await asyncio.gather(*self._tareas, return_exceptions=True)
        self._tareas = []

    async def recuperar(self) -> int:
        """Devuelve a la cola los mensajes que quedaron a medio procesar (caída del proceso)."""
        recuperados = 0
        while await self.redis.lmove(self.procesando, self.cola, "RIGHT", "RIGHT"):
            recuperados += 1
        if recuperados:
            logger.warning("Se reencolaron %d mensajes que habían quedado en proceso", recuperados)
        return recuperados

    async def _consumir(self, numero: int) -> None:
        while self._activo:
            try:
                crudo = await self.redis.blmove(self.cola, self.procesando, self.espera_cola_s, "RIGHT", "LEFT")
            except asyncio.CancelledError:
                raise
            except Exception:  # Redis caído: se espera y se reintenta, el consumidor no muere
                logger.exception("worker-%d: no pude leer la cola", numero)
                await asyncio.sleep(1)
                continue
            if crudo is None:
                await asyncio.sleep(0.05)
                continue
            # procesar() nunca levanta salvo CancelledError (apagado): en ese caso el mensaje
            # queda en `procesando` a propósito, para que recuperar() lo reencole.
            await self.procesar(json.loads(crudo))
            await self.redis.lrem(self.procesando, 1, crudo)

    # --- Ejecución de un job ---

    async def procesar(self, mensaje: dict[str, Any]) -> None:
        """Corre (o reanuda) el grafo de un job. Nunca deja escapar una excepción."""
        job_id = mensaje["job_id"]
        try:
            job = await self.store.get(job_id)
            if job is None:
                logger.warning("Mensaje para un job inexistente (%s): se descarta", job_id)
                return
            await self.store.actualizar(job_id, Estado.RUNNING, aprobacion_pendiente=None)
            config = run_config(job_id, fase=mensaje["tipo"])

            if mensaje["tipo"] == "reanudar":
                # La decisión humana entra al grafo como valor de retorno del interrupt().
                entrada: Any = Command(resume=mensaje["decision"])
            elif (await self.graph.aget_state(config)).next:
                # Ya hay un checkpoint a mitad de camino (el proceso se cayó): se continúa.
                entrada = None
            else:
                entrada = estado_inicial(job["pregunta"], publicar=job["publicar"])

            await self.graph.ainvoke(entrada, config)

            snapshot = await self.graph.aget_state(config)
            interrupciones = [i for tarea in snapshot.tasks for i in tarea.interrupts]
            if interrupciones:
                await self.store.actualizar(
                    job_id, Estado.WAITING_APPROVAL, aprobacion_pendiente=interrupciones[0].value
                )
            else:
                await self.store.actualizar(job_id, Estado.DONE, resultado=resumen_resultado(snapshot.values))
        except asyncio.CancelledError:
            # Apagado ordenado: el job queda como está y su mensaje sigue en `procesando`
            # para que recuperar() lo reencole al volver a arrancar.
            raise
        except Exception as exc:
            logger.exception("El job %s falló", job_id)
            try:
                await self.store.actualizar(job_id, Estado.FAILED, error=f"{type(exc).__name__}: {exc}"[:MAX_ERROR])
            except Exception:  # ni siquiera Redis responde: se registra en el log
                logger.exception("No pude marcar FAILED el job %s", job_id)


async def _main() -> None:
    """Worker como proceso aparte (escala horizontal): `uv run python -m app.worker`.

    Útil con `WORKER_EMBEBIDO=0` en la API: la API solo encola y estos procesos ejecutan.
    """
    from dotenv import load_dotenv

    from app.main import Recursos

    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    async with Recursos.desde_entorno(iniciar_worker=True) as recursos:
        logger.info("Worker standalone con modelos %s; Ctrl+C para salir", recursos.modelos)
        await asyncio.Event().wait()


if __name__ == "__main__":
    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        pass
