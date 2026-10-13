"""Prueba de carga: N peticiones CONCURRENTES contra la API (por defecto 5).

    uv run python scripts/load_test.py                       # 5 concurrentes contra localhost:8000
    uv run python scripts/load_test.py --base-url http://localhost:8000 --timeout 900

Qué hace:
1. Lanza los N `POST /tasks` a la vez (asyncio.gather) y mide cuánto tarda cada POST en
   responder: tiene que ser instantáneo (202), aunque el grafo tarde minutos.
2. Hace polling de `GET /tasks/{id}` de cada job, en paralelo.
3. Si un job queda en WAITING_APPROVAL (tarea crítica), lo aprueba con
   `POST /tasks/{id}/approve` (como haría el revisor humano) y sigue esperando.
4. Reporta estados finales y tiempos: p50/p95 de la latencia total por job, del tiempo de
   cómputo (total menos la espera de aprobación) y del POST.

Guarda el detalle en `docs/load_test.json` y un resumen en `docs/load_test.md`.
El COSTO por ejecución y la latencia p95 "oficiales" salen de Phoenix (las trazas): ver
`scripts/phoenix_report.py` y las capturas en `screenshots/`.
"""

import argparse
import asyncio
import json
import math
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx

DOCS = Path(__file__).resolve().parent.parent / "docs"

# 5 tareas distintas. Dos son CRÍTICAS (publicar el reporte): pasan por la aprobación humana.
TAREAS: list[dict[str, Any]] = [
    {"pregunta": "Según la documentación de Caudal, ¿cuál es el tamaño de lote (tamano_lote) por defecto?"},
    {"pregunta": "¿Qué significa el error CAU-417 en Caudal y cómo se resuelve según la documentación?"},
    {
        "pregunta": "Según la documentación de Caudal, ¿qué tamaño de lote se recomienda para ingesta masiva? "
        "¿Cuántos lotes se forman con 2.000.000 de eventos usando ese tamaño?",
        "publicar": True,
    },
    {"pregunta": "¿Qué conectores de origen soporta Caudal según su documentación?"},
    {
        "pregunta": "Buscá en la documentación de Caudal el tamano_lote por defecto, calculá cuántos lotes son "
        "750.000 eventos y publicá el reporte para el equipo.",
    },
]


def percentil(valores: list[float], p: float) -> float:
    """Percentil con interpolación lineal (el método por defecto de numpy). Con 5 valores,
    el p95 queda entre el 4.º y el 5.º: 0,8 del camino hacia el máximo."""
    if not valores:
        return math.nan
    ordenados = sorted(valores)
    posicion = (len(ordenados) - 1) * p / 100
    abajo, arriba = math.floor(posicion), math.ceil(posicion)
    return ordenados[abajo] + (ordenados[arriba] - ordenados[abajo]) * (posicion - abajo)


async def correr_job(
    client: httpx.AsyncClient, indice: int, tarea: dict[str, Any], inicio_global: float, timeout: float, intervalo: float
) -> dict[str, Any]:
    t0 = time.monotonic()
    r = await client.post("/tasks", json=tarea)
    post_ms = (time.monotonic() - t0) * 1000
    r.raise_for_status()
    job_id = r.json()["job_id"]
    registro: dict[str, Any] = {
        "indice": indice,
        "job_id": job_id,
        "pregunta": tarea["pregunta"],
        "publicar": tarea.get("publicar", False),
        "post_status": r.status_code,
        "post_ms": round(post_ms, 1),
        "lanzado_s": round(t0 - inicio_global, 3),
        "aprobaciones": [],
        "estados_vistos": [],
    }
    espera_humana = 0.0
    while time.monotonic() - t0 < timeout:
        job = (await client.get(f"/tasks/{job_id}")).json()
        estado = job["status"]
        if not registro["estados_vistos"] or registro["estados_vistos"][-1] != estado:
            registro["estados_vistos"].append(estado)
            print(f"  [{time.monotonic() - inicio_global:7.1f}s] job {indice} ({job_id[:8]}) -> {estado}", flush=True)
        if estado == "WAITING_APPROVAL":
            # El "humano" revisa el borrador y aprueba (la espera queda medida aparte).
            t_aprob = time.monotonic()
            await asyncio.sleep(2)
            resp = await client.post(
                f"/tasks/{job_id}/approve",
                json={"aprobado": True, "comentario": "Aprobado en la prueba de carga", "revisor": "load_test"},
            )
            espera_humana += time.monotonic() - t_aprob
            registro["aprobaciones"].append({"status_code": resp.status_code, "motivos": job["aprobacion_pendiente"]["motivos"]})
        elif estado in ("DONE", "FAILED"):
            total = time.monotonic() - t0
            registro.update(
                status=estado,
                total_s=round(total, 2),
                computo_s=round(total - espera_humana, 2),
                espera_humana_s=round(espera_humana, 2),
                error=job.get("error"),
                respuesta=(job.get("resultado") or {}).get("respuesta"),
                ruta_supervisor=(job.get("resultado") or {}).get("ruta_supervisor"),
                reporte_publicado=(job.get("resultado") or {}).get("reporte_publicado"),
                eventos=job["eventos"],
            )
            return registro
        await asyncio.sleep(intervalo)
    registro.update(status="TIMEOUT", total_s=round(time.monotonic() - t0, 2))
    return registro


def resumen(jobs: list[dict[str, Any]], base_url: str, duracion: float) -> dict[str, Any]:
    terminados = [j for j in jobs if j["status"] == "DONE"]
    totales = [j["total_s"] for j in terminados]
    computo = [j["computo_s"] for j in terminados]
    posts = [j["post_ms"] for j in jobs]
    return {
        "fecha": datetime.now().isoformat(timespec="seconds"),
        "base_url": base_url,
        "concurrentes": len(jobs),
        "duracion_total_s": round(duracion, 1),
        "estados": {e: sum(1 for j in jobs if j["status"] == e) for e in sorted({j["status"] for j in jobs})},
        "pasaron_por_hitl": sum(1 for j in jobs if j["aprobaciones"]),
        "post_ms": {"p50": round(percentil(posts, 50), 1), "p95": round(percentil(posts, 95), 1), "max": max(posts)},
        "total_s": {"p50": round(percentil(totales, 50), 1), "p95": round(percentil(totales, 95), 1)},
        "computo_s": {"p50": round(percentil(computo, 50), 1), "p95": round(percentil(computo, 95), 1)},
    }


def a_markdown(res: dict[str, Any], jobs: list[dict[str, Any]]) -> str:
    lineas = [
        f"# Prueba de carga: {res['concurrentes']} peticiones concurrentes",
        "",
        f"- Fecha: {res['fecha']} · API: `{res['base_url']}` · duración total: {res['duracion_total_s']} s",
        f"- Estados finales: {res['estados']} · pasaron por aprobación humana: {res['pasaron_por_hitl']}",
        f"- POST /tasks (respuesta 202): p50 {res['post_ms']['p50']} ms · p95 {res['post_ms']['p95']} ms · máx {res['post_ms']['max']} ms",
        f"- Latencia total por job (POST -> DONE, incluye la espera de aprobación): p50 {res['total_s']['p50']} s · p95 {res['total_s']['p95']} s",
        f"- Tiempo de cómputo por job (sin la espera humana): p50 {res['computo_s']['p50']} s · p95 {res['computo_s']['p95']} s",
        "",
        "| # | job_id | crítica | estados vistos | total (s) | cómputo (s) | ruta del supervisor | publicado |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for j in jobs:
        lineas.append(
            f"| {j['indice']} | `{j['job_id'][:8]}` | {'sí' if j['aprobaciones'] else 'no'} | {' → '.join(j['estados_vistos'])} "
            f"| {j.get('total_s')} | {j.get('computo_s')} | {' → '.join(j.get('ruta_supervisor') or [])} | {j.get('reporte_publicado') or '-'} |"
        )
    lineas += ["", "Percentiles con interpolación lineal; con 5 muestras el p95 queda cerca del máximo."]
    return "\n".join(lineas) + "\n"


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("-n", type=int, default=5, help="peticiones concurrentes (máximo %d)" % len(TAREAS))
    parser.add_argument("--timeout", type=float, default=900, help="segundos máximos por job")
    parser.add_argument("--intervalo", type=float, default=1.0, help="segundos entre consultas de polling")
    parser.add_argument("--salida", default="load_test", help="nombre base de los archivos en docs/")
    args = parser.parse_args()

    tareas = TAREAS[: args.n]
    async with httpx.AsyncClient(base_url=args.base_url, timeout=30) as client:
        (await client.get("/health")).raise_for_status()
        print(f"Lanzando {len(tareas)} POST /tasks concurrentes contra {args.base_url}...", flush=True)
        inicio = time.monotonic()
        jobs = await asyncio.gather(
            *(correr_job(client, i + 1, t, inicio, args.timeout, args.intervalo) for i, t in enumerate(tareas))
        )
        duracion = time.monotonic() - inicio

    res = resumen(list(jobs), args.base_url, duracion)
    DOCS.mkdir(exist_ok=True)
    (DOCS / f"{args.salida}.json").write_text(
        json.dumps({"resumen": res, "jobs": jobs}, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (DOCS / f"{args.salida}.md").write_text(a_markdown(res, list(jobs)), encoding="utf-8")
    print("\n" + a_markdown(res, list(jobs)))
    print(f"Detalle en docs/{args.salida}.json y docs/{args.salida}.md")


if __name__ == "__main__":
    asyncio.run(main())
