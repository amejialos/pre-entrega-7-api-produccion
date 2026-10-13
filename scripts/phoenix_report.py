"""Lee de Phoenix (API GraphQL) el costo y la latencia de las corridas y arma un reporte.

    uv run python scripts/phoenix_report.py                    # proyecto pe7-api-agente
    uv run python scripts/phoenix_report.py --proyecto otro --salida phoenix_report

Es lo mismo que muestra el dashboard de Phoenix (las capturas están en `screenshots/`), en
texto: sirve para citar números exactos en el README y para ver DÓNDE se concentra la
latencia y QUÉ NODO consume más tokens (suma de los spans LLM por nodo del grafo).

- Costo por ejecución: Phoenix lo calcula por span LLM (tokens de entrada/salida x precio del
  modelo en su tabla) y lo suma por traza. Una traza = una corrida del grafo (la reanudación
  tras la aprobación humana es otra traza, de la misma sesión/job).
- Latencia p50/p95/p99: `latencyMsQuantile` del proyecto (sobre las trazas).
"""

import argparse
import json
import statistics
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx

DOCS = Path(__file__).resolve().parent.parent / "docs"
NODOS = ("supervisor", "researcher", "analyst", "synthesizer", "evaluar_criticidad", "aprobacion_humana", "publicar_reporte")


def gql(url: str, query: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
    r = httpx.post(f"{url}/graphql", json={"query": query, "variables": variables or {}}, timeout=60)
    r.raise_for_status()
    datos = r.json()
    if datos.get("errors"):
        raise RuntimeError(datos["errors"])
    return datos["data"]


def proyecto(url: str, nombre: str) -> dict[str, Any]:
    datos = gql(
        url,
        """{ projects { edges { node { id name traceCount
              costSummary { total { cost tokens } prompt { cost tokens } completion { cost tokens } }
              p50: latencyMsQuantile(probability: 0.5)
              p95: latencyMsQuantile(probability: 0.95)
              p99: latencyMsQuantile(probability: 0.99) } } } }""",
    )
    for edge in datos["projects"]["edges"]:
        if edge["node"]["name"] == nombre:
            return edge["node"]
    raise SystemExit(f"No existe el proyecto {nombre!r} en Phoenix")


def spans(url: str, project_id: str) -> list[dict[str, Any]]:
    """Todos los spans del proyecto (paginado)."""
    query = """query($id: ID!, $after: String) { node(id: $id) { ... on Project {
        spans(first: 500, after: $after) { pageInfo { hasNextPage endCursor }
          edges { node { name spanKind spanId parentId latencyMs startTime
                         tokenCountPrompt tokenCountCompletion
                         costSummary { total { cost } }
                         metadata context { traceId } } } } } } }"""
    resultado, cursor = [], None
    while True:
        pagina = gql(url, query, {"id": project_id, "after": cursor})["node"]["spans"]
        resultado += [e["node"] for e in pagina["edges"]]
        if not pagina["pageInfo"]["hasNextPage"]:
            return resultado
        cursor = pagina["pageInfo"]["endCursor"]


def nodo_del_grafo(span: dict[str, Any], por_id: dict[str, dict[str, Any]]) -> str:
    """Sube por los padres hasta encontrar el nodo del orquestador al que pertenece el span."""
    actual: dict[str, Any] | None = span
    while actual is not None:
        if actual["name"] in NODOS:
            return actual["name"]
        actual = por_id.get(actual["parentId"]) if actual["parentId"] else None
    return "(fuera de un nodo)"


def analizar(url: str, nombre: str) -> dict[str, Any]:
    proj = proyecto(url, nombre)
    todos = spans(url, proj["id"])
    por_id = {s["spanId"]: s for s in todos}
    raices = sorted((s for s in todos if not s["parentId"]), key=lambda s: s["startTime"])

    costo_por_traza: dict[str, float] = defaultdict(float)
    tokens_por_traza: dict[str, list[int]] = defaultdict(lambda: [0, 0])
    por_nodo: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for s in todos:
        traza = s["context"]["traceId"]
        if s["spanKind"].upper() == "LLM":
            costo = ((s["costSummary"] or {}).get("total") or {}).get("cost") or 0.0
            costo_por_traza[traza] += costo
            tokens_por_traza[traza][0] += s["tokenCountPrompt"] or 0
            tokens_por_traza[traza][1] += s["tokenCountCompletion"] or 0
            nodo = por_nodo[nodo_del_grafo(s, por_id)]
            nodo["llamadas_llm"] += 1
            nodo["tokens_entrada"] += s["tokenCountPrompt"] or 0
            nodo["tokens_salida"] += s["tokenCountCompletion"] or 0
            nodo["costo_usd"] += costo
            nodo["latencia_llm_s"] += (s["latencyMs"] or 0) / 1000
        if s["name"] in NODOS and s["parentId"] and por_id.get(s["parentId"], {}).get("parentId") is None:
            por_nodo[s["name"]]["latencia_nodo_s"] += (s["latencyMs"] or 0) / 1000
            por_nodo[s["name"]]["ejecuciones"] += 1

    trazas = []
    for r in raices:
        meta = json.loads(r["metadata"]) if isinstance(r["metadata"], str) else (r["metadata"] or {})
        traza = r["context"]["traceId"]
        trazas.append(
            {
                "trace_id": traza,
                "nombre": r["name"],
                "job_id": meta.get("job_id"),
                "fase": meta.get("fase"),
                "inicio": r["startTime"],
                "latencia_s": round((r["latencyMs"] or 0) / 1000, 2),
                "tokens_entrada": tokens_por_traza[traza][0],
                "tokens_salida": tokens_por_traza[traza][1],
                "costo_usd": round(costo_por_traza[traza], 6),
            }
        )
    # Costo por EJECUCIÓN (job): corrida inicial + reanudación tras la aprobación.
    por_job: dict[str, float] = defaultdict(float)
    for t in trazas:
        por_job[t["job_id"] or t["trace_id"]] += t["costo_usd"]
    return {
        "fecha": datetime.now().isoformat(timespec="seconds"),
        "proyecto": nombre,
        "trazas_en_proyecto": proj["traceCount"],
        "costo_total_usd": proj["costSummary"]["total"]["cost"],
        "tokens_totales": proj["costSummary"]["total"]["tokens"],
        "latencia_ms": {"p50": proj["p50"], "p95": proj["p95"], "p99": proj["p99"]},
        "costo_por_job_usd": {k: round(v, 6) for k, v in por_job.items()},
        "costo_promedio_por_job_usd": round(statistics.mean(por_job.values()), 6) if por_job else None,
        "trazas": trazas,
        "por_nodo": {k: {m: round(v, 6) for m, v in d.items()} for k, d in por_nodo.items()},
    }


def a_markdown(rep: dict[str, Any]) -> str:
    lat = rep["latencia_ms"]
    lineas = [
        f"# Costo y latencia según Phoenix (proyecto `{rep['proyecto']}`)",
        "",
        f"- Leído de la API de Phoenix el {rep['fecha']}. Trazas: {rep['trazas_en_proyecto']}.",
        f"- Costo total: US$ {rep['costo_total_usd']:.6f} · tokens: {rep['tokens_totales']:.0f}",
        f"- Costo promedio por ejecución (job = corrida + reanudación): US$ {rep['costo_promedio_por_job_usd']:.6f}",
        f"- Latencia de las trazas: p50 {lat['p50'] / 1000:.1f} s · **p95 {lat['p95'] / 1000:.1f} s** · p99 {lat['p99'] / 1000:.1f} s",
        "",
        "## Por traza",
        "",
        "| inicio (UTC) | traza | job | fase | latencia (s) | tokens in/out | costo (US$) |",
        "|---|---|---|---|---|---|---|",
    ]
    for t in rep["trazas"]:
        lineas.append(
            f"| {t['inicio'][11:19]} | `{t['trace_id'][:8]}` | `{(t['job_id'] or '-')[:8]}` | {t['fase'] or '-'} "
            f"| {t['latencia_s']} | {t['tokens_entrada']}/{t['tokens_salida']} | {t['costo_usd']:.6f} |"
        )
    lineas += [
        "",
        "## Por nodo del grafo (dónde se concentra la latencia y qué consume tokens)",
        "",
        "| nodo | ejecuciones | latencia total del nodo (s) | llamadas LLM | tokens in/out | costo (US$) |",
        "|---|---|---|---|---|---|",
    ]
    for nodo, d in sorted(rep["por_nodo"].items(), key=lambda kv: -kv[1].get("costo_usd", 0)):
        lineas.append(
            f"| {nodo} | {d.get('ejecuciones', 0):.0f} | {d.get('latencia_nodo_s', 0):.1f} | {d.get('llamadas_llm', 0):.0f} "
            f"| {d.get('tokens_entrada', 0):.0f}/{d.get('tokens_salida', 0):.0f} | {d.get('costo_usd', 0):.6f} |"
        )
    return "\n".join(lineas) + "\n"


def a_html(rep: dict[str, Any]) -> str:
    """Mismo reporte como página HTML (para la captura del costo exacto por ejecución:
    la UI de Phoenix redondea a centavos y muestra "<$0.01")."""
    lat = rep["latencia_ms"]
    por_job: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for t in rep["trazas"]:
        por_job[t["job_id"] or t["trace_id"]].append(t)
    filas = "".join(
        f"<tr><td><code>{job[:8]}</code></td><td>{len(ts)}</td>"
        f"<td>{' + '.join(t['fase'] or '-' for t in ts)}</td>"
        f"<td>{sum(t['latencia_s'] for t in ts):.1f}</td>"
        f"<td>{sum(t['tokens_entrada'] for t in ts):,} / {sum(t['tokens_salida'] for t in ts):,}</td>"
        f"<td><b>US$ {rep['costo_por_job_usd'][job]:.6f}</b></td></tr>"
        for job, ts in por_job.items()
    )
    nodos = "".join(
        f"<tr><td>{n}</td><td>{d.get('llamadas_llm', 0):.0f}</td><td>{d.get('latencia_nodo_s', 0):.1f}</td>"
        f"<td>{d.get('tokens_entrada', 0):,.0f} / {d.get('tokens_salida', 0):,.0f}</td><td>US$ {d.get('costo_usd', 0):.6f}</td></tr>"
        for n, d in sorted(rep["por_nodo"].items(), key=lambda kv: -kv[1].get("costo_usd", 0))
        if d.get("llamadas_llm")
    )
    return f"""<!doctype html><html lang="es"><head><meta charset="utf-8"><title>Costo por ejecución</title>
<style>body{{font-family:system-ui,sans-serif;margin:24px;color:#1b1b1f;background:#fff}}
table{{border-collapse:collapse;margin:8px 0 20px}}td,th{{border:1px solid #ccd;padding:6px 12px;text-align:left}}
th{{background:#eef}}.kpi{{display:inline-block;margin:0 24px 12px 0;padding:10px 16px;border:1px solid #ccd;border-radius:8px}}
.kpi b{{display:block;font-size:22px}}small{{color:#555}}</style></head><body>
<h2>Costo por ejecución y latencia: proyecto <code>{rep['proyecto']}</code> en Phoenix</h2>
<small>Datos leídos de la API GraphQL de Phoenix (costSummary, latencyMsQuantile) el {rep['fecha']}.
Costo = tokens de entrada/salida x precio por modelo de la tabla de Phoenix.</small>
<p><span class="kpi">Costo promedio por ejecución<b>US$ {rep['costo_promedio_por_job_usd']:.6f}</b></span>
<span class="kpi">Costo total ({len(por_job)} ejecuciones)<b>US$ {rep['costo_total_usd']:.6f}</b></span>
<span class="kpi">Latencia p50<b>{lat['p50'] / 1000:.1f} s</b></span>
<span class="kpi">Latencia p95<b>{lat['p95'] / 1000:.1f} s</b></span></p>
<h3>Por ejecución (job = corrida inicial + reanudación tras la aprobación)</h3>
<table><tr><th>job</th><th>trazas</th><th>fases</th><th>latencia (s)</th><th>tokens in / out</th><th>costo</th></tr>{filas}</table>
<h3>Por nodo del grafo</h3>
<table><tr><th>nodo</th><th>llamadas LLM</th><th>latencia total (s)</th><th>tokens in / out</th><th>costo</th></tr>{nodos}</table>
</body></html>"""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--url", default="http://localhost:6006")
    parser.add_argument("--proyecto", default="pe7-api-agente")
    parser.add_argument("--salida", default="phoenix_report")
    args = parser.parse_args()
    rep = analizar(args.url, args.proyecto)
    DOCS.mkdir(exist_ok=True)
    (DOCS / f"{args.salida}.json").write_text(json.dumps(rep, ensure_ascii=False, indent=2), encoding="utf-8")
    (DOCS / f"{args.salida}.md").write_text(a_markdown(rep), encoding="utf-8")
    (DOCS / f"{args.salida}.html").write_text(a_html(rep), encoding="utf-8")
    print(a_markdown(rep))


if __name__ == "__main__":
    main()
