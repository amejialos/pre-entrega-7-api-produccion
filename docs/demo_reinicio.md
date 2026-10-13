# Demo: la pausa HITL sobrevive a un reinicio de la API y de Redis

Corrida real del 2026-10-12 contra Gemini + Pinecone (proyecto de Phoenix `pe7-demo-reinicio`, aparte del de la prueba de carga).

1. Se crea una tarea que la pregunta marca como crítica ("Publicá el reporte") y el job queda en WAITING_APPROVAL.
2. Se apaga la API y se reinicia el contenedor de Redis (AOF activado en el compose).
3. Una API NUEVA (otro proceso) ve el job en WAITING_APPROVAL; la aprobación reanuda el hilo desde el checkpoint en Redis y publica.
4. En el log del proceso nuevo hay **0** llamadas a `chat/completions`: nada se recalculó, el estado del grafo salió del checkpoint.

```text
$ curl -X POST localhost:8000/tasks -d {"pregunta": "...¿qué significa el error CAU-503? Publicá el reporte."}
{"job_id":"06f0f5ec0d7f48f5bb5cdd2c9da21fc9","status":"PENDING","links":{"estado":"/tasks/06f0f5ec0d7f48f5bb5cdd2c9da21fc9"}}
21:57:18 estado: WAITING_APPROVAL
{"status": "WAITING_APPROVAL", "aprobacion_pendiente": {"accion": "publicar_reporte", "motivos": ["La pregunta pide una acción hacia afuera ('publica'): se ejecutaría 'publicar_reporte'."]}}
21:57:30 # se apaga la API (pkill uvicorn) y se REINICIA el contenedor de Redis
 Container pe7-api-agente-redis-1 Restarting 
 Container pe7-api-agente-redis-1 Started 
$ redis-cli --scan --pattern "checkpoint:06f0f5ec0d7f48f5bb5cdd2c9da21fc9:*" | wc -l
12
21:57:42 # API nueva (otro proceso): el job sigue esperando
estado: WAITING_APPROVAL
$ curl -X POST localhost:8000/tasks/06f0f5ec0d7f48f5bb5cdd2c9da21fc9/approve -d {"aprobado": true, "comentario": "ok tras reinicio", "revisor": "demo"}
{"job_id":"06f0f5ec0d7f48f5bb5cdd2c9da21fc9","status":"PENDING","aprobado":true,"links":{"estado":"/tasks/06f0f5ec0d7f48f5bb5cdd2c9da21fc9"}}
estado: DONE
reporte_publicado: pe7:reporte:06f0f5ec0d7f48f5bb5cdd2c9da21fc9
ruta_supervisor: ['researcher', 'FINISH']
eventos: ['PENDING', 'RUNNING', 'WAITING_APPROVAL', 'PENDING', 'RUNNING', 'DONE']
respuesta: **Reporte de Análisis: Error CAU-503**

A continuación, se detalla la información técnica recopilada sobre el error solicitado:

*   **Significado del error CAU-503:** Se clasifica como un error interno o de estado. Ocurre cuando el consumo de memoria aumenta de forma continua hasta que el sistema o
```
