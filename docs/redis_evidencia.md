# Evidencia: checkpoints y estado de jobs en Redis

Capturado el 2026-10-12 21:55 con docker exec pe7-api-agente-redis-1 redis-cli, después de la prueba de carga.

```text
$ redis-cli --scan --pattern "*checkpoint*" | sort | head -15
checkpoint:2256c2d6666a43538c91df6d2e941cd0:__empty__:1f1c69e3-27aa-6898-bfff-7d3aed4a3239
checkpoint:2256c2d6666a43538c91df6d2e941cd0:__empty__:1f1c69e3-27d9-62ce-8000-2ce7ea73a29f
checkpoint:2256c2d6666a43538c91df6d2e941cd0:__empty__:1f1c69e3-c1c2-6020-8001-98958fb9fb48
checkpoint:2256c2d6666a43538c91df6d2e941cd0:__empty__:1f1c69e7-ba5b-6cc4-8002-b0d8b46524f4
checkpoint:2256c2d6666a43538c91df6d2e941cd0:__empty__:1f1c69e7-c5c6-65d2-8003-795826bd0d28
checkpoint:2256c2d6666a43538c91df6d2e941cd0:__empty__:1f1c69e9-7ec5-61b8-8004-3516ad1c9cc2
checkpoint:2256c2d6666a43538c91df6d2e941cd0:__empty__:1f1c69e9-89a7-6360-8005-feb3a2105055
checkpoint:2256c2d6666a43538c91df6d2e941cd0:__empty__:1f1c69ea-1732-6342-8006-71b41e7b2c49
checkpoint:2256c2d6666a43538c91df6d2e941cd0:__empty__:1f1c69ea-1779-61fc-8007-34565738b361
checkpoint:2256c2d6666a43538c91df6d2e941cd0:__empty__:1f1c69ea-3584-61f6-8008-219c0f2a93c0
checkpoint:2256c2d6666a43538c91df6d2e941cd0:__empty__:1f1c69ea-358e-68fe-8009-91ba25125ae3
checkpoint:2256c2d6666a43538c91df6d2e941cd0:analyst:b49b84db-0d73-d120-c1e9-542a365f4a56:1f1c69e7-c5d0-6546-bfff-d6d95fac12f5
checkpoint:2256c2d6666a43538c91df6d2e941cd0:analyst:b49b84db-0d73-d120-c1e9-542a365f4a56:1f1c69e7-c5d2-690e-8000-11876e41e932
checkpoint:2256c2d6666a43538c91df6d2e941cd0:analyst:b49b84db-0d73-d120-c1e9-542a365f4a56:1f1c69e8-784a-6bcc-8001-cd0a7b3d9dbd
checkpoint:2256c2d6666a43538c91df6d2e941cd0:analyst:b49b84db-0d73-d120-c1e9-542a365f4a56:1f1c69e8-7876-6588-8002-9c7cecb5a4c6

$ redis-cli --scan --pattern "checkpoint:*" | wc -l   # checkpoints guardados
111

$ redis-cli --scan --pattern "checkpoint:45e67ab9a9614e92a8522fa594064f4b:*" | wc -l   # un hilo (= job)
23

$ redis-cli FT._LIST   # índices de RediSearch creados por AsyncRedisSaver.asetup()
checkpoint_write
checkpoint

$ redis-cli --scan --pattern "pe7:*" | sort | head -20
pe7:job:2256c2d6666a43538c91df6d2e941cd0
pe7:job:2256c2d6666a43538c91df6d2e941cd0:eventos
pe7:job:45e67ab9a9614e92a8522fa594064f4b
pe7:job:45e67ab9a9614e92a8522fa594064f4b:eventos
pe7:job:4dfe60c477754b4886b9157479a60f65
pe7:job:4dfe60c477754b4886b9157479a60f65:eventos
pe7:job:722c0eeea5a14ae78415dcee0ae157be
pe7:job:722c0eeea5a14ae78415dcee0ae157be:eventos
pe7:job:7a3d6da1c5494cadada50fa0cc6a6a7b
pe7:job:7a3d6da1c5494cadada50fa0cc6a6a7b:eventos
pe7:job:8c42b114039b41999ed9493d71b21af8
pe7:job:8c42b114039b41999ed9493d71b21af8:eventos
pe7:jobs
pe7:reporte:2256c2d6666a43538c91df6d2e941cd0
pe7:reporte:45e67ab9a9614e92a8522fa594064f4b
pe7:reporte:7a3d6da1c5494cadada50fa0cc6a6a7b
pe7:reportes_publicados

$ redis-cli HMGET pe7:job:45e67ab9a9614e92a8522fa594064f4b status created_at started_at finished_at
DONE
2026-10-13T00:36:57.854+00:00
2026-10-13T00:36:57.900+00:00
2026-10-13T00:40:55.250+00:00

$ redis-cli LRANGE pe7:job:45e67ab9a9614e92a8522fa594064f4b:eventos 0 -1
{"status": "PENDING", "t": "2026-10-13T00:36:57.854+00:00"}
{"status": "RUNNING", "t": "2026-10-13T00:36:57.900+00:00"}
{"status": "WAITING_APPROVAL", "t": "2026-10-13T00:40:52.974+00:00"}
{"status": "PENDING", "t": "2026-10-13T00:40:55.218+00:00", "por": "approve"}
{"status": "RUNNING", "t": "2026-10-13T00:40:55.223+00:00"}
{"status": "DONE", "t": "2026-10-13T00:40:55.250+00:00"}

$ redis-cli LRANGE pe7:reportes_publicados 0 -1
45e67ab9a9614e92a8522fa594064f4b
2256c2d6666a43538c91df6d2e941cd0
7a3d6da1c5494cadada50fa0cc6a6a7b
```
