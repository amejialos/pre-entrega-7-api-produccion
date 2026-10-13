# Imagen de la API (despliegue inicial). La levanta `docker compose --profile app up -d`.
FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:0.12.5 /uv /uvx /bin/

WORKDIR /srv
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PYTHONUNBUFFERED=1

# Primero solo las dependencias (capa cacheada mientras no cambie uv.lock).
COPY pyproject.toml uv.lock .python-version ./
RUN uv sync --frozen --no-dev

COPY app ./app
COPY data ./data

EXPOSE 8000
# Un solo proceso: la API y el worker embebido (WORKER_CONCURRENCY jobs a la vez).
CMD ["uv", "run", "--no-sync", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
