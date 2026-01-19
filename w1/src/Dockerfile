FROM python:3.13.11-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

WORKDIR /code
ENV PATH="/code/.venv/bin/:$PATH"
ENV UV_PROJECT_ENVIRONMENT="/code/.venv"

COPY pyproject.toml uv.lock ./
RUN uv venv /code/.venv
RUN uv sync --locked

COPY ./ .
# copy ca thu muc, .venv o local se ghi de .venv duoc tao trong container => .dockerignore

ENTRYPOINT ["python", "src/main.py"]