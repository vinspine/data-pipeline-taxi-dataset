FROM python:3.13.11-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

WORKDIR /code
ENV PATH="/code/.venv/bin:$PATH"
ENV DB_HOST=localhost
ENV DB_PORT=5432
ENV DB_USER=root
ENV DB_PASSWORD=root
ENV DB_NAME=ny_taxi
ENV DB_TABLE=yellow_taxi_data
ENV DB_YEAR=2019
ENV DB_MONTH=01

COPY pyproject.toml .python-version uv.lock ./
RUN uv sync --locked

COPY ingest_data.py .
COPY main.py .

ENTRYPOINT ["uv", "run", "python", "main.py"]