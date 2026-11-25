FROM python:3.12-slim AS builder

WORKDIR /build
COPY pyproject.toml README.md LICENSE ./
COPY src/ ./src/
RUN pip install --no-cache-dir --prefix=/install .

FROM python:3.12-slim

RUN useradd --create-home --shell /bin/bash flowguard
WORKDIR /app

COPY --from=builder /install /usr/local
COPY examples/ ./examples/

USER flowguard
EXPOSE 8888

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8888/health')" || exit 1

CMD ["uvicorn", "examples.demo_app:app", "--host", "0.0.0.0", "--port", "8888"]
