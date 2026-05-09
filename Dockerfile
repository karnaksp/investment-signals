FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

ENV PROTO_DIR=/app/proto

RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md ./
COPY proto ./proto
COPY src ./src
COPY conf ./conf
COPY sql ./sql

ARG INVEST_PYTHON_REF=0.2.0-beta117
RUN rm -rf /app/src/tinkoff \
    && git clone --depth 1 --branch "${INVEST_PYTHON_REF}" \
        https://github.com/RussianInvestments/invest-python.git /tmp/invest-python \
    && cp -a /tmp/invest-python/tinkoff /app/src/tinkoff \
    && rm -rf /tmp/invest-python

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir --retries 20 --timeout 60 "setuptools>=69" wheel && \
    pip install --no-cache-dir --no-build-isolation ".[orchestration]"

CMD ["tinvest-api"]

