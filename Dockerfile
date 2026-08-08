ARG INVEST_PYTHON_REF=0.2.0-beta117

FROM python:3.12-slim AS invest-source
ARG INVEST_PYTHON_REF
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && git clone --depth 1 --branch "${INVEST_PYTHON_REF}" \
        https://github.com/RussianInvestments/invest-python.git /tmp/invest-python

FROM python:3.12-slim

ARG APP_VERSION=0.2.0
ARG APP_COMMIT_SHA=unknown
ARG APP_BUILD_TIME=unknown
ARG APP_EXTRAS=replay

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV GRPC_DEFAULT_SSL_ROOTS_FILE_PATH=/etc/ssl/certs/ca-certificates.crt
ENV APP_VERSION=${APP_VERSION}
ENV APP_COMMIT_SHA=${APP_COMMIT_SHA}
ENV APP_BUILD_TIME=${APP_BUILD_TIME}

WORKDIR /app

ENV PROTO_DIR=/app/proto

COPY pyproject.toml README.md ./
COPY certs/russian-trusted-root-ca.crt /usr/local/share/ca-certificates/russian-trusted-root-ca.crt
COPY proto ./proto
COPY src ./src
COPY --from=invest-source /tmp/invest-python/tinkoff ./src/tinkoff
COPY conf ./conf
COPY config/scientific_hypotheses ./config/scientific_hypotheses
COPY sql ./sql

RUN update-ca-certificates && \
    pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir --retries 20 --timeout 60 "setuptools>=69" wheel && \
    pip install --no-cache-dir --no-build-isolation ".[${APP_EXTRAS}]" && \
    groupadd --gid 10001 tinvest && \
    useradd --uid 10001 --gid tinvest --no-create-home --home-dir /nonexistent \
        --shell /usr/sbin/nologin tinvest

USER 10001:10001

CMD ["tinvest-api"]
