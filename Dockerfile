# syntax=docker/dockerfile:1.7@sha256:a57df69d0ea827fb7266491f2813635de6f17269be881f696fbfdf2d83dda33e

ARG UBUNTU_IMAGE=ubuntu:26.04@sha256:678c6550cc43645e08669028bc177f50be4e7c5b8cca677067b1914d4afc7a03
ARG INVEST_PYTHON_REF=0.2.0-beta117

FROM ${UBUNTU_IMAGE} AS invest-source
ARG INVEST_PYTHON_REF
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates git \
    && git clone --depth 1 --branch "${INVEST_PYTHON_REF}" \
        https://github.com/RussianInvestments/invest-python.git /tmp/invest-python \
    && rm -rf /var/lib/apt/lists/*

FROM ${UBUNTU_IMAGE} AS build

ARG APP_EXTRAS=replay

ENV PIP_DISABLE_PIP_VERSION_CHECK=1
ENV PIP_NO_CACHE_DIR=1
ENV PYTHONDONTWRITEBYTECODE=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        python3 \
        python3-venv \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

COPY pyproject.toml README.md LICENSE ./
COPY proto ./proto
COPY src ./src
COPY --from=invest-source /tmp/invest-python/tinkoff ./src/tinkoff

# Only the virtual environment crosses the runtime boundary. Pip and its
# vendored build dependencies are removed after the wheel-only install.
RUN python3 -m venv /opt/venv \
    && /opt/venv/bin/pip install --upgrade pip "setuptools>=69" wheel \
    && /opt/venv/bin/pip install --only-binary=:all: ".[${APP_EXTRAS}]" \
    && /opt/venv/bin/pip uninstall --yes pip setuptools wheel

FROM ${UBUNTU_IMAGE} AS runtime

ARG APP_VERSION=0.2.0
ARG APP_COMMIT_SHA=unknown
ARG APP_BUILD_TIME=unknown

ENV APP_BUILD_TIME=${APP_BUILD_TIME}
ENV APP_COMMIT_SHA=${APP_COMMIT_SHA}
ENV APP_VERSION=${APP_VERSION}
ENV GRPC_DEFAULT_SSL_ROOTS_FILE_PATH=/etc/ssl/certs/ca-certificates.crt
ENV PATH=/opt/venv/bin:$PATH
ENV PROTO_DIR=/app/proto
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        passwd \
        python3 \
    && groupadd --gid 10001 tinvest \
    && useradd --uid 10001 --gid tinvest --no-create-home \
        --home-dir /nonexistent --shell /usr/sbin/nologin tinvest \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=build --chown=10001:10001 /opt/venv /opt/venv
COPY --chown=10001:10001 certs/russian-trusted-root-ca.crt /usr/local/share/ca-certificates/russian-trusted-root-ca.crt
COPY --chown=10001:10001 proto ./proto
COPY --chown=10001:10001 conf ./conf
COPY --chown=10001:10001 config/scientific_hypotheses ./config/scientific_hypotheses
COPY --chown=10001:10001 sql ./sql

RUN update-ca-certificates

USER 10001:10001

CMD ["tinvest-api"]
