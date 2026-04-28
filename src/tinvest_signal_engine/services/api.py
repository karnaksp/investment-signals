from __future__ import annotations

from fastapi import FastAPI, Query
import uvicorn

from ..config import RuntimeSettings
from ..logging_utils import configure_logging
from ..sinks import create_postgres_signal_store_with_retry


def create_app() -> FastAPI:
    settings = RuntimeSettings.from_env()
    configure_logging(settings.log_level)
    app = FastAPI(
        title="T-Invest Signal API",
        version="0.1.0",
        description=(
            "Realtime anomaly signals produced from T-Invest market data."
        ),
    )

    @app.on_event("startup")
    def startup() -> None:
        app.state.settings = settings
        app.state.signal_store = create_postgres_signal_store_with_retry(
            settings,
            service_name="api",
        )

    @app.on_event("shutdown")
    def shutdown() -> None:
        signal_store = getattr(app.state, "signal_store", None)
        if signal_store is not None:
            signal_store.close()

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/signals/recent")
    def recent_signals(
        limit: int = Query(default=50, ge=1, le=500),
        instrument_id: str | None = None,
    ) -> dict[str, object]:
        rows = app.state.signal_store.fetch_recent(
            limit=limit, instrument_id=instrument_id
        )
        return {"items": rows, "count": len(rows)}

    @app.get("/signals/summary")
    def signal_summary(
        minutes: int = Query(default=60, ge=1, le=1440)
    ) -> dict[str, object]:
        return {
            "items": app.state.signal_store.fetch_summary(minutes=minutes),
            "minutes": minutes,
        }

    return app


app = create_app()


def main() -> None:
    settings = RuntimeSettings.from_env()
    uvicorn.run(app, host=settings.api_host, port=settings.api_port)
