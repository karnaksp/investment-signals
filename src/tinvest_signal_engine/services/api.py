"""HTTP API поверх Postgres: последние сигналы и сводки по типам."""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import FastAPI, Query
from pydantic import BaseModel, Field
import uvicorn

from ..config import RuntimeSettings
from ..logging_utils import configure_logging
from ..sinks import create_postgres_signal_store_with_retry


class HealthResponse(BaseModel):
    """Ответ проверки живости процесса (без запроса к Postgres)."""

    status: str = Field(
        description="Обычно `ok`, если процесс принимает HTTP.",
    )


class RecentSignalsResponse(BaseModel):
    """Список последних сигналов из таблицы Postgres."""

    items: list[dict[str, Any]] = Field(
        description=(
            "Записи сигналов в том же виде, что возвращает хранилище."
        ),
    )
    count: int = Field(description="Длина списка `items`.")


class SignalSummaryRow(BaseModel):
    """Одна строка агрегированной статистики по типу сигнала."""

    signal_type: str = Field(
        description="Имя типа сигнала (`signal_type`).",
    )
    signal_count: int = Field(
        description="Число срабатываний за окно.",
    )


class SignalSummaryResponse(BaseModel):
    """Сводка по типам сигналов за последние `minutes` минут."""

    items: list[SignalSummaryRow] = Field(
        description=(
            "Строки сводки, отсортированные по убыванию счётчика."
        ),
    )
    minutes: int = Field(
        description="Размер временного окна запроса в минутах.",
    )


def create_app() -> FastAPI:
    settings = RuntimeSettings.from_env()
    configure_logging(settings.log_level)
    fastapi_app = FastAPI(
        title="T-Invest Signal API",
        version="0.1.0",
        description=(
            "Чтение накопленных аномалий рынка (сигналов), "
            "записанных сервисом детектора в Postgres. "
            "Источник данных — T-Invest MarketDataStream → Kafka → детектор."
        ),
        openapi_tags=[
            {
                "name": "health",
                "description": "Проверка доступности HTTP-сервиса.",
            },
            {
                "name": "signals",
                "description": "Выборки и агрегаты по таблице сигналов.",
            },
        ],
    )

    @fastapi_app.on_event("startup")
    def startup() -> None:
        fastapi_app.state.settings = settings
        fastapi_app.state.signal_store = create_postgres_signal_store_with_retry(
            settings,
            service_name="api",
        )

    @fastapi_app.on_event("shutdown")
    def shutdown() -> None:
        signal_store = getattr(fastapi_app.state, "signal_store", None)
        if signal_store is not None:
            signal_store.close()

    @fastapi_app.get(
        "/health",
        tags=["health"],
        summary="Проверка живости",
        response_model=HealthResponse,
        responses={200: {"description": "Сервис принимает запросы."}},
    )
    def health() -> HealthResponse:
        """Возвращает статус без обращения к базе данных."""
        return HealthResponse(status="ok")

    @fastapi_app.get(
        "/signals/recent",
        tags=["signals"],
        summary="Последние сигналы",
        response_model=RecentSignalsResponse,
        responses={
            200: {"description": "Выборка из Postgres по убыванию времени."},
        },
    )
    def recent_signals(
        limit: Annotated[
            int,
            Query(
                ge=1,
                le=500,
                description="Максимум строк (ограничено и на стороне SQL).",
            ),
        ] = 50,
        instrument_id: Annotated[
            str | None,
            Query(
                description=(
                    "Фильтр по `instrument_id` (например `SBER_TQBR`). "
                    "Если не задан — все инструменты."
                ),
            ),
        ] = None,
    ) -> RecentSignalsResponse:
        """Последние сигналы; опционально фильтр по инструменту."""
        rows = fastapi_app.state.signal_store.fetch_recent(
            limit=limit, instrument_id=instrument_id
        )
        return RecentSignalsResponse(items=rows, count=len(rows))

    @fastapi_app.get(
        "/signals/summary",
        tags=["signals"],
        summary="Сводка по типам сигналов",
        response_model=SignalSummaryResponse,
        responses={
            200: {
                "description": (
                    "Группировка COUNT по `signal_type` за окно времени."
                ),
            },
        },
    )
    def signal_summary(
        minutes: Annotated[
            int,
            Query(
                ge=1,
                le=1440,
                description=(
                    "Окно в минутах от текущего момента "
                    "(UTC на стороне БД)."
                ),
            ),
        ] = 60,
    ) -> SignalSummaryResponse:
        """COUNT по каждому `signal_type` за указанный период."""
        raw_rows = fastapi_app.state.signal_store.fetch_summary(minutes=minutes)
        items = [SignalSummaryRow(**row) for row in raw_rows]
        return SignalSummaryResponse(items=items, minutes=minutes)

    return fastapi_app


app = create_app()


def main() -> None:
    settings = RuntimeSettings.from_env()
    uvicorn.run(app, host=settings.api_host, port=settings.api_port)
