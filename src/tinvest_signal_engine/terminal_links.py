"""Ссылки на карточки инструментов на сайте Т-Банк Инвестиции (веб)."""

from __future__ import annotations

from urllib.parse import quote

BASE = "https://www.tbank.ru/invest"
WEB_TERMINAL = "https://www.tbank.ru/terminal/"


def t_invest_terminal_open_chart_url(
    *,
    ticker: str,
    instrument_uid: str | None = None,
    class_code: str | None = None,
) -> str:
    """
    Deeplink в **веб-терминал** с открытием виджета «График» (TradingView) по инструменту.

    Логика соответствует фронту invest-terminal (парсинг ``location.search``): параметры
    ``workspace``, ``widget_name``, ``widget_settings``. В ``widget_settings`` через запятую
    передаются пары ключ/значение; для графика задаётся ``symbolId,<тикер>`` либо
    ``instrumentUid,<uid>`` (если известен UID инструмента из API — надёжнее для части тикеров).

    Публичной страницы с контрактом URL у Т-Банка нет; при смене терминала формат может поменяться.
    """
    raw = (ticker or "").strip()
    cc = (class_code or "").strip().upper()
    t = raw if cc == "SPBFUT" else raw.upper()
    uid = (instrument_uid or "").strip()
    if not t and not uid:
        return WEB_TERMINAL
    # Запятая в значении widget_settings — разделитель пар; не кодируем её в %2C.
    if uid:
        pair = f"instrumentUid,{uid}"
    else:
        pair = f"symbolId,{t}"
    q = (
        "workspace=new_tab"
        "&widget_name=CHART_TV"
        f"&widget_settings={quote(pair, safe=',')}"
    )
    return f"{WEB_TERMINAL}?{q}"


def t_invest_terminal_search_url(*, ticker: str, class_code: str | None = None) -> str:
    """
    Ссылка «открыть график в веб-терминале» (deeplink на виджет CHART_TV).

    Раньше использовался несуществующий для терминала ``?search=``; см. ``t_invest_terminal_open_chart_url``.
    """
    return t_invest_terminal_open_chart_url(
        ticker=ticker, class_code=class_code
    )


def t_invest_instrument_url(*, ticker: str, class_code: str) -> str:
    """
    Публичная страница инструмента (не deep-link в терминал, но стандартный веб-каталог).

    Веб-терминал: https://www.tbank.ru/terminal/ — открытие конкретного тикера
    зависит от клиента; ссылка на карточку даёт стабильный URL для мессенджеров.
    """
    raw = (ticker or "").strip()
    cc = (class_code or "").strip().upper()
    t = raw.upper() if cc != "SPBFUT" else raw
    if not t:
        return WEB_TERMINAL
    if cc == "SPBFUT":
        return f"{BASE}/futures/{quote(t, safe='')}/"
    if cc in {"TQBR", "TQTF", "TQTD", "TQTE", "TQCB", "TQOB"}:
        if cc == "TQTF":
            return f"{BASE}/etfs/{quote(t, safe='')}/"
        return f"{BASE}/stocks/{quote(t, safe='')}/"
    return f"{WEB_TERMINAL}?search={quote(t)}"


def t_invest_web_terminal_url() -> str:
    return WEB_TERMINAL
