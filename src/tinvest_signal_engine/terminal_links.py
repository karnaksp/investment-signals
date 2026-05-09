"""Ссылки на карточки инструментов на сайте Т-Банк Инвестиции (веб)."""

from __future__ import annotations

from urllib.parse import quote

BASE = "https://www.tbank.ru/invest"
WEB_TERMINAL = "https://www.tbank.ru/terminal/"


def t_invest_terminal_search_url(*, ticker: str) -> str:
    """
    Веб-терминал Т-Банка с подстановкой тикера в поиск (удобно из мессенджеров).

    Карточка инструмента в каталоге «Инвестиции» — см. ``t_invest_instrument_url``.
    """
    t = (ticker or "").strip().upper()
    if not t:
        return WEB_TERMINAL
    return f"{WEB_TERMINAL}?search={quote(t)}"


def t_invest_instrument_url(*, ticker: str, class_code: str) -> str:
    """
    Публичная страница инструмента (не deep-link в терминал, но стандартный веб-каталог).

    Веб-терминал: https://www.tbank.ru/terminal/ — открытие конкретного тикера
    зависит от клиента; ссылка на карточку даёт стабильный URL для мессенджеров.
    """
    t = (ticker or "").strip().upper()
    cc = (class_code or "").strip().upper()
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
