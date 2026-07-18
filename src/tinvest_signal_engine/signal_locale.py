"""Русские подписи для доставки сигналов (мессенджеры, админка)."""

from __future__ import annotations

import html

from .models import TriggerSignal
from .signal_interpretation import build_signal_interpretation
from .terminal_links import t_invest_web_terminal_url

_SIGNAL_TYPE_RU: dict[str, str] = {
    # Имена из detector_core.py и исторические алиасы
    "volume_spike": "Всплеск объёма",
    "trade_rate_spike": "Всплеск частоты сделок",
    "trade_count_spike": "Всплеск числа сделок",
    "price_jump": "Сильное движение цены",
    "price_move_spike": "Резкое движение цены",
    "spread_widening": "Расширение спреда",
    "spread_spike": "Расширение спреда",
    "orderbook_imbalance": "Дисбаланс стакана",
    "imbalance_spike": "Дисбаланс стакана",
    "obi_dynamics": "Скачок индикатора OBI",
    "obi_delta_spike": "Скачок дельты OBI",
    "trading_status_changed": "Смена торгового статуса",
    "microstructure_combo_long": "Комбо микроструктуры (лонг)",
    "microstructure_combo_short": "Комбо микроструктуры (шорт)",
    "orderbook_spoofing_bid_pull": "Спуфинг стакана (снятие бида)",
    "orderbook_spoofing_ask_pull": "Спуфинг стакана (снятие аска)",
    "aggressive_trade_burst": "Пачка агрессивных сделок",
    "lead_lag_divergence": "Расхождение lead–lag",
    "market_access_changed": "Изменение доступности заявок",
    "orderbook_snapshot_inconsistent": "Несогласованный снимок стакана",
    "price_near_limit_band": "Близко к лимиту цены дня",
    "open_interest_spike": "Всплеск открытого интереса",
    "candle_range_spike": "Широкий диапазон свечи",
    "bond_maturity_convergence": "Схождение облигации к номиналу",
}


def signal_type_ru(signal_type: str) -> str:
    if signal_type in _SIGNAL_TYPE_RU:
        return _SIGNAL_TYPE_RU[signal_type]
    human = signal_type.replace("_", " ").strip()
    return human or signal_type


def _severity_ru(sev: int) -> str:
    if sev >= 3:
        return "высокая"
    if sev == 2:
        return "средняя"
    return "низкая"


def build_plain_explanation_ru(signal: TriggerSignal) -> str:
    """Короткое объяснение «что случилось» без английского жаргона в заголовке."""
    interpretation = build_signal_interpretation(signal)
    headline = interpretation.get("headline_ru")
    if isinstance(headline, str) and headline.strip():
        return headline.strip()

    st = signal.signal_type
    p = signal.payload or {}
    z = abs(float(signal.z_score))
    win = int(signal.window_seconds)

    if st == "trading_status_changed":
        prev = str(p.get("previous_status", "—"))
        cur = str(p.get("current_status", "—"))
        return (
            f"У инструмента сменился торговый статус биржи: было «{prev}», стало «{cur}». "
            "Имеет смысл проверить доступность заявок и ликвидность."
        )
    if st == "volume_spike":
        return (
            f"Суммарный объём сделок за окно ~{win} с сильно выше недавней нормы "
            f"(статистически |z|≈{z:.1f}): возможен всплеск активности или крупные пачки."
        )
    if st == "trade_rate_spike":
        return (
            f"Число сделок за ~{win} с заметно выше обычного (|z|≈{z:.1f}): "
            "часто это всплеск «тика» или алгоритмическая активность."
        )
    if st == "price_jump":
        return (
            f"Диапазон движения цены за ~{win} с выбивается из недавней волатильности "
            f"(|z|≈{z:.1f}): резкий сдвиг котировок относительно базы."
        )
    if st == "spread_widening":
        return (
            f"Спред bid/ask в базисных пунктах за окно ~{win} с вырос относительно нормы "
            f"(|z|≈{z:.1f}): ликвидность на лучших ценах могла ухудшиться."
        )
    if st == "orderbook_imbalance":
        return (
            f"Доля объёма на одной стороне стакана (относительный дисбаланс) за ~{win} с "
            f"аномальна (|z|≈{z:.1f}): перекос спроса/предложения в глубине книги."
        )
    if st == "obi_dynamics":
        return (
            f"Индикатор OBI (дисбаланс верхних уровней) резко изменился за ~{win} с "
            f"(|z|≈{z:.1f}): быстрый сдвиг давления в стакане."
        )
    if st == "orderbook_spoofing_bid_pull":
        side = "бида"
    elif st == "orderbook_spoofing_ask_pull":
        side = "аска"
        return (
            f"Крупная «стена» на стороне {side} заметно истончилась за короткое время "
            "при относительно малом движении mid — типичный признак спуфинга/снятия ликвидности "
            "(см. детали в payload)."
        )
    if st == "aggressive_trade_burst":
        d = str(p.get("direction", "")).lower()
        dr = "покупок" if d == "buy" else "продаж" if d == "sell" else d or "одной стороны"
        n = int(p.get("print_count") or signal.metric_value)
        return (
            f"За короткое окно зафиксирована плотная серия сделок в сторону {dr} "
            f"({n} тиков): агрессивный поток одного направления."
        )
    if st in {"microstructure_combo_long", "microstructure_combo_short"}:
        bias = "лонговая" if "long" in st else "шортовая"
        cd = p.get("combo_detail") if isinstance(p.get("combo_detail"), dict) else {}
        pa = cd.get("points_awarded") if isinstance(cd.get("points_awarded"), dict) else {}
        bits = []
        if pa.get("spread"):
            bits.append("спред")
        if pa.get("tick_rate"):
            bits.append("частота сделок")
        if pa.get("imbalance_long") or pa.get("imbalance_short"):
            bits.append("перекос стакана")
        if pa.get("delta_long") or pa.get("delta_short"):
            bits.append("агрессивная дельта")
        tail = ", ".join(bits) if bits else "см. combo_detail в payload"
        return (
            f"Совпало несколько микроструктурных признаков ({bias} комбинация): {tail} "
            f"в окне свежести {cd.get('freshness_seconds', '?')} с."
        )
    if st == "lead_lag_divergence":
        leader = str(p.get("leader_instrument_id", ""))[:12]
        suf = f" (лидер: {leader})" if leader else ""
        return (
            "Лидер в паре сильно пошёл в цене, а этот инструмент почти не отреагировал "
            "в том же окне — возможное запаздывание или расхождение корреляции."
            + suf
        )
    # Универсальный fallback
    return (
        f"Сработал детектор «{signal_type_ru(st)}»: метрика заметно отклонилась от базы "
        f"в окне ~{win} с (|z|≈{z:.1f}). Сверьте с лентой сделок и стаканом."
    )


def build_summary_ru(signal: TriggerSignal, quality: dict) -> str:
    """Многострочное описание на русском для БД и логов."""
    expl = build_plain_explanation_ru(signal)
    lines = [
        f"{signal_type_ru(signal.signal_type)} — {signal.ticker} ({signal.class_code}).",
        expl,
        f"Серьёзность: {_severity_ru(int(signal.severity))} (уровень {signal.severity}). "
        f"|z|={abs(signal.z_score):.2f}, метрика={signal.metric_value:.4g}, "
        f"база={signal.baseline_value:.4g}, окно {signal.window_seconds} с.",
        f"Оценка полезности: {quality['quality_score']}/100 "
        f"({quality['quality_tier_ru']}). {quality['quality_hint_ru']}",
    ]
    return "\n".join(lines)


def _telegram_br_lines(text: str) -> str:
    """Telegram HTML: перенос строки — символ ``\\n``; теги ``<br>`` / ``<br/>`` в HTML mode не поддерживаются."""
    return text


def build_telegram_html(
    signal: TriggerSignal,
    quality: dict,
    *,
    ticker_terminal_url: str,
    instrument_page_url: str,
) -> str:
    """HTML для Telegram: тикер ведёт в веб-терминал, отдельно — карточка инструмента."""
    t_esc = html.escape(signal.ticker)
    type_ru = html.escape(signal_type_ru(signal.signal_type))
    type_raw = html.escape(signal.signal_type)
    summ_plain = build_summary_ru(signal, quality)
    summ = _telegram_br_lines(html.escape(summ_plain))
    term_href = html.escape(ticker_terminal_url, quote=True)
    inv_href = html.escape(instrument_page_url, quote=True)
    wterm = html.escape(t_invest_web_terminal_url())
    score = quality["quality_score"]
    tier = html.escape(str(quality["quality_tier_ru"]))
    # Вложенность <b><a>…</a></b> у Bot API часто даёт 400; допустимо <a><b>…</b></a>.
    return (
        f"<a href=\"{term_href}\"><b>{t_esc}</b></a> ({html.escape(signal.class_code)})\n"
        f"Тип: {type_ru} <code>{type_raw}</code>\n"
        f"Оценка: <b>{score}</b>/100 ({tier})\n"
        f"Терминал: <a href=\"{wterm}\">tbank.ru/terminal</a> · "
        f"<a href=\"{inv_href}\">карточка инструмента</a>\n\n"
        f"{summ}"
    )


def format_plain_alert_ru(
    signal: TriggerSignal,
    *,
    ticker_terminal_url: str,
    instrument_page_url: str,
) -> str:
    """Простой текст без HTML (fallback для Telegram при ошибках разметки)."""
    q = signal.payload or {}
    score = q.get("quality_score", "")
    tier = q.get("quality_tier_ru", "")
    head = (
        f"{signal.ticker} ({signal.class_code}) — {signal_type_ru(signal.signal_type)}\n"
        f"Терминал: {ticker_terminal_url}\n"
        f"Карточка: {instrument_page_url}\n"
    )
    if score != "":
        head += f"Оценка: {score}/100 ({tier})\n"
    return head + "\n" + signal.summary
