"""Deterministic, phone-readable position bars and a short accessible caption."""

from dataclasses import asdict
from datetime import date
import html
from io import BytesIO
from pathlib import Path

from tinvest_signal_engine.domain.asset_positions import ASSET_REPORT_VERSION, AssetPositionReport


ASSET_NAMES = {
    "gold": "Золото", "silver": "Серебро", "platinum": "Платина", "palladium": "Палладий",
    "copper": "Медь", "wti": "Нефть WTI", "brent": "Нефть Brent",
    "natural_gas": "Газ Henry Hub", "wheat_srw": "Пшеница SRW",
}
_GROUPS = {"broker_clients": "Через брокеров", "direct_members": "Напрямую на бирже",
           "managed_money": "Фонды", "producers_merchants": "Производители и торговцы"}
_VENUES = {"SHFE": "Китай · SHFE", "CFTC": "США · CFTC", "ICE": "Лондон · ICE"}
_INK, _MUTED, _LONG, _SHORT = "#182D3D", "#556371", "#296CC2", "#CF6946"


def asset_report_payload(report: AssetPositionReport) -> dict:
    sections = []
    for section in report.sections:
        row = asdict(section)
        row.update(trading_day=section.trading_day.isoformat(), previous_day=section.previous_day.isoformat(),
                   source_event_at=section.source_event_at.isoformat())
        sections.append(row)
    primary = sections[0]
    return {"daily_report_version": ASSET_REPORT_VERSION, "commodity": report.commodity,
            "source_exchange": primary["source"], "period": primary["period"],
            "trading_day": primary["trading_day"], "previous_day": primary["previous_day"],
            "sections": sections, "unavailable_sources": list(report.unavailable_sources)}


def _quantity(value: int) -> str:
    if abs(value) >= 1_000_000:
        return f"{value / 1_000_000:.2f}".replace(".", ",") + " млн"
    if abs(value) >= 1000:
        return f"{value / 1000:.1f}".replace(".", ",") + " тыс."
    return str(value)


def group_takeaway(group: dict, period: str, *, include_period: bool = True) -> str:
    net = group["longs"] - group["shorts"]
    before = group["previous_longs"] - group["previous_shorts"]
    if net == before:
        return "Перевес не изменился" if net else "Лонги и шорты в равновесии"
    if net == 0:
        return "Перевес исчез: лонги и шорты сравнялись"
    side = "лонгов" if net > 0 else "шортов"
    if net * before < 0:
        return f"Сторона сменилась: теперь больше {side}"
    verb = "вырос" if abs(net) > abs(before) else "сократился"
    interval = (" за день" if period == "daily" else " за неделю") if include_period else ""
    return f"Перевес {side} {verb}{interval} на {_quantity(abs(net - before))}"


def _display_groups(section: dict) -> list[dict]:
    order = ("broker_clients", "direct_members") if section["source"] == "SHFE" else ("managed_money", "producers_merchants")
    by_category = {g["category"]: g for g in section["groups"]}
    return [by_category[key] for key in order]


def _dates(section: dict) -> str:
    day = date.fromisoformat(section["trading_day"]).strftime("%d.%m.%Y")
    before = date.fromisoformat(section["previous_day"]).strftime("%d.%m.%Y")
    return f"{day} к {before}"


def render_asset_positions_html(payload: dict) -> str:
    lines = [f"<b>{ASSET_NAMES[payload['commodity']]} · позиции игроков</b>"]
    for section in payload["sections"]:
        cadence = "день" if section["period"] == "daily" else "неделя, с задержкой"
        lines.extend(("", f"<b>{_VENUES[section['source']]} · {cadence}</b>", _dates(section)))
        group = _display_groups(section)[0]
        sentence = f"{_GROUPS[group['category']]}: {group_takeaway(group, section['period']).lower()}"
        lines.append(sentence.rstrip(".") + ".")
    if payload["unavailable_sources"]:
        lines.append("Дополнительные данные недоступны: " + html.escape(", ".join(payload["unavailable_sources"])))
    lines.extend(("", "Полосы — доли лонгов и шортов внутри группы, без спредов. "
                  "Изменения — в контрактах. Биржи не суммируются; позиции производителей могут быть хеджем."))
    links = [f'<a href="{html.escape(s["source_url"], quote=True)}">{s["source"]}</a>' for s in payload["sections"]]
    lines.append("Источники: " + " · ".join(links))
    caption = "\n".join(lines)
    if len(caption) > 1024:
        raise ValueError("position card caption exceeds Telegram limit")
    return caption


def _font(size: int, bold: bool = False):
    from PIL import ImageFont

    filename = "DejaVuSans-Bold.ttf" if bold else "DejaVuSans.ttf"
    paths = (Path("/usr/share/fonts/truetype/dejavu") / filename,
             Path("/System/Library/Fonts/Supplemental") / ("Arial Bold.ttf" if bold else "Arial.ttf"))
    path = next((p for p in paths if p.exists()), None)
    if path is None:
        raise RuntimeError("Cyrillic card font is missing: install fonts-dejavu-core")
    return ImageFont.truetype(str(path), size)


def render_asset_positions_png(payload: dict) -> bytes:
    """100% bars use only L/(L+S), not OI or cross-exchange contract totals."""
    from PIL import Image, ImageDraw

    width, margin = 1080, 64
    sections = payload["sections"]
    height = 300 + len(sections) * 480 + (80 if payload["unavailable_sources"] else 0)
    image = Image.new("RGB", (width, height), "#FAFAF7")
    draw = ImageDraw.Draw(image)
    title, heading, body, small = _font(68, True), _font(42, True), _font(37), _font(32)
    draw.text((margin, 45), ASSET_NAMES[payload["commodity"]], font=title, fill=_INK)
    draw.text((margin, 130), "Кто держит лонги и шорты", font=body, fill=_MUTED)
    y = 220
    for section in sections:
        cadence = "за день" if section["period"] == "daily" else "за неделю · с задержкой"
        draw.text((margin, y), _VENUES[section["source"]] + " · " + cadence, font=heading, fill=_INK)
        draw.text((margin, y + 57), _dates(section), font=small, fill=_MUTED)
        y += 118
        for group in _display_groups(section):
            draw.text((margin, y), _GROUPS[group["category"]], font=body, fill=_INK)
            total = group["longs"] + group["shorts"]
            bar_y = y + 84
            draw.rounded_rectangle((margin, bar_y, width - margin, bar_y + 20), radius=6, fill="#E0E4E6")
            if total:
                boundary = margin + round((width - 2 * margin) * group["longs"] / total)
                if boundary > margin:
                    draw.rectangle((margin, bar_y, boundary, bar_y + 20), fill=_LONG)
                if boundary < width - margin:
                    draw.rectangle((boundary, bar_y, width - margin, bar_y + 20), fill=_SHORT)
                long_pct = round(group["longs"] / total * 100)
                draw.text((margin, y + 47), f"Лонги {long_pct}%", font=small, fill=_LONG)
                label = f"Шорты {100 - long_pct}%"
                draw.text((width - margin, y + 47), label, anchor="ra", font=small, fill=_SHORT)
            else:
                draw.text((margin, y + 47), "Нет направленных позиций", font=small, fill=_MUTED)
            # Exact direction, including a flip, comes from unrounded counts.
            takeaway = group_takeaway(group, section["period"], include_period=False)
            draw.text((margin, y + 120), takeaway, font=body, fill=_MUTED)
            y += 170
        y += 22
    if payload["unavailable_sources"]:
        draw.text((margin, y), "Часть источников недоступна", font=small, fill=_MUTED)
        y += 80
    draw.text((margin, height - 95), "Доли внутри группы · без спредов", font=_font(29), fill=_MUTED)
    draw.text((margin, height - 55), "Изменения в контрактах · биржи не суммируются", font=_font(29), fill=_MUTED)
    output = BytesIO()
    image.save(output, "PNG", optimize=True)
    return output.getvalue()
