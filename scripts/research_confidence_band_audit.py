#!/usr/bin/env python3
"""Audit confidence bands for the up/down/skip research objective."""

from __future__ import annotations

import argparse
import csv
import json
import statistics
from collections import Counter
from pathlib import Path
from typing import Any, Mapping, Sequence


BANDS: tuple[tuple[str, str, float, float], ...] = (
    ("skip", "пропустить, недостаточно уверенности", 0.0, 0.60),
    ("weak_observation", "слабое наблюдение", 0.60, 0.75),
    ("working_hypothesis", "рабочая гипотеза", 0.75, 0.90),
    ("strong_signal", "сильный сигнал", 0.90, 1.01),
)

RU_DECISIONS = {
    "up": "ожидается рост",
    "down": "ожидается снижение",
    "skip": "пропустить, недостаточно уверенности",
}


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _float_or_none(value: object) -> float | None:
    if value in {None, ""}:
        return None
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _int_or_zero(value: object) -> int:
    numeric = _float_or_none(value)
    return int(numeric) if numeric is not None else 0


def wilson_lower_bound(successes: int, total: int, z: float = 1.959963984540054) -> float:
    if total <= 0:
        return 0.0
    phat = successes / total
    denominator = 1 + z * z / total
    centre = phat + z * z / (2 * total)
    margin = z * ((phat * (1 - phat) + z * z / (4 * total)) / total) ** 0.5
    return (centre - margin) / denominator


def confidence_band(confidence: float) -> tuple[str, str, float, float]:
    for band in BANDS:
        if band[2] <= confidence < band[3]:
            return band
    return BANDS[-1] if confidence >= 0.90 else BANDS[0]


def _band_rows(rows: Sequence[Mapping[str, Any]], band_name: str) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for row in rows:
        confidence = _float_or_none(row.get("max_confidence")) or _float_or_none(row.get("frontier_confidence")) or 0.0
        if confidence_band(confidence)[0] == band_name:
            result.append(dict(row))
    return result


def _accepted(metric: Mapping[str, Any]) -> bool:
    return bool(
        int(metric["selected_rows"]) >= int(metric["min_required_rows"])
        and int(metric["sessions"]) >= int(metric["min_required_sessions"])
        and float(metric["success_rate"]) >= float(metric["target_success_rate"])
        and float(metric["wilson_lower_95"]) >= float(metric["min_required_wilson_lower_95"])
        and float(metric["mean_result_bps"]) > 0
    )


def _blocking_reasons(metric: Mapping[str, Any]) -> str:
    reasons: list[str] = []
    if int(metric["selected_rows"]) < int(metric["min_required_rows"]):
        reasons.append("мало случаев")
    if int(metric["sessions"]) < int(metric["min_required_sessions"]):
        reasons.append("мало торговых дней")
    if float(metric["success_rate"]) < float(metric["target_success_rate"]):
        reasons.append("доля успеха ниже цели")
    if float(metric["wilson_lower_95"]) < float(metric["min_required_wilson_lower_95"]):
        reasons.append("нижняя граница надёжности ниже порога")
    if float(metric["mean_result_bps"]) <= 0:
        reasons.append("результат после издержек не положительный")
    return "; ".join(reasons)


def _metric_row(
    *,
    scope: str,
    rule: str,
    band_name: str,
    band_label_ru: str,
    rows: Sequence[Mapping[str, Any]],
    min_rows: int,
    min_sessions: int,
    min_wilson_lower: float,
    target_success_rate: float,
) -> dict[str, Any]:
    successes = sum(_int_or_zero(row.get("frontier_success")) for row in rows)
    results = [_float_or_none(row.get("frontier_result_bps")) or 0.0 for row in rows]
    confidences = [
        _float_or_none(row.get("max_confidence")) or _float_or_none(row.get("frontier_confidence")) or 0.0
        for row in rows
    ]
    decisions = Counter(str(row.get("frontier_decision", "")) for row in rows)
    relations = Counter(str(row.get("frontier_decision_relation", "")) for row in rows)
    selected = len(rows)
    dominant_decision = decisions.most_common(1)[0][0] if decisions else "skip"
    metric = {
        "scope": scope,
        "rule": rule,
        "band": band_name,
        "band_label_ru": band_label_ru,
        "candidate_decision": dominant_decision,
        "candidate_decision_ru": RU_DECISIONS.get(dominant_decision, RU_DECISIONS["skip"]),
        "selected_rows": selected,
        "sessions": len({str(row.get("trading_day", "")) for row in rows if row.get("trading_day")}),
        "success_count": successes,
        "success_rate": successes / selected if selected else 0.0,
        "wilson_lower_95": wilson_lower_bound(successes, selected),
        "mean_confidence": statistics.fmean(confidences) if confidences else 0.0,
        "mean_result_bps": statistics.fmean(results) if results else 0.0,
        "up_rows": decisions.get("up", 0),
        "down_rows": decisions.get("down", 0),
        "direct_rows": relations.get("direct", 0),
        "inverse_rows": relations.get("inverse", 0),
        "neutral_rows": relations.get("neutral", 0),
        "min_required_rows": min_rows,
        "min_required_sessions": min_sessions,
        "min_required_wilson_lower_95": min_wilson_lower,
        "target_success_rate": target_success_rate,
        "product_claim_allowed": False,
    }
    accepted = _accepted(metric)
    metric["accepted_shadow"] = accepted
    metric["safe_runtime_decision_ru"] = metric["candidate_decision_ru"] if accepted else RU_DECISIONS["skip"]
    metric["blocking_reasons_ru"] = "" if accepted else _blocking_reasons(metric)
    return metric


def build_confidence_band_audit(
    rows: Sequence[Mapping[str, Any]],
    *,
    min_rows: int = 300,
    min_sessions: int = 30,
    min_wilson_lower: float = 0.75,
    target_success_rate: float = 0.90,
) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for band_name, band_label, _, _ in BANDS:
        band_selected = _band_rows(rows, band_name)
        result.append(
            _metric_row(
                scope="confidence_band",
                rule=band_name,
                band_name=band_name,
                band_label_ru=band_label,
                rows=band_selected,
                min_rows=min_rows,
                min_sessions=min_sessions,
                min_wilson_lower=min_wilson_lower,
                target_success_rate=target_success_rate,
            )
        )
        for decision in ("up", "down"):
            decision_rows = [row for row in band_selected if str(row.get("frontier_decision", "")) == decision]
            result.append(
                _metric_row(
                    scope="confidence_band_direction",
                    rule=f"{band_name} | {decision}",
                    band_name=band_name,
                    band_label_ru=band_label,
                    rows=decision_rows,
                    min_rows=min_rows,
                    min_sessions=min_sessions,
                    min_wilson_lower=min_wilson_lower,
                    target_success_rate=target_success_rate,
                )
            )
    return result


def write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    if not fields:
        fields = ["scope", "rule"]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fields})


def write_report(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    top = [row for row in rows if row.get("scope") == "confidence_band"]
    accepted = [row for row in rows if row.get("accepted_shadow")]
    lines = [
        "# Аудит уровней уверенности",
        "",
        "Этот отчёт проверяет, можно ли превращать уверенность модели в действие. "
        "Даже уровень «сильный сигнал» остаётся пропуском, если не прошёл историческую проверку.",
        "",
        "| Уровень | Случаев | Торговых дней | Успешных | Доля успеха | Нижняя граница | Действие | Причина блокировки |",
        "|---|---:|---:|---:|---:|---:|---|---|",
    ]
    for row in top:
        lines.append(
            "| {band} | {rows} | {sessions} | {successes} | {rate:.1%} | {lower:.1%} | {action} | {reasons} |".format(
                band=row.get("band_label_ru", ""),
                rows=row.get("selected_rows", 0),
                sessions=row.get("sessions", 0),
                successes=row.get("success_count", 0),
                rate=float(row.get("success_rate") or 0.0),
                lower=float(row.get("wilson_lower_95") or 0.0),
                action=row.get("safe_runtime_decision_ru", ""),
                reasons=row.get("blocking_reasons_ru", ""),
            )
        )
    lines.extend(
        [
            "",
            "## Вывод",
            "",
        ]
    )
    if accepted:
        lines.append("Найдены уровни, которые можно переносить в режим скрытой проверки:")
        for row in accepted[:10]:
            lines.append(f"- `{row.get('rule')}` → {row.get('safe_runtime_decision_ru')}")
    else:
        lines.append(
            "Пока ни один уровень уверенности не даёт права показывать «ожидается рост» "
            "или «ожидается снижение». Безопасное действие — «пропустить, недостаточно уверенности»."
        )
    lines.append("")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def run_confidence_band_audit(*, audit_path: Path, output_dir: Path) -> dict[str, Any]:
    rows = build_confidence_band_audit(_read_csv(audit_path))
    output_dir.mkdir(parents=True, exist_ok=True)
    write_csv(output_dir / "confidence-band-audit.csv", rows)
    write_report(output_dir / "confidence-band-audit.md", rows)
    summary = {
        "schema_version": 1,
        "kind": "confidence_band_audit",
        "rows": len(rows),
        "accepted_shadow": sum(1 for row in rows if row.get("accepted_shadow")),
        "product_claim_allowed": False,
        "output_dir": str(output_dir),
    }
    (output_dir / "confidence-band-audit-summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return summary


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="research-confidence-band-audit")
    parser.add_argument("--audit", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    result = run_confidence_band_audit(audit_path=args.audit, output_dir=args.output_dir)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
