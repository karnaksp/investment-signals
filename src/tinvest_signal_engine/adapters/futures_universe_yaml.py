"""YAML adapters for the configured and resolved futures universe."""

from __future__ import annotations

import os
import json
import re
import tempfile
from pathlib import Path
from typing import Any

import yaml

from tinvest_signal_engine.domain.futures_universe import (
    FutureWatchSpec,
    ResolvedFuture,
)


BEGIN = "# >>> FUTURES_NEAREST_BEGIN"
END = "# <<< FUTURES_NEAREST_END"

_SUBSCRIPTIONS = """    subscriptions:
      trades: true
      last_price: true
      info: false
      order_book_depth: 50
      candles: false
      candle_interval: 1m"""


def load_future_watch_specs(path: Path) -> tuple[FutureWatchSpec, ...]:
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict) or raw.get("version") != 1:
        raise ValueError(f"unsupported futures universe config: {path}")
    defaults = raw.get("defaults") or {}
    items = raw.get("futures") or []
    if not isinstance(defaults, dict) or not isinstance(items, list) or not items:
        raise ValueError(f"futures universe must contain a non-empty futures list: {path}")

    def value(item: dict[str, Any], key: str, fallback: Any) -> Any:
        return item[key] if key in item else defaults.get(key, fallback)

    specs: list[FutureWatchSpec] = []
    for item in items:
        if not isinstance(item, dict):
            raise ValueError("future watch entries must be mappings")
        pattern = value(item, "ticker_pattern", None)
        if pattern:
            re.compile(str(pattern))
        specs.append(
            FutureWatchSpec(
                alias=str(item["alias"]).strip(),
                basic_asset=str(item["basic_asset"]).strip(),
                roll_before_expiry_days=int(
                    value(item, "roll_before_expiry_days", 5)
                ),
                minimum_two_sided_lots=int(
                    value(item, "minimum_two_sided_lots", 1)
                ),
                require_two_sided_order_book=bool(
                    value(item, "require_two_sided_order_book", True)
                ),
                max_candidates=int(value(item, "max_candidates", 4)),
                chain_depth=int(value(item, "chain_depth", 1)),
                include_expiring_contract=bool(
                    value(item, "include_expiring_contract", False)
                ),
                ticker_pattern=(str(pattern) if pattern else None),
                require_name_substrings=tuple(
                    str(part)
                    for part in value(item, "require_name_substrings", [])
                ),
                exclude_name_substrings=tuple(
                    str(part)
                    for part in value(item, "exclude_name_substrings", [])
                ),
            )
        )
    return tuple(specs)


def render_futures_block(resolved: tuple[ResolvedFuture, ...]) -> str:
    lines = [
        f"  {BEGIN} (managed by tinvest-refresh-futures)",
        "  # Generated from conf/futures_universe.yaml; do not edit this block manually.",
    ]
    for item in resolved:
        lines.extend(
            (
                f"  - ticker: {item.ticker}",
                f"    class_code: {item.class_code}",
                f"    alias: {item.alias}",
                "    futures_contract:",
                f"      chain_id: {json.dumps(item.chain_id or item.alias)}",
                f"      basic_asset: {json.dumps(item.basic_asset, ensure_ascii=False)}",
                f"      expiration_at: {json.dumps(item.expiration_at.isoformat())}",
                f"      basic_asset_size: {item.basic_asset_size}",
                f"      min_price_increment: {item.min_price_increment}",
                f"      min_price_increment_amount: {item.min_price_increment_amount}",
                f"      settlement_currency: {json.dumps(item.settlement_currency)}",
                f"      settlement_type: {json.dumps(item.settlement_type)}",
                f"      maturity_rank: {item.maturity_rank}",
                _SUBSCRIPTIONS,
            )
        )
    lines.append(f"  {END}")
    return "\n".join(lines)


class AtomicYamlFuturesUniversePublisher:
    def __init__(self, instruments_path: Path) -> None:
        self._path = instruments_path

    def publish(self, resolved: tuple[ResolvedFuture, ...]) -> bool:
        original = self._path.read_text(encoding="utf-8")
        pattern = re.compile(
            r"^[ \t]*" + re.escape(BEGIN) + r".*?^[ \t]*" + re.escape(END),
            re.MULTILINE | re.DOTALL,
        )
        if pattern.search(original):
            updated = pattern.sub(render_futures_block(resolved), original, count=1)
        elif BEGIN not in original and END not in original:
            separator = "" if original.endswith("\n\n") else "\n"
            updated = original + separator + render_futures_block(resolved) + "\n"
        else:
            raise ValueError(f"managed futures markers are incomplete in {self._path}")
        if updated == original:
            return False

        mode = self._path.stat().st_mode & 0o777
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{self._path.name}.",
            dir=self._path.parent,
            text=True,
        )
        temporary = Path(temporary_name)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                handle.write(updated)
                handle.flush()
                os.fsync(handle.fileno())
            os.chmod(temporary, mode)
            os.replace(temporary, self._path)
            directory_fd = os.open(self._path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        finally:
            temporary.unlink(missing_ok=True)
        return True
