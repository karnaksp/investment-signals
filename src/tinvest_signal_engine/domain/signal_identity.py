"""Stable identity rules for signals derived from source events."""

from __future__ import annotations

from uuid import UUID, uuid5


_SIGNAL_NAMESPACE = UUID("cb5961e4-7685-5cc0-8b7b-144178e433ca")


def deterministic_signal_id(source_event_id: str, signal_type: str) -> str:
    """Return the same UUID for the same source event and signal type."""
    event_id = source_event_id.strip()
    kind = signal_type.strip()
    if not event_id:
        raise ValueError("source_event_id must not be empty")
    if not kind:
        raise ValueError("signal_type must not be empty")
    return str(uuid5(_SIGNAL_NAMESPACE, f"{event_id}\x1f{kind}"))
