#!/usr/bin/env python3
"""
Регистрация protobuf-схем ``NormalizedEventV1`` и ``TriggerSignalV1`` в Schema Registry.

Пример::

  export SCHEMA_REGISTRY_URL=http://localhost:18081
  python scripts/register_schemas_sr.py
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description="Register tinvest protos in Schema Registry")
    parser.add_argument(
        "--registry-url",
        default=os.getenv("SCHEMA_REGISTRY_URL", ""),
        help="Base URL (default: SCHEMA_REGISTRY_URL)",
    )
    parser.add_argument(
        "--proto-dir",
        type=Path,
        default=None,
        help="Directory with .proto files (default: ./proto or PROTO_DIR)",
    )
    args = parser.parse_args()
    base = (args.registry_url or "").strip()
    if not base:
        print("Set SCHEMA_REGISTRY_URL or pass --registry-url", file=sys.stderr)
        return 2
    proto_dir = args.proto_dir
    if proto_dir is None:
        raw = (os.getenv("PROTO_DIR") or "").strip()
        proto_dir = Path(raw).resolve() if raw else (Path(__file__).resolve().parents[1] / "proto")
    ne = proto_dir / "normalized_event.proto"
    ts = proto_dir / "trigger_signal.proto"
    for p in (ne, ts):
        if not p.is_file():
            print(f"Missing proto file: {p}", file=sys.stderr)
            return 2

    from tinvest_signal_engine.schema_registry import (
        register_protobuf_schema,
        schema_subject_for_topic,
    )

    raw_topic = os.getenv("KAFKA_RAW_TOPIC", "marketdata.raw")
    sig_topic = os.getenv("KAFKA_SIGNAL_TOPIC", "marketdata.signals")
    raw_subject = schema_subject_for_topic(raw_topic)
    sig_subject = schema_subject_for_topic(sig_topic)
    rid = register_protobuf_schema(base, raw_subject, ne)
    sid = register_protobuf_schema(base, sig_subject, ts)
    print(json_out(rid, sid, raw_topic, sig_topic, raw_subject, sig_subject))
    return 0


def json_out(
    rid: int, sid: int, raw_topic: str, sig_topic: str, rs: str, ss: str
) -> str:
    import json

    return json.dumps(
        {
            raw_topic: {"subject": rs, "schema_id": rid},
            sig_topic: {"subject": ss, "schema_id": sid},
            "env_hint": {
                "KAFKA_PROTOBUF_SCHEMA_ID_RAW": rid,
                "KAFKA_PROTOBUF_SCHEMA_ID_SIGNAL": sid,
            },
        },
        indent=2,
    )


if __name__ == "__main__":
    raise SystemExit(main())
