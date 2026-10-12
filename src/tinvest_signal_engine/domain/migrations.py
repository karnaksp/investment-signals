"""Versioned migration entities and invariants."""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256


SUPPORTED_ENGINES = frozenset({"postgresql", "clickhouse"})


class MigrationError(RuntimeError):
    """Base error for a migration invariant failure."""


class MigrationChecksumConflict(MigrationError):
    """An applied migration no longer matches its immutable source."""


@dataclass(frozen=True)
class Migration:
    engine: str
    version: int
    name: str
    sql: str

    def __post_init__(self) -> None:
        if self.engine not in SUPPORTED_ENGINES:
            raise ValueError(f"Unsupported migration engine: {self.engine!r}")
        if not 100 <= self.version <= 399:
            raise ValueError("Migration version must be between 0100 and 0399")
        if not self.name or not self.name.replace("_", "").isalnum():
            raise ValueError(f"Invalid migration name: {self.name!r}")
        if not self.sql.strip():
            raise ValueError("Migration SQL must not be empty")

    @property
    def checksum_sha256(self) -> bytes:
        return sha256(self.sql.encode("utf-8")).digest()


@dataclass(frozen=True)
class AppliedMigration:
    engine: str
    version: int
    name: str
    checksum_sha256: bytes
