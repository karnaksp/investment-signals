# Versioned database migrations

Canonical upgrade migrations live in `postgres/migrations` and
`clickhouse/migrations`. Core owns the contiguous `0100-0199` range. Files are
UTF-8, checksummed by content, and immutable after release.

Run both engines through the composition root:

```bash
tinvest-migrate --release-version 1.0.0
```

The PostgreSQL `schema_migrations` ledger stores the checksum and execution
metadata for both engines. Replaying an identical migration is a no-op; a
changed checksum stops the upgrade.

The `*/init` directories remain only for compatibility with the development
Compose baseline. Product upgrades must use the versioned runner and never
re-execute init-only SQL.
