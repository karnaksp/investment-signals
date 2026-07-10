---
title: Clean Architecture Dependency Rule
status: accepted
---

# Clean Architecture Dependency Rule

## Rule

1. Files under `src/tinvest_signal_engine/domain/**` MUST import only the Python
   standard library and `tinvest_signal_engine.domain`.
2. Files under `src/tinvest_signal_engine/application/**` MUST import only the
   Python standard library, `tinvest_signal_engine.domain`, and
   `tinvest_signal_engine.application`.
3. Inner-layer public interfaces MUST NOT expose FastAPI, Pydantic, psycopg,
   Kafka, Redis, ClickHouse, Dagster, Prometheus, or T-Invest SDK types.
4. Concrete adapters MUST be constructed in a service bootstrap/composition
   root and passed through declared ports.
5. Every changed inner-layer package MUST pass `architecture-boundaries` before
   merge.

## Rationale

The core must remain usable and testable independently of the private product
wrapper and its runtime stack.

## Examples

### Good

```python
class SignalStore(Protocol):
    def save(self, signal: Signal) -> None: ...
```

### Bad

```python
from psycopg import Connection

def save_signal(connection: Connection, signal: Signal) -> None: ...
```

## Enforcement

`@scripts/check_architecture.py`, `@tests/test_architecture_boundaries.py`, and
the CI step `architecture-boundaries` are blocking. Root review verifies DTO
mapping and composition roots.
