# Contributing

Это production-ориентированный data engineering сервис для realtime market signals. Изменения должны сохранять воспроизводимость локального стека и не добавлять секреты или пользовательские рыночные данные.

## Development Setup

```bash
python -m pip install -e ".[dev,docs]"
python -m pytest
python -m mkdocs build --strict
```

For integration work, use Docker Compose and `.env.example` as the template:

```bash
cp .env.example .env
docker compose up --build
```

## Contribution Checklist

- Keep `.env` and real tokens out of git.
- Add or update tests for detector logic, API behavior, serialization, or config parsing.
- Update docs when changing architecture, ports, topics, schemas, or operator runbooks.
- Prefer reproducible synthetic events for tests and examples.
- Document operational tradeoffs in `docs/roadmap.md` or the relevant runbook page.

## Pull Requests

PRs should include:

- summary of the changed pipeline behavior;
- verification commands;
- any migration or local data reset steps;
- screenshots when touching Signal Cockpit or docs visuals.
