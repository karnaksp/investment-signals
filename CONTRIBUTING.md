# Contributing

This project is a portfolio-grade data engineering service. Contributions should keep the local stack reproducible and avoid committing secrets or user-specific market data.

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

