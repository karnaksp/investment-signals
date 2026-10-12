# Repository execution rules

## Clean Architecture gate

All new and modified production code MUST follow Robert C. Martin's Clean
Architecture dependency rule.

- `src/tinvest_signal_engine/domain/**` MUST import only Python's standard
  library and other domain modules.
- `src/tinvest_signal_engine/application/**` MUST depend only on domain modules
  and application-owned ports.
- Inner layers MUST NOT import `services`, adapters, framework packages,
  database drivers, broker clients, or vendor SDKs.
- Concrete construction MUST live in a service bootstrap/composition root.
- Transport and persistence records MUST be mapped to explicit domain or
  application DTOs at an adapter boundary.
- Existing modules outside the target layer directories are legacy; new inner
  layers MUST NOT depend on them. Product work MUST reduce or preserve legacy
  coupling, never increase it.

Run `python scripts/check_architecture.py` and the full test suite before every
commit. Any package-boundary change requires a corresponding verifier update.

## Productization ownership

The root integrator alone edits master API contracts, release assembly,
lockfiles, migration ledger, and master task state. Core migrations use numbers
`0100-0199`. Agent work must be isolated in assigned branches/worktrees and
must return a commit SHA, verification evidence, and contract deviations.

