---
title: "Запуск и остановка Signal Research Lab"
status: draft
tags:
  - "guide"
  - "operations"
  - "productization"
  - "research"
---

Reader: product owner; task: operate Signal Research Lab; step actor: local installation operator.

## Prerequisites

- Investment Signals Pro is installed and answers on port 18443.
- Signal Research Lab installation files and secrets exist.
- Port 18444 is free.
- No migration or restore operation is active.

## Steps

1. Check the working product status.

`signalctl status`

2. Start the laboratory admin and isolated stores without compute workers.

`docker compose -f deploy/research-lab.compose.yaml up -d research-api`

3. Open `http://localhost:18444`.

4. Confirm PostgreSQL, ClickHouse, API, and workers report current checkpoints.

5. Perform hypothesis or publication work.

6. Confirm no run remains queued or active.

7. Stop the laboratory Compose project.

`docker compose -f deploy/research-lab.compose.yaml down`

8. Recheck the working product status.

`signalctl status`

## Verification

- Port 18444 closes after the stop command.
- Port 18443 continues returning HTTP 200.
- Laboratory containers report `exited`.
- Working signal, delivery, and outcome services remain healthy.

## Common Issues

- Port 18444 occupied: identify the listener and stop the conflicting process.
- Laboratory database unhealthy: inspect laboratory logs without restarting the working product.
- Active run blocks shutdown: cancel or complete the run, then repeat the stop.
- Working status changes: abort and inspect cross-product mounts or networks.
