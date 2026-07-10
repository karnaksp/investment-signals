# Reliable processing contract

The detector uses Kafka manual commits and PostgreSQL as the durable product
source of truth.

For each valid raw record:

1. Look up the event id and exact broker position in `processed_events`.
2. Detect only when the event has not already committed.
3. Insert the inbox row, deterministic signals, and delivery outbox rows in one
   PostgreSQL transaction.
4. Publish committed signals to `marketdata.signals` and wait for broker acks.
5. Checkpoint detector windows to Redis, then commit the exact source offset.

A crash before step 3 rolls the database work back and leaves the offset
uncommitted. A crash after step 3 replays from the inbox without re-running the
detector. Signal-topic publication is at-least-once and carries deterministic
`signal_id`; PostgreSQL signals and delivery outbox rows remain exactly once.

Invalid transport payloads are published to `KAFKA_RAW_DLQ_TOPIC`; their source
offset is committed only after the DLQ broker acknowledgement. Unexpected
infrastructure failures remain uncommitted and terminate the process so the
supervisor can restart it.

`tinvest-delivery-worker` claims outbox rows with `FOR UPDATE SKIP LOCKED`, uses
an expiring lease in `next_attempt_at`, retries with bounded exponential
backoff, and moves exhausted deliveries to `dead_letter`.

Runtime assembly must:

- apply PostgreSQL migration `0103` before starting detector/worker;
- create `marketdata.raw.dlq` (or the configured DLQ topic);
- run `tinvest-delivery-worker` as a separate service;
- start Redis with `conf/redis.conf` so AOF and `noeviction` are active.
