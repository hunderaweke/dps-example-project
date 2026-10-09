---
name: add-kafka-consumer
description: Add a Kafka/Redpanda topic, event publisher and consumer handler to this Go service using franz-go, with trace propagation, idempotency and worker registration. Use when asked to publish or consume events, add a topic, or react asynchronously to something.
---

# Add a Kafka topic, publisher and consumer

Reference files:
- `internal/const/messaging/kafka/kafka.go` (clients, kotel tracer)
- `internal/storage/publisher/example.go` (outbound adapter)
- `internal/handler/event/consumer.go` (generic poll loop)
- `internal/handler/event/example.go` (per-topic handler)
- `initiator/initiator.go` (`InitiateWorker` handlers map)

## Steps

1. **Topic config**: add a field to `config.Topics` (`config/config.go`) and to `config/config.yaml` under `kafka.topics`. Topics are auto-created in dev. In production, create them with explicit partitions and retention.
2. **Event**: in `internal/const/models/`, a struct with JSON tags and `validate` tags. Include the entity ID.
3. **Publish** (outbound):
   - Add a method to an `EventPublisher`-style port in `internal/module`, then run `make mocks`.
   - Implement it in `internal/storage/publisher/<domain>.go`: `json.Marshal`, then `kgo.Record{Topic, Key: []byte(id), Value}`, then `ProduceSync(ctx, rec).FirstErr()`, wrapping errors in `apperrors.ErrPublish`.
   - **Key by entity ID** so a single entity's events are ordered on one partition.
   - Wire it in `initiator/module.go` (it needs `p.Producer`; add `Producer: true` to the process `needs` if the worker publishes too).
4. **Consume** (inbound):
   - `internal/handler/event/<domain>.go`: `func XHappened(m module.X) HandlerFunc` decodes JSON. On decode failure it returns `apperrors.ErrInvalidInput.Wrap(...)`, which marks the record as poison and skips it. Then it calls a module method.
   - Add the module method, such as `HandleXHappened`. It validates the event with `validator`, then acts.
   - Register it in `InitiateWorker`: `handlers[cfg.Kafka.Topics.XHappened] = event.XHappened(mods.X)`.
5. **Idempotency (required)**: delivery is at least once (offsets are committed after the batch). Make handlers safe to repeat:
   - deterministic Temporal workflow IDs;
   - `INSERT ... ON CONFLICT DO NOTHING`;
   - or a processed-events table.
6. **Tracing** is automatic. kotel injects `traceparent` on produce, and the consumer continues it in `handle()`.
7. **Failure handling**: the consumer retries 3 times with backoff, then drops the record and logs it. For a DLQ, publish to `<topic>.dlq` at the marked extension point in `consumer.go`.
8. **Tests**:
   - module unit tests for `HandleX` with mocks;
   - for the publisher, an e2e step like `eventPublished` in `tests/e2e/steps_test.go` that reads the topic from the start and matches the key.

9. **Benchmarks** (see `add-benchmark`): a handler benchmark in `internal/handler/event/<domain>_bench_test.go` (decode + dispatch to a stub module) and a core benchmark for `HandleX`.

Guaranteed delivery: if the publish must not be lost after a DB write, implement a transactional outbox (see README §2.11).

Verify: `make lint test test-e2e`. Locally, check messages in Redpanda Console (http://localhost:8081).
