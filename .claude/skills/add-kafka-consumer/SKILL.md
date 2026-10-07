---
name: add-kafka-consumer
description: Add a Kafka/Redpanda topic, event publisher and consumer handler to this Go service using franz-go, with trace propagation, idempotency and worker registration. Use when asked to publish or consume events, add a topic, or react asynchronously to something.
---

# Add a Kafka topic, publisher and consumer

Reference files:
- `internal/const/events/` (event catalog: `events.go` with `Domains()`, `All()`, `Topics()`, `AuditConsumerGroup`, `HeaderEventType`; one `<domain>.go` per DPS domain)
- `internal/const/messaging/kafka/kafka.go` (`NewProducer`, `NewConsumer`, shared kotel `Tracer`)
- `internal/handler/event/audit.go` (`AuditConsumer`: the **never-drop reference**; per-poll, partitions in parallel, retry until stored, commit after store)
- `internal/handler/event/envelope.go` (`decodeRecord`: header, Schema Registry framing, `EventMetadata` field 1, never fails) with `envelope_test.go` and `envelope_bench_test.go`
- `internal/handler/event/consumer.go` (generic poll loop, `HandlerFunc` map keyed by topic; drops after retries, for non-audit consumers only; nothing uses it today)
- `config/config.go` (`Kafka`, `Topics` + `Topics.List()`, `Audit`) and `config/config.yaml` (`kafka.topics.<domain>`, `consumer_group: audit.universal`, `audit.{batch_size,max_poll_records,retry_max_backoff}`)
- `initiator/initiator.go` (`BuildWorker`, `Worker.Run` / `Close`, `InitiateWorker`)
- `tests/integration/consumer_test.go` (the **container-test reference**: testcontainers Redpanda + Postgres, the real worker in-process; `make test-integration`)

There is **no publisher in the repo yet**. The only consumer is the audit consumer, which calls `module.Audit.Record`.

## Steps

1. **Topic and event names**: DPS topics are `<domain>.events` (17 domains); the event name (`domain.entity.event.vN`, mirroring dps-contracts) travels in the `x-dps-event-type` header (`events.HeaderEventType`). For a new domain or event type, add it to `internal/const/events/<domain>.go` and to the `domains` list, add a field to `config.Topics` and an entry under `kafka.topics` in `config.yaml`, and keep `events_test.go` passing. Topics are auto-created in dev; in production, create them with explicit partitions and retention.
2. **Payload**: the envelope is `dps.events.v1.EventMetadata` from `pkg/dpsapi/gen` (`make proto`). Map it to a model in `internal/const/models/` (as `AuditRecord` does) at the adapter; the core never sees proto types.
3. **Publish** (outbound, none in the repo yet):
   - Add a publisher port in `internal/module`, then run `make mocks`.
   - Implement it in `internal/storage/publisher/<domain>.go`: marshal, then `kgo.Record{Topic, Key: []byte(aggregateID), Value, Headers}` with the event-type header, then `ProduceSync(ctx, rec).FirstErr()`, wrapping errors in `apperrors.ErrPublish`.
   - **Key by aggregate ID** so a single aggregate's events are ordered on one partition.
   - Wire it in `initiator/module.go` (it needs `p.Producer`; set `Producer: true` in the process `needs`, which are Postgres only today).
4. **Consume** (inbound):
   - **Never-drop (audit-like) consumer**: follow `AuditConsumer`. Decode in a separate file like `envelope.go` that never fails, loop on `PollRecords`, store each partition in order (partitions concurrently), retry until stored, then `CommitUncommittedOffsets` and `AllowRebalance`. Create the client with `kgo.BlockRebalanceOnPoll()` and `defer client.AllowRebalance()` in `Run`, or `Close` hangs in `LeaveGroup`.
   - **Other consumers**: `internal/handler/event/<domain>.go` with a `func X(m module.X) HandlerFunc` that decodes the record and calls a module method, run by the generic `event.NewConsumer(...)`.
   - The consumer group is `cfg.Kafka.ConsumerGroup` (`audit.universal`, `events.AuditConsumerGroup`); a separate process such as the planned search indexer (`audit.search-indexer`) uses its own group.
   - Wire it in `initiator`, as `BuildWorker` does: `kafka.NewConsumer(ctx, cfg.Kafka, cfg.Kafka.Topics.List(), opts...)` (auto-commit off; a new group starts at the earliest offset), build the consumer, run it in the errgroup of `InitiateWorker`, and close the client before `Platform.Close`.
5. **Idempotency (required)**: delivery is at least once. Make handlers safe to repeat, as audit does: the producer's `event_id` is the primary key and `InsertAuditRecords` uses `ON CONFLICT (event_id) DO NOTHING`.
6. **Tracing** is automatic. kotel injects `traceparent` on produce; the generic consumer continues it in `handle()`, and `AuditConsumer` per batch from its first record (`WithProcessSpan`).
7. **Failure handling**:
   - The generic `consumer.go` retries 3 times with backoff, then **drops** the record and logs it; `ErrInvalidInput` is dropped at once. A DLQ (`<topic>.dlq`) would go at the marked extension point.
   - **Audit never drops a record** (`audit.go`). A failed batch is retried with backoff from 200 ms up to `audit.retry_max_backoff` until stored or shutdown, including `ErrInvalidInput` (logged as a bug). An undecodable record is stored with `metadata_valid=false`, event ID `kafka:<topic>/<partition>/<offset>`, the type from the header (then metadata, then `unknown`) and the Kafka timestamp. Offsets are committed only after every partition of the poll is stored; a shutdown mid-poll commits nothing. Trade-off: a partition stuck retrying holds back the commit of the others in that poll (R4 in `docs/architecture.md`). Do not reuse the generic drop path for audit.
8. **Tests**:
   - module unit tests with mocks, as in `module/audit_test.go`;
   - decoder tests that build `*kgo.Record` values directly (valid, Schema Registry framed, undecodable, missing header), as in `envelope_test.go`;
   - a container test like `tests/integration/consumer_test.go`: the real worker (`initiator.BuildWorker`) against testcontainers Redpanda + Postgres, checking dedupe, undecodable records kept, group lag 0, resume after restart, and no commit during a store outage. `tests/integration` may import `initiator` (depguard `composition-root`); skip with `testing.Short()`.
9. **Benchmarks** (see `add-benchmark`): a decode benchmark like `BenchmarkDecodeRecord` in `envelope_bench_test.go` (about 1 µs, 13 allocs per record) or a handler benchmark in `internal/handler/event/<domain>_bench_test.go`, and a core benchmark like `BenchmarkRecord`.

Guaranteed delivery: if a publish must not be lost after a DB write, implement a transactional outbox rather than publishing after commit.

Verify: `make lint test test-integration`. Locally, check messages in Redpanda Console (http://localhost:8081).
