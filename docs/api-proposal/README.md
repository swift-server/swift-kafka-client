# swift-kafka-client — 1.0 Beta API proposal

**Reviewer:** @FranzBusch
**Author:** @ssikka100
**Status:** Draft — for review before Beta tag
**Related PRs:**
- #248 (merged) — DocC catalog
- #250 — deprecated pass-through removal + `commit() → commitStoredOffsets()`
- #239 — `KafkaContiguousBytes` refactor / `Span` migration (waiting on decision here)
- #244 — auto-register metrics API (waiting on decision here)
- #237 — bounded backpressure for event sequences

## What this PR is

This PR contains **no implementation changes**. It proposes the public API surface for the 1.0 Beta release as a single Swift file (`KafkaAPI.swift`) so you can line-comment on any signature.

The file is not part of the build — it's a review artifact only. Once we agree on the surface, follow-up PRs adopt each change in `Sources/Kafka/`.

## What this PR is not

- Not a re-review of already-approved work (#248 merged; #250 approved for cleanup).
- Not the implementation.
- Not exhaustive on the ~110 config properties per role — those are shown by shape, not enumerated.

## Open decisions

Each is marked in the Swift file with `// OPEN Qn:` next to the relevant signature. Summary:

| ID | Decision | Impact |
|---|---|---|
| Q1 | Delete legacy `KafkaConsumerConfiguration` / `KafkaProducerConfiguration` types entirely, or keep them as `@deprecated`? | Cuts ~670 lines + a public type from the surface. |
| Q2 | Rename `KafkaConsumerConfig` / `KafkaProducerConfig` → `KafkaConsumerConfiguration` / `KafkaProducerConfiguration` (to match `swift-nio`, `swift-service-lifecycle` naming convention)? | Depends on Q1. |
| Q3 | Keep `scheduleCommit(_:)` and `scheduleCommit()`, or drop them? Fire-and-forget silently swallows errors — Swift-native is `Task { try? await commit(...) }`. | Cuts 2 methods. |
| Q4 | Keep `storeOffset(_:)` name (matches librdkafka + Rust + Python), or rename to `markProcessed(_:)` for Swift-native readability? | Ergonomic only. |
| Q5 | Replace `KafkaContiguousBytes` protocol with `Span<UInt8>` / `RawSpan` per your comment on #239? | Breaking. Rewrites `KafkaProducerMessage`, `send`, `sendAndAwait`. Scope: medium. |
| Q6 | Metrics API — keep per-gauge opt-in, or land the auto-register `.enabled(prefix:updateInterval:)` model from #244 as the Beta API? | Rewrites `KafkaConfiguration.ConsumerMetrics` / `.ProducerMetrics`. |
| Q7 | `KafkaError` — keep the flat `code` / `rdKafkaCode` / `isFatal` / `isRetriable` shape, or introduce typed subcases (`.connection`, `.auth`, `.protocol`, `.timeout`)? | Ergonomic + catch-friendliness. |
| Q8 | Add `swift-distributed-tracing` integration in Beta, or defer to 1.0 proper? | Third pillar of observability per production-readiness checklist. |
| Q9 | Backpressure for event sequences (#237) — required for Beta, or 1.0? | Related to the "iterate or events buffer indefinitely" footgun. |

## Deferred to post-1.0 (please confirm cut list)

- Admin client (issues #156, #31, #75) — no `KafkaAdminClient`, no `rd_kafka_metadata` wrapper.
- Transactional producer (#78) — exactly-once semantics.
- Batch consume (`rd_kafka_consume_batch_queue`).
- Batch send (`sendBatch(_:)`).
- Custom partitioner.
- Certificate reloading (blocked upstream in librdkafka).
- KIP-848 new consumer group protocol integration.
- `connectAdditional(brokers:)` (#11).

## How to review

Open `KafkaAPI.swift` and read top-to-bottom. Each `// OPEN Qn:` marker is a decision point. Everything else is either already-approved shape or mechanical (types that don't have design questions).

If a signature is missing from the proposal or misrepresented, flag it — this file was assembled from the current `Sources/Kafka/*.swift` surface plus the settled changes from #250.
