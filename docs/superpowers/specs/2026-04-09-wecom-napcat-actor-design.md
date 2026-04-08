# WeCom + NapCat Unified Actor State Machine Design (Redis Streams)

- Date: 2026-04-09
- Status: Approved (design confirmed in chat)
- Scope: Unify WeCom + NapCat inbound processing into one interruptible actor state machine.

## 1. Goals

1. Make replies feel human-like:
- interrupt in-flight generation/delivery when new user messages arrive;
- split one assistant answer into multiple short messages;
- apply configurable delay before first reply and between chunks.
2. Unify inbound behavior for WeCom and NapCat.
3. Provide runtime hot-reloadable behavior settings via admin-ui + `.env` fallback.
4. Ensure delivery reliability with at-least-once processing, idempotency, retries, and DLQ.

## 2. Non-Goals

1. No Kafka introduction in this phase.
2. No historical conversation schema rewrite.
3. No cross-instance strict exactly-once guarantee.

## 3. Confirmed Product Rules

1. New user message immediately interrupts both `GENERATING` and `DELIVERING`.
2. Input aggregation uses both dimensions:
- `debounce_ms=2400`
- `max_messages_per_turn=10`
3. Overflow messages beyond one turn are queued for next turn (never dropped).
4. Delivery behavior:
- first reply delay fixed `300ms`;
- chunk delay fixed `200ms`;
- reply chunk count random `1-5`.
5. Reliability:
- at-least-once processing;
- retry up to 3 times with exponential backoff;
- after max retries, push to DLQ.
6. Runtime config source:
- admin runtime config first;
- `.env` fallback;
- hot reload without service restart.

## 4. High-Level Architecture

### 4.1 Components

1. `InboundEventProducer` (channel adapters)
- WeCom/NapCat receive events and append normalized events to Redis Stream.
- They no longer call graph execution directly.

2. `ActorRouterConsumer`
- Reads Redis Stream events via consumer group.
- Routes each event by `actor_key = "{channel}:{external_user_id}"`.
- Ensures per-actor serialized handling.

3. `ConversationActor`
- Owns per-user state machine and cancellation tokens/tasks.
- State transitions: `COLLECTING -> GENERATING -> DELIVERING -> COLLECTING`.
- On newer message arrival: increments `generation_version`, cancels in-flight tasks, rebuilds next turn.

4. `ReplyPlanner`
- Converts generated reply into 1-5 chunks based on config and text heuristics.

5. `DeliveryExecutor`
- Sends chunks through existing `channel_dispatcher`.
- Checks version before each send to prevent stale output.

6. `ActorStateStore` (DB lightweight)
- Persists minimal inflight state for observability/recovery.

7. `DedupStore` (DB)
- Stores processed `event_id + actor_key` for idempotency.

### 4.2 Data Plane

1. Stream: `stream:inbound_events`
2. DLQ: `stream:dlq`
3. Consumer Group: `cg:inbound_actors`

## 5. Inbound Event Contract

Normalized stream event fields:

1. `event_id` (string, globally unique or deterministic hash)
2. `channel` (`wecom` | `napcat`)
3. `external_user_id` (string)
4. `msg_type` (default `text`)
5. `content` (string)
6. `received_at` (ISO8601)
7. `trace_id` (optional)
8. `retry_count` (int, default 0)

## 6. Actor Lifecycle and Interrupt Model

### 6.1 Turn Batching

1. Incoming event appended into actor buffer.
2. Trigger generate when either:
- no new message for `debounce_ms`, or
- buffer length reaches `max_messages_per_turn`.

### 6.2 Interrupt Semantics

When event arrives while actor is `GENERATING` or `DELIVERING`:

1. `generation_version += 1`
2. cancel `llm_task` if alive
3. cancel `delivery_task` if alive
4. append new message into buffer
5. schedule next generation from latest buffered state

### 6.3 Stale-Output Guard

1. Each generation/delivery operation captures `local_version`.
2. Before sending each chunk, compare with actor current version.
3. Mismatch means stale job; stop immediately.

## 7. Prompting and Humanization Strategy

1. Keep existing persona framework.
2. Add generation constraints for conversational realism:
- produce naturally short mobile-chat chunks;
- avoid AI-assistant framing and templated empathy openers;
- vary openers/endings and rhetorical shape.
3. Generate machine-readable chunk candidate list first, then safe-normalize before sending.
4. If parsing fails, fallback to deterministic splitter and still enforce chunk count cap.

## 8. Configuration Model

New runtime section: `channels_actor`.

Fields:

1. `redis_url` (string)
2. `redis_password` (string)
3. `stream_inbound` (default `stream:inbound_events`)
4. `stream_dlq` (default `stream:dlq`)
5. `debounce_ms` (default `2400`)
6. `max_messages_per_turn` (default `10`)
7. `first_reply_delay_ms` (default `300`)
8. `chunk_delay_ms` (default `200`)
9. `reply_chunk_min` (default `1`)
10. `reply_chunk_max` (default `5`)
11. `interrupt_on_new_message` (default `true`)
12. `retry_max_attempts` (default `3`)
13. `retry_backoff_base_ms` (default `300`)

Validation rules:

1. `debounce_ms >= 0`
2. `max_messages_per_turn >= 1`
3. `reply_chunk_min >= 1`
4. `reply_chunk_max >= reply_chunk_min`
5. `retry_max_attempts >= 0`

Config precedence and hot reload:

1. read runtime-config DB first;
2. fallback to `.env`;
3. reload config at each actor cycle boundary and before delivery start.

## 9. Persistence Additions

### 9.1 Table: `actor_inflight_state`

Columns:

1. `id`
2. `channel`
3. `external_user_id`
4. `generation_version`
5. `status` (`collecting`|`generating`|`delivering`)
6. `buffer_count`
7. `last_event_at`
8. `updated_at`

Unique index on `(channel, external_user_id)`.

### 9.2 Table: `inbound_event_dedup`

Columns:

1. `id`
2. `event_id`
3. `actor_key`
4. `processed_at`

Unique index on `(event_id, actor_key)`.

## 10. Reliability and Failure Handling

1. Consume with Redis Stream consumer group.
2. Dedup check before processing.
3. On failure:
- increment retry count;
- backoff: `base * 2^attempt`;
- re-enqueue until max attempts.
4. Exceeded retries -> push event with error metadata to DLQ stream.
5. Worker restart behavior:
- pending entries reclaimed using XPENDING/XCLAIM strategy;
- inflight state table used for diagnostics, not strict recovery replay.

## 11. Channel Integration Changes

1. WeCom callback path:
- replace direct aggregation/graph execution with stream append.

2. NapCat websocket path:
- replace direct awaited graph execution with stream append;
- avoid blocking ws message loop by heavy business logic.

3. Outbound remains via `channel_dispatcher` so channel-specific send stays centralized.

## 12. Observability

Structured logs with keys:

1. `actor_key`
2. `generation_version`
3. `event_id`
4. `phase` (`collecting`|`generating`|`delivering`|`interrupt`|`retry`|`dlq`)
5. `latency_ms`

Counters/metrics:

1. inbound events total
2. interrupted generations total
3. interrupted deliveries total
4. retries total
5. dlq total
6. average first-token latency and full-turn latency

## 13. Testing Strategy

### 13.1 Unit Tests

1. debounce trigger behavior
2. max message threshold behavior
3. interrupt during generating
4. interrupt during delivering
5. stale version suppression
6. reply chunk range enforcement
7. retry backoff progression
8. DLQ routing after max attempts

### 13.2 Integration Tests

1. WeCom inbound -> stream -> actor -> chunked outbound
2. NapCat inbound -> stream -> actor -> chunked outbound
3. hot-update runtime config then verify next turn uses new values
4. duplicate event ingestion only processed once

## 14. Migration and Rollout Plan

1. Add new tables and runtime config schema fields.
2. Ship producer writes from WeCom/NapCat behind feature flag `ACTOR_PIPELINE_ENABLED`.
3. Run actor consumer in shadow mode logs-only (optional short phase).
4. Enable active delivery for actor pipeline.
5. Remove legacy direct inbound processing path after validation window.

## 15. Risks and Mitigations

1. Risk: cancellation races causing partial stale sends.
- Mitigation: version check before each chunk send.

2. Risk: replay/duplicate from at-least-once stream semantics.
- Mitigation: dedup table unique key.

3. Risk: Redis outage blocks ingress.
- Mitigation: fail-fast alerting + optional local fallback queue in future iteration.

4. Risk: increased latency from debounce.
- Mitigation: configurable debounce and threshold, defaults already confirmed by product decision.

## 16. Acceptance Criteria

1. WeCom and NapCat both route inbound through one actor pipeline.
2. New message interrupts in-flight generation and chunk delivery within one event loop cycle.
3. Replies are delivered in 1-5 chunks with configured delays.
4. Runtime config update is effective without restart.
5. Processing uses at-least-once + dedup; failed messages retry and finally enter DLQ.
6. Existing conversation persistence and memory update flow remain compatible.
