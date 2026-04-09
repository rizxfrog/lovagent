# Unified Inbound Actor Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace direct WeCom/NapCat inbound processing with one Redis Streams based per-user actor pipeline that supports interruptible generation, chunked delayed replies, and runtime hot-reload settings.

**Architecture:** WeCom/NapCat adapters become event producers only; an `InboundActorService` consumes stream events, routes by `channel:external_user_id`, batches messages with debounce/threshold, and runs versioned generate-deliver tasks that are canceled on new inbound events. Runtime behavior is read from `runtime_config` with `.env` fallback; reliability is at-least-once + dedup + retry + DLQ.

**Tech Stack:** FastAPI, asyncio, redis-py (`redis.asyncio`), SQLAlchemy, Pydantic, pytest, React/TypeScript admin-ui.

---

### Task 1: Add Redis + Actor Config Surface

**Files:**
- Modify: `requirements.txt`
- Modify: `app/config.py`
- Modify: `app/services/runtime_config_service.py`
- Modify: `app/schemas/admin.py`
- Modify: `app/routers/setup.py`
- Modify: `app/routers/admin.py`
- Modify: `admin-ui/src/types.ts`
- Modify: `admin-ui/src/api.ts`
- Test: `tests/test_runtime_config_service.py`
- Test: `tests/test_admin_api.py`

- [ ] **Step 1: Write failing tests for actor config defaults + API read/write**

```python
# tests/test_runtime_config_service.py

def test_actor_settings_defaults_from_env(monkeypatch):
    monkeypatch.setenv("ACTOR_DEBOUNCE_MS", "2400")
    cfg = runtime_config_service.get_effective_actor_config()
    assert cfg["debounce_ms"] == 2400
    assert cfg["max_messages_per_turn"] == 10

# tests/test_admin_api.py

def test_actor_settings_roundtrip(client):
    payload = {"debounce_ms": 2400, "max_messages_per_turn": 10}
    resp = client.put("/admin-api/actor-settings", json=payload)
    assert resp.status_code == 200
    current = client.get("/admin-api/actor-settings").json()
    assert current["debounce_ms"] == 2400
```

- [ ] **Step 2: Run tests to confirm failure**

Run: `python -m pytest tests/test_runtime_config_service.py tests/test_admin_api.py -q`
Expected: FAIL for missing actor config getters/endpoints/schemas.

- [ ] **Step 3: Implement backend + frontend config contracts**

```python
# app/config.py (new env fields)
redis_url: str = os.getenv("REDIS_URL", "redis://127.0.0.1:6379")
redis_password: str = os.getenv("REDIS_PASSWORD", "")
actor_pipeline_enabled: bool = os.getenv("ACTOR_PIPELINE_ENABLED", "false").lower() == "true"
actor_debounce_ms: int = int(os.getenv("ACTOR_DEBOUNCE_MS", "2400"))
actor_max_messages_per_turn: int = int(os.getenv("ACTOR_MAX_MESSAGES_PER_TURN", "10"))
actor_first_reply_delay_ms: int = int(os.getenv("ACTOR_FIRST_REPLY_DELAY_MS", "300"))
actor_chunk_delay_ms: int = int(os.getenv("ACTOR_CHUNK_DELAY_MS", "200"))
actor_reply_chunk_min: int = int(os.getenv("ACTOR_REPLY_CHUNK_MIN", "1"))
actor_reply_chunk_max: int = int(os.getenv("ACTOR_REPLY_CHUNK_MAX", "5"))
actor_retry_max_attempts: int = int(os.getenv("ACTOR_RETRY_MAX_ATTEMPTS", "3"))
actor_retry_backoff_base_ms: int = int(os.getenv("ACTOR_RETRY_BACKOFF_BASE_MS", "300"))
```

```python
# app/services/runtime_config_service.py (new section + resolver)
DEFAULT_RUNTIME_CONFIG["channels_actor"] = {
    "redis_url": "",
    "redis_password": "",
    "stream_inbound": "stream:inbound_events",
    "stream_dlq": "stream:dlq",
    "debounce_ms": 2400,
    "max_messages_per_turn": 10,
    "first_reply_delay_ms": 300,
    "chunk_delay_ms": 200,
    "reply_chunk_min": 1,
    "reply_chunk_max": 5,
    "interrupt_on_new_message": True,
    "retry_max_attempts": 3,
    "retry_backoff_base_ms": 300,
}

def get_effective_actor_config(self) -> Dict:
    raw = self.get_config()["channels_actor"]
    return {
        "pipeline_enabled": bool(raw.get("pipeline_enabled", settings.actor_pipeline_enabled)),
        "redis_url": str(raw.get("redis_url") or settings.redis_url).strip(),
        "redis_password": str(raw.get("redis_password") or settings.redis_password).strip(),
        "stream_inbound": str(raw.get("stream_inbound") or "stream:inbound_events").strip(),
        "stream_dlq": str(raw.get("stream_dlq") or "stream:dlq").strip(),
        "debounce_ms": max(0, int(raw.get("debounce_ms") or settings.actor_debounce_ms)),
        "max_messages_per_turn": max(1, int(raw.get("max_messages_per_turn") or settings.actor_max_messages_per_turn)),
    }
```

```ts
// admin-ui/src/types.ts
export type ActorSettings = {
  redis_url: string;
  has_redis_password: boolean;
  stream_inbound: string;
  stream_dlq: string;
  debounce_ms: number;
  max_messages_per_turn: number;
  first_reply_delay_ms: number;
  chunk_delay_ms: number;
  reply_chunk_min: number;
  reply_chunk_max: number;
  interrupt_on_new_message: boolean;
  retry_max_attempts: number;
  retry_backoff_base_ms: number;
};
```

- [ ] **Step 4: Re-run tests**

Run: `python -m pytest tests/test_runtime_config_service.py tests/test_admin_api.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add requirements.txt app/config.py app/services/runtime_config_service.py app/schemas/admin.py app/routers/setup.py app/routers/admin.py admin-ui/src/types.ts admin-ui/src/api.ts tests/test_runtime_config_service.py tests/test_admin_api.py
git commit -m "feat(config): add actor pipeline runtime settings and admin api"
```

### Task 2: Add Persistence for Inflight State + Dedup

**Files:**
- Modify: `app/models/admin.py`
- Modify: `app/models/__init__.py`
- Modify: `app/models/database.py`
- Create: `app/models/actor.py`
- Test: `tests/test_actor_models.py`

- [ ] **Step 1: Write failing model migration/CRUD tests**

```python
# tests/test_actor_models.py

def test_actor_inflight_state_upsert(db_session):
    state = ActorInflightState(channel="napcat", external_user_id="123", generation_version=1, status="collecting")
    db_session.add(state)
    db_session.commit()
    assert state.id is not None

def test_inbound_event_dedup_unique(db_session):
    first = InboundEventDedup(event_id="evt-1", actor_key="napcat:123")
    db_session.add(first)
    db_session.commit()
    db_session.add(InboundEventDedup(event_id="evt-1", actor_key="napcat:123"))
    with pytest.raises(Exception):
        db_session.commit()
```

- [ ] **Step 2: Run tests to confirm failure**

Run: `python -m pytest tests/test_actor_models.py -q`
Expected: FAIL because new models/tables do not exist.

- [ ] **Step 3: Implement new SQLAlchemy models + startup schema ensure**

```python
# app/models/actor.py
class ActorInflightState(Base):
    __tablename__ = "actor_inflight_state"
    id = Column(Integer, primary_key=True)
    channel = Column(String(32), nullable=False)
    external_user_id = Column(String(128), nullable=False)
    generation_version = Column(Integer, nullable=False, default=0)
    status = Column(String(24), nullable=False, default="collecting")
    buffer_count = Column(Integer, nullable=False, default=0)
    last_event_at = Column(DateTime)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    __table_args__ = (UniqueConstraint("channel", "external_user_id", name="uq_actor_inflight_identity"),)

class InboundEventDedup(Base):
    __tablename__ = "inbound_event_dedup"
    id = Column(Integer, primary_key=True)
    event_id = Column(String(128), nullable=False)
    actor_key = Column(String(180), nullable=False)
    processed_at = Column(DateTime, default=datetime.utcnow)
    __table_args__ = (UniqueConstraint("event_id", "actor_key", name="uq_actor_dedup"),)
```

- [ ] **Step 4: Re-run tests**

Run: `python -m pytest tests/test_actor_models.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add app/models/actor.py app/models/admin.py app/models/__init__.py app/models/database.py tests/test_actor_models.py
git commit -m "feat(actor): add inflight and dedup persistence models"
```

### Task 3: Implement Redis Stream Bus + Actor Core

**Files:**
- Create: `app/services/redis_stream_bus.py`
- Create: `app/services/inbound_actor_service.py`
- Modify: `app/services/__init__.py`
- Modify: `app/main.py`
- Test: `tests/test_inbound_actor_service.py`

- [ ] **Step 1: Write failing actor behavior tests**

```python
# tests/test_inbound_actor_service.py

@pytest.mark.asyncio
async def test_interrupt_generation_on_new_message(actor_service):
    await actor_service.ingest_event({
        "event_id": "e-1",
        "channel": "napcat",
        "external_user_id": "1",
        "msg_type": "text",
        "content": "第一条",
        "received_at": "2026-04-09T09:00:00",
    })
    await actor_service.ingest_event({
        "event_id": "e-2",
        "channel": "napcat",
        "external_user_id": "1",
        "msg_type": "text",
        "content": "第二条",
        "received_at": "2026-04-09T09:00:01",
    })
    state = actor_service.get_debug_state("napcat:1")
    assert state["generation_version"] >= 1
    assert state["interrupted_count"] >= 1

@pytest.mark.asyncio
async def test_overflow_goes_to_next_turn(actor_service):
    for i in range(12):
        await actor_service.ingest_event({
            "event_id": f"e-{i}",
            "channel": "napcat",
            "external_user_id": "1",
            "msg_type": "text",
            "content": f"m-{i}",
            "received_at": f"2026-04-09T09:00:{i:02d}",
        })
    turns = actor_service.get_emitted_turns("napcat:1")
    assert len(turns[0]["messages"]) == 10
    assert len(turns[1]["messages"]) == 2
```

- [ ] **Step 2: Run tests to confirm failure**

Run: `python -m pytest tests/test_inbound_actor_service.py -q`
Expected: FAIL because actor service/bus not implemented.

- [ ] **Step 3: Implement bus + per-actor state machine + retry/DLQ skeleton**

```python
# app/services/redis_stream_bus.py
class RedisStreamBus:
    async def publish_inbound(self, event: Dict[str, str]) -> str:
        return await self._client.xadd(self._stream_inbound, event)
    async def publish_dlq(self, payload: Dict[str, str]) -> str:
        return await self._client.xadd(self._stream_dlq, payload)
    async def read_group(self, *, count: int = 50, block_ms: int = 1000):
        return await self._client.xreadgroup(self._group, self._consumer, {self._stream_inbound: ">"}, count=count, block=block_ms)
    async def ack(self, message_id: str) -> None:
        await self._client.xack(self._stream_inbound, self._group, message_id)
```

```python
# app/services/inbound_actor_service.py
@dataclass
class ActorState:
    generation_version: int = 0
    status: str = "collecting"
    buffer: deque[str] = field(default_factory=deque)
    llm_task: asyncio.Task | None = None
    delivery_task: asyncio.Task | None = None

class InboundActorService:
    async def ingest_event(self, event: Dict[str, str]) -> None:
        actor_key = f"{event['channel']}:{event['external_user_id']}"
        state = self._actors.setdefault(actor_key, ActorState())
        state.buffer.append(str(event["content"]))
        await self._maybe_schedule(actor_key, state)
    async def _interrupt(self, actor_key: str) -> None:
        state.generation_version += 1
        if state.llm_task and not state.llm_task.done():
            state.llm_task.cancel()
        if state.delivery_task and not state.delivery_task.done():
            state.delivery_task.cancel()
```

- [ ] **Step 4: Re-run tests**

Run: `python -m pytest tests/test_inbound_actor_service.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add app/services/redis_stream_bus.py app/services/inbound_actor_service.py app/services/__init__.py app/main.py tests/test_inbound_actor_service.py
git commit -m "feat(actor): implement redis stream consumer and interruptible actor core"
```

### Task 4: Switch WeCom + NapCat Inbound to Producer Mode

**Files:**
- Modify: `app/routers/wecom.py`
- Modify: `app/services/napcat_service.py`
- Modify: `app/services/incoming_aggregation_service.py`
- Modify: `app/graph/graphs/incoming.py`
- Test: `tests/test_wecom_callback.py`
- Test: `tests/test_incoming_aggregation_service.py`
- Test: `tests/test_napcat_service.py` (new)

- [ ] **Step 1: Write failing tests for producer-only channel adapters**

```python
# tests/test_wecom_callback.py

def test_handler_pushes_stream_event_not_direct_graph(client, mocker):
    publish = mocker.patch("app.routers.wecom.inbound_actor_service.publish_inbound_event", autospec=True)
    response = client.post(
        "/wecom/callback",
        params={"msg_signature": "sig", "timestamp": "1700000000", "nonce": "abc"},
        data="<xml><ToUserName><![CDATA[toUser]]></ToUserName><FromUserName><![CDATA[fromUser]]></FromUserName></xml>",
    )
    assert response.status_code == 200
    publish.assert_called_once()
```

```python
# tests/test_napcat_service.py
@pytest.mark.asyncio
async def test_napcat_message_only_publishes_event(mocker):
    publish = mocker.patch("app.services.napcat_service.inbound_actor_service.publish_inbound_event", autospec=True)
    await napcat_service._handle_message(
        json.dumps(
            {
                "post_type": "message",
                "message_type": "private",
                "user_id": 10001,
                "raw_message": "hello",
            }
        )
    )
    publish.assert_called_once()
```

- [ ] **Step 2: Run tests to confirm failure**

Run: `python -m pytest tests/test_wecom_callback.py tests/test_incoming_aggregation_service.py tests/test_napcat_service.py -q`
Expected: FAIL because adapters still invoke legacy direct flow.

- [ ] **Step 3: Implement producer-mode adapters with legacy flag fallback**

```python
# app/routers/wecom.py
if runtime_config_service.get_effective_actor_config()["pipeline_enabled"]:
    await inbound_actor_service.publish_inbound_event(normalized_event)
else:
    registration = await incoming_aggregation_service.register_event(message)
    if not registration.get("duplicate"):
        incoming_aggregation_service.schedule_user_processing(str(message.get("from_user") or ""))
```

```python
# app/services/napcat_service.py
if runtime_config_service.get_effective_actor_config()["pipeline_enabled"]:
    await inbound_actor_service.publish_inbound_event(
        {
            "event_id": f"napcat:{payload.get('time')}:{payload.get('user_id')}",
            "channel": "napcat",
            "external_user_id": str(payload.get("user_id") or ""),
            "msg_type": "text",
            "content": content,
            "received_at": datetime.now().isoformat(),
        }
    )
    return
await run_incoming_message_graph(
    {
        "channel": "napcat",
        "external_user_id": external_user_id,
        "user_content": content,
    }
)
```

- [ ] **Step 4: Re-run tests**

Run: `python -m pytest tests/test_wecom_callback.py tests/test_incoming_aggregation_service.py tests/test_napcat_service.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add app/routers/wecom.py app/services/napcat_service.py app/services/incoming_aggregation_service.py app/graph/graphs/incoming.py tests/test_wecom_callback.py tests/test_incoming_aggregation_service.py tests/test_napcat_service.py
git commit -m "feat(inbound): route wecom and napcat inbound events through actor pipeline"
```

### Task 5: Human-like Reply Planning and Version-Safe Chunk Delivery

**Files:**
- Modify: `app/prompts/templates.py`
- Modify: `app/services/llm_service.py`
- Modify: `app/services/channel_dispatcher.py`
- Modify: `app/graph/executors/delivery.py`
- Modify: `app/services/inbound_actor_service.py`
- Test: `tests/test_llm_service.py`
- Test: `tests/test_channel_dispatcher.py` (new)
- Test: `tests/test_inbound_actor_service.py`

- [ ] **Step 1: Write failing tests for chunk generation and stale-send suppression**

```python
# tests/test_channel_dispatcher.py
@pytest.mark.asyncio
async def test_send_chunks_stops_on_version_mismatch(dispatcher):
    sent = []
    async def guard():
        return False
    await dispatcher.send_chunked("napcat", "123", ["a", "b", "c"], before_send=guard)
    assert sent == []
```

```python
# tests/test_llm_service.py
@pytest.mark.asyncio
async def test_reply_planner_outputs_1_to_5_chunks(llm_service):
    chunks = llm_service.plan_reply_chunks("好呀，我们今晚一起去看吧，先吃饭")
    assert 1 <= len(chunks) <= 5
```

- [ ] **Step 2: Run tests to confirm failure**

Run: `python -m pytest tests/test_llm_service.py tests/test_channel_dispatcher.py tests/test_inbound_actor_service.py -q`
Expected: FAIL for missing chunk planner and dispatcher API.

- [ ] **Step 3: Implement planner + delayed chunk sender + interrupt-aware delivery**

```python
# app/services/channel_dispatcher.py
async def send_chunked(
    self,
    channel: str,
    external_user_id: str,
    chunks: list[str],
    *,
    first_delay_ms: int,
    chunk_delay_ms: int,
    before_send: Callable[[], Awaitable[bool]],
) -> Dict[str, object]:
    await asyncio.sleep(first_delay_ms / 1000)
    for idx, chunk in enumerate(chunks):
        if not await before_send():
            return {"status": "cancelled", "sent_chunks": idx}
        await self.send_text(channel, external_user_id, chunk)
        if idx < len(chunks) - 1:
            await asyncio.sleep(chunk_delay_ms / 1000)
    return {"status": "sent", "sent_chunks": len(chunks)}
```

```python
# app/services/inbound_actor_service.py
async def _deliver_versioned(self, actor_key: str, channel: str, external_user_id: str, chunks: list[str]):
    local_version = state.generation_version
    async def _guard() -> bool:
        return state.generation_version == local_version
    await channel_dispatcher.send_chunked(
        channel,
        external_user_id,
        chunks,
        first_delay_ms=actor_config["first_reply_delay_ms"],
        chunk_delay_ms=actor_config["chunk_delay_ms"],
        before_send=_guard,
    )
```

- [ ] **Step 4: Re-run tests**

Run: `python -m pytest tests/test_llm_service.py tests/test_channel_dispatcher.py tests/test_inbound_actor_service.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add app/prompts/templates.py app/services/llm_service.py app/services/channel_dispatcher.py app/graph/executors/delivery.py app/services/inbound_actor_service.py tests/test_llm_service.py tests/test_channel_dispatcher.py tests/test_inbound_actor_service.py
git commit -m "feat(reply): add interrupt-safe chunked delayed human-like delivery"
```

### Task 6: End-to-End Verification and Rollout Guardrails

**Files:**
- Modify: `.env.example`
- Modify: `README.md`
- Modify: `tests/test_setup_api.py`
- Modify: `tests/test_admin_api.py`

- [ ] **Step 1: Add failing tests for setup/admin exposure of actor settings**

```python
# tests/test_setup_api.py

def test_setup_status_includes_actor_settings(client):
    resp = client.get("/setup/status")
    assert "channels_actor" in resp.json().get("raw", {})
```

- [ ] **Step 2: Run tests to confirm failure**

Run: `python -m pytest tests/test_setup_api.py tests/test_admin_api.py -q`
Expected: FAIL for missing status/docs sync assertions.

- [ ] **Step 3: Document env and rollout steps**

```dotenv
# .env.example
ACTOR_PIPELINE_ENABLED=false
REDIS_URL=redis://127.0.0.1:6379
REDIS_PASSWORD=
ACTOR_DEBOUNCE_MS=2400
ACTOR_MAX_MESSAGES_PER_TURN=10
ACTOR_FIRST_REPLY_DELAY_MS=300
ACTOR_CHUNK_DELAY_MS=200
ACTOR_REPLY_CHUNK_MIN=1
ACTOR_REPLY_CHUNK_MAX=5
```

- [ ] **Step 4: Run full test sweep and build admin-ui**

Run: `python -m pytest tests -q`
Expected: PASS.

Run: `npm --prefix admin-ui run build`
Expected: build succeeds and emits `admin-ui/dist`.

- [ ] **Step 5: Final integration commit**

```bash
git add .env.example README.md tests/test_setup_api.py tests/test_admin_api.py
git commit -m "chore(actor): document rollout and validate actor settings exposure"
```

## Execution Notes

1. Keep feature flag `ACTOR_PIPELINE_ENABLED=false` until end-to-end tests pass on staging.
2. During rollout, log both stream enqueue and actor delivery events with `actor_key` + `generation_version`.
3. If rollback needed, set `ACTOR_PIPELINE_ENABLED=false` and keep Redis consumer disabled.
