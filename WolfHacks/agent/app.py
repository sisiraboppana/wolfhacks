"""Local telemetry gateway, Databricks readback, and hosted-agent transport."""
import asyncio
import json
import os
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from fastapi import FastAPI, HTTPException, Query
from agent.cloud_session import scope as analysis_scope, snapshot_id
from fastapi.responses import StreamingResponse
from dotenv import load_dotenv
from kinesis_producer.mock import MockGenerator
from shared.schemas import TelemetryEvent, ChatRequest
from pydantic import BaseModel, Field

load_dotenv()
history: deque[TelemetryEvent] = deque(maxlen=600)
seen: set[tuple] = set()
subscribers: set[asyncio.Queue] = set()

def accept(event):
    key = (event.user_id, event.session_id, event.sequence)
    if key in seen:
        return False
    if len(history) == history.maxlen:
        old = history[0]
        seen.discard((old.user_id, old.session_id, old.sequence))
    seen.add(key)
    history.append(event)
    for queue in subscribers:
        if queue.full():
            queue.get_nowait()
        queue.put_nowait(event)
    return True

async def mock_loop():
    generator = MockGenerator(session_id="builtin-demo")
    while True:
        accept(generator.next())
        await asyncio.sleep(1)

@asynccontextmanager
async def lifespan(app):
    mode = os.getenv("STREAM_MODE", "mock")
    if mode not in {"mock", "external"}:
        raise RuntimeError("STREAM_MODE must be mock or external")
    task = asyncio.create_task(mock_loop()) if mode == "mock" else None
    yield
    if task:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

app = FastAPI(title="PacePilot telemetry gateway", lifespan=lifespan)

@app.get('/api/databricks/check')
async def check_databricks():
    from agent.databricks_sql import get_counts, DatabricksQueryError
    import httpx
    try:
        return {'status':'connected', 'counts': await get_counts()}
    except DatabricksQueryError as error:
        raise HTTPException(503, str(error)) from error
    except httpx.RequestError as error:
        raise HTTPException(503, 'Cannot reach Databricks. Check connectivity and whether the SQL warehouse is running.') from error

@app.get('/api/databricks/gold')
async def cloud_gold(session_id: str | None = None):
    from agent.databricks_sql import get_gold, DatabricksQueryError
    import httpx
    try:
        windows = await get_gold(session_id=session_id)
        return {'source':'databricks_gold', 'data_kind':windows[-1].get('data_kind','unknown') if windows else 'unknown',
                'fetched_at':datetime.now(timezone.utc).isoformat(), 'windows':windows}
    except DatabricksQueryError as error:
        raise HTTPException(503, str(error)) from error
    except httpx.RequestError as error:
        raise HTTPException(503, 'Cannot reach Databricks. Check connectivity and the SQL warehouse.') from error

@app.get('/api/databricks/sessions')
async def cloud_sessions():
    from agent.databricks_sql import get_sessions, DatabricksQueryError
    import httpx
    try:
        return {'sessions': await get_sessions()}
    except DatabricksQueryError as error:
        raise HTTPException(503,str(error)) from error
    except httpx.RequestError as error:
        raise HTTPException(503,'Cannot reach Databricks SQL warehouse.') from error

@app.get('/api/databricks/session')
async def processed_session(session_id: str = Query(min_length=1, max_length=128)):
    from agent.cloud_session import get_session
    from agent.databricks_sql import DatabricksQueryError
    import httpx
    try:
        return await get_session(session_id)
    except DatabricksQueryError as error:
        raise HTTPException(503,str(error)) from error
    except httpx.RequestError as error:
        raise HTTPException(503,'Databricks could not be reached. The SQL warehouse may be waking up; retry shortly.') from error

@app.get("/api/health")
def health():
    return {"status": "ok", "telemetry_mode": os.getenv("STREAM_MODE", "mock"), "analysis_mode": "local_mock", "agent_mode": "databricks_model_serving", "recording_context_supported": True, "cloud_session_api": 1}

class HostedAnalysisRequest(BaseModel):
    expected_snapshot_id: str | None = Field(default=None, pattern=r'^[0-9a-f]{64}$')
    through_sequence: int | None = Field(default=None, ge=0, le=9223372036854775807)
    session_id: str = Field(min_length=1, max_length=128)
    prompt: str = Field(default='Explain the session pattern and suggest a useful next step', min_length=1,max_length=2000)

@app.post('/api/databricks/analyze')
async def hosted_analysis(request: HostedAnalysisRequest):
    from agent.databricks_sql import get_silver_session, DatabricksQueryError
    from agent.model_serving import invoke_agent, ServingError
    import httpx
    try:
        events = await get_silver_session(request.session_id, through_sequence=request.through_sequence) if request.through_sequence is not None else await get_silver_session(request.session_id)
        actual_snapshot_id = snapshot_id(events)
        if request.expected_snapshot_id is not None and request.expected_snapshot_id != actual_snapshot_id:
            raise HTTPException(409,'Saved data changed since the preview. Refresh the cloud session, then analyze the updated slice.')
        analysis = await invoke_agent(events, prompt=request.prompt)
        return {'source':'databricks_model_serving','session_id':request.session_id,
                'notification_count':len(events),'analysis':analysis,
                'analyzed_at':datetime.now(timezone.utc).isoformat(), 'analysis_scope':analysis_scope(events), 'snapshot_id':actual_snapshot_id}
    except (DatabricksQueryError, ServingError) as error:
        raise HTTPException(503,str(error)) from error
    except httpx.RequestError as error:
        raise HTTPException(503,'Databricks request timed out or could not connect. The serving endpoint may be waking from scale to zero; retry after it is Ready.') from error

@app.post("/api/telemetry")
def ingest(event: TelemetryEvent):
    if os.getenv("STREAM_MODE", "mock") != "external":
        raise HTTPException(409, "Use STREAM_MODE=external for producer uploads")
    return {"accepted": accept(event)}

@app.get("/api/stream/telemetry")
async def stream(user_id: str = "demo-athlete"):
    queue = asyncio.Queue(maxsize=30)
    subscribers.add(queue)
    async def events():
        try:
            for event in list(history)[-60:]:
                if event.user_id == user_id:
                    yield f"data: {event.model_dump_json()}\n\n"
            while True:
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=15)
                    if event.user_id == user_id:
                        yield f"data: {event.model_dump_json()}\n\n"
                except asyncio.TimeoutError:
                    yield ": heartbeat\n\n"
        finally:
            subscribers.discard(queue)
    return StreamingResponse(events(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

@app.post("/api/chat")
def chat(request: ChatRequest):
    events = sorted((e for e in history if e.user_id == request.user_id and e.session_id == request.session_id), key=lambda e:e.sequence)
    from agent.session_tools import inspect_rr_continuity
    metrics = inspect_rr_continuity(events)['metrics']
    kind = 'Recorded replay' if events and all(e.source == 'replay' for e in events) else 'Local'
    message = f"{kind} deterministic analysis: {len(events)} events in the observed slice; RMSSD {metrics['rmssd_ms']} ms. No workload or historical baseline is available, so cardiovascular drift and recovery cannot be assessed."
    return {"mode": "deterministic_preview", "message": message, "metrics": metrics,
        "chart_spec": {"chartType": "LineChart", "metric": "heart_rate", "title": "Session heart rate"}}
