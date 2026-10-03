"""Local mock gateway. Cloud telemetry readback and agent hosting are pending."""
import asyncio
import json
import os
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from dotenv import load_dotenv
from kinesis_producer.mock import MockGenerator
from shared.schemas import TelemetryEvent, ChatRequest
from pydantic import BaseModel, Field
from shared.metrics import hrv

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
        raise RuntimeError("STREAM_MODE must be mock or external; cloud readback is not implemented yet")
    task = asyncio.create_task(mock_loop()) if mode == "mock" else None
    yield
    if task:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

app = FastAPI(title="PacePilot mock gateway", lifespan=lifespan)

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
async def cloud_gold():
    from agent.databricks_sql import get_gold, DatabricksQueryError
    import httpx
    try:
        windows = await get_gold()
        return {'source':'databricks_gold', 'data_kind':'synthetic',
                'fetched_at':datetime.now(timezone.utc).isoformat(), 'windows':windows}
    except DatabricksQueryError as error:
        raise HTTPException(503, str(error)) from error
    except httpx.RequestError as error:
        raise HTTPException(503, 'Cannot reach Databricks. Check connectivity and the SQL warehouse.') from error

@app.get("/api/health")
def health():
    return {"status": "ok", "telemetry_mode": os.getenv("STREAM_MODE", "mock"), "analysis_mode": "local_mock", "agent_mode": "deterministic_preview"}

class HostedAnalysisRequest(BaseModel):
    session_id: str = Field(min_length=1, max_length=128)
    prompt: str = Field(default='Explain the session pattern and suggest a useful next step', min_length=1,max_length=2000)

@app.post('/api/databricks/analyze')
async def hosted_analysis(request: HostedAnalysisRequest):
    from agent.databricks_sql import get_silver_session, DatabricksQueryError
    from agent.model_serving import invoke_agent, ServingError
    import httpx
    try:
        events = await get_silver_session(request.session_id)
        analysis = await invoke_agent(events, prompt=request.prompt)
        return {'source':'databricks_model_serving','session_id':request.session_id,
                'notification_count':len(events),'analysis':analysis}
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
    # Do not concatenate RR across a missing packet or missing RR measurement.
    segment = []
    previous = None
    for event in events:
        if previous is not None and event.sequence != previous + 1:
            segment = []
        if not event.rr_intervals_ms:
            segment = []
        else:
            segment.extend(event.rr_intervals_ms)
        previous = event.sequence
    metrics = hrv(segment)
    message = f"Mock analysis: {len(events)} notifications in this session; RMSSD {metrics['rmssd_ms']} ms. No workload or historical baseline is available, so cardiovascular drift and recovery cannot be assessed."
    return {"mode": "deterministic_preview", "message": message, "metrics": metrics,
        "chart_spec": {"chartType": "LineChart", "metric": "heart_rate", "title": "Session heart rate"}}
