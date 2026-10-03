"""Server-side adapter for an existing chat-compatible Databricks endpoint.
No orchestration or deployment choice is implied by this transport adapter.
"""
import os
import json
from urllib.parse import quote
import httpx
from pydantic import BaseModel, Field

class ServingError(RuntimeError):
    pass

class HRVMetrics(BaseModel):
    rmssd_ms: float | None = Field(default=None, ge=0, allow_inf_nan=False)
    sdnn_ms: float | None = Field(default=None, ge=0, allow_inf_nan=False)
    rr_count: int = Field(ge=0)

class PreviewResponse(BaseModel):
    mode: str
    message: str
    metrics: HRVMetrics
    tool_trace: list[dict]
    chart_spec: dict
    next_step: str | None = None
    question: str | None = None
    error: str | None = None
    evidence: dict | None = None

async def invoke_agent(events: list[dict], transport=None, prompt='Explain the session pattern and suggest a useful next step'):
    host = os.getenv('DATABRICKS_HOST','').rstrip('/')
    token = os.getenv('DATABRICKS_TOKEN','')
    endpoint = os.getenv('DATABRICKS_AGENT_ENDPOINT','pacepilot-agent')
    if not host.startswith('https://') or not token or not endpoint:
        raise ServingError('Configure DATABRICKS_HOST, DATABRICKS_TOKEN, and DATABRICKS_AGENT_ENDPOINT in .env')
    if not events:
        raise ServingError('No Silver notifications exist for the selected session.')
    async with httpx.AsyncClient(timeout=180,transport=transport) as client:
        response = await client.post(f'{host}/serving-endpoints/{quote(endpoint,safe="")}/invocations',
            headers={'Authorization':f'Bearer {token}'},
            json={'dataframe_records':[{'prompt':prompt,'telemetry_json':json.dumps(events)}]})
    if response.status_code in (401,403):
        raise ServingError('Serving access denied. Check token serving scope and CAN QUERY permission on pacepilot-agent.')
    if not response.is_success:
        raise ServingError(f'Model Serving failed (HTTP {response.status_code}). Check endpoint status and logs.')
    try:
        prediction = response.json()['predictions'][0]['response_json']
        result = PreviewResponse.model_validate_json(prediction)
        if result.mode not in {'deterministic_preview','llm_agent','llm_unavailable'}:
            raise ValueError('Unexpected serving mode')
        return result.model_dump()
    except (KeyError,IndexError,ValueError,TypeError) as error:
        raise ServingError('Endpoint returned an unexpected response; confirm it serves the registered wearable_agent model.') from error

async def invoke_chat(messages: list[dict]) -> dict:
    host = os.environ['DATABRICKS_HOST'].rstrip('/')
    if not host.startswith('https://'):
        raise ValueError('DATABRICKS_HOST must use HTTPS')
    endpoint = quote(os.environ['DATABRICKS_SERVING_ENDPOINT'], safe='')
    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.post(f'{host}/serving-endpoints/{endpoint}/invocations',
            headers={'Authorization': f"Bearer {os.environ['DATABRICKS_TOKEN']}"},
            json={'messages': messages, 'max_tokens': 512})
        response.raise_for_status()
        return response.json()
