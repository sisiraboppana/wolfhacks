import asyncio
import json
import httpx
import pytest
from fastapi.testclient import TestClient
from agent.model_serving import invoke_agent, ServingError
from agent import app as gateway

def test_custom_serving_contract(monkeypatch):
    monkeypatch.setenv('DATABRICKS_HOST','https://example.cloud.databricks.com')
    monkeypatch.setenv('DATABRICKS_TOKEN','test-only-token')
    monkeypatch.setenv('DATABRICKS_AGENT_ENDPOINT','pacepilot-agent')
    def handle(request):
        payload = json.loads(request.content)
        assert 'dataframe_records' in payload and 'messages' not in payload
        assert json.loads(payload['dataframe_records'][0]['telemetry_json']) == [{'sequence':0}]
        assert request.url.path.endswith('/pacepilot-agent/invocations')
        result = {'mode':'deterministic_preview','message':'test','metrics':{'rmssd_ms':15.55,'sdnn_ms':8.54,'rr_count':4},
            'tool_trace':[{'tool':'compute_hrv','status':'completed'}],'chart_spec':{}}
        return httpx.Response(200,json={'predictions':[{'response_json':json.dumps(result)}]})
    result = asyncio.run(invoke_agent([{'sequence':0}],httpx.MockTransport(handle)))
    assert result['metrics']['rmssd_ms'] == 15.55

def test_serving_auth_error(monkeypatch):
    monkeypatch.setenv('DATABRICKS_HOST','https://example.cloud.databricks.com')
    monkeypatch.setenv('DATABRICKS_TOKEN','test-only-token')
    with pytest.raises(ServingError,match='Serving access denied'):
        asyncio.run(invoke_agent([{}],httpx.MockTransport(lambda r:httpx.Response(403))))

def test_selected_cloud_session_is_forwarded(monkeypatch):
    from agent import databricks_sql, model_serving
    monkeypatch.setenv('STREAM_MODE','external')
    async def silver(session_id):
        assert session_id == 'uploaded-session'
        return [{'session_id':session_id}]
    async def serving(events, prompt):
        assert events == [{'session_id':'uploaded-session'}]
        assert prompt
        return {'mode':'deterministic_preview'}
    monkeypatch.setattr(databricks_sql,'get_silver_session',silver)
    monkeypatch.setattr(model_serving,'invoke_agent',serving)
    with TestClient(gateway.app) as client:
        result = client.post('/api/databricks/analyze',json={'session_id':'uploaded-session'})
        assert result.status_code == 200
        assert result.json()['source'] == 'databricks_model_serving'
        assert result.json()['notification_count'] == 1
