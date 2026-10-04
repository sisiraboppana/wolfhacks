import asyncio
import json
from fastapi.testclient import TestClient
from agent import app as gateway
from kinesis_producer.mock import MockGenerator

def test_external_ingest_dedup_and_analysis(monkeypatch):
    monkeypatch.setenv('STREAM_MODE', 'external')
    gateway.history.clear()
    gateway.seen.clear()
    with TestClient(gateway.app) as client:
        generator = MockGenerator(session_id='test-session')
        event = generator.next().model_dump(mode='json')
        assert client.post('/api/telemetry', json=event).json()['accepted']
        assert not client.post('/api/telemetry', json=event).json()['accepted']
        response = client.post('/api/chat', json={'prompt':'summary','session_id':'test-session'})
        assert response.status_code == 200
        assert response.json()['mode'] == 'deterministic_preview'
        assert '1 events' in response.json()['message']
        event['rr_intervals_ms'] = [-1]
        assert client.post('/api/telemetry', json=event).status_code == 422

def test_sse_initial_event_and_cleanup():
    gateway.history.clear()
    gateway.seen.clear()
    event = MockGenerator().next()
    gateway.accept(event)
    async def check():
        response = await gateway.stream()
        item = await anext(response.body_iterator)
        assert json.loads(item.removeprefix('data: ').strip())['sequence'] == 0
        await response.body_iterator.aclose()
        assert not gateway.subscribers
    asyncio.run(check())
