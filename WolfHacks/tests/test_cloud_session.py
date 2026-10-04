import asyncio
import json
import pytest
from fastapi.testclient import TestClient
from agent import cloud_session, databricks_sql, model_serving
from agent import app as gateway
from kinesis_producer.mock import MockGenerator

def events(session='saved-session'):
    generator=MockGenerator(session_id=session)
    return [generator.next().model_dump(mode='json') for _ in range(3)]

@pytest.mark.parametrize('bronze,silver,status',[(0,0,'waiting_for_upload'),(3,0,'processing'),(3,3,'ready')])
def test_session_progress_before_gold_finalizes(monkeypatch,bronze,silver,status):
    async def progress(session):
        assert session=='saved-session'
        return {'counts':{'bronze':bronze,'silver':silver,'gold':0},
                'latest_bronze_ingested_at':None,'latest_silver_ingested_at':None}
    async def saved(session):
        return events() if silver else []
    async def gold(session_id):
        assert session_id=='saved-session'
        return []
    monkeypatch.setattr(cloud_session,'progress',progress)
    monkeypatch.setattr(cloud_session,'get_silver_session',saved)
    monkeypatch.setattr(cloud_session,'get_gold',gold)
    result=asyncio.run(cloud_session.get_session('saved-session'))
    assert result['status']==status
    assert bool(result['analysis_scope']) == bool(silver)
    assert result['windows']==[]
    if silver:
        assert result['analysis_scope']['last_sequence']==2
        assert result['data_kind']=='synthetic'

def test_replay_identity_and_context_are_explicit(monkeypatch):
    rows=events()
    context={'dataset':'ppg_dalia','subject':'S10','hr_method':'ecg_rpeaks_derived',
             'original_offset_seconds':1000,'activity_label':'stairs'}
    for row in rows:
        row.update(source='replay',recording=context)
    async def progress(session):
        return {'counts':{'bronze':3,'silver':3,'gold':0},'latest_bronze_ingested_at':None,'latest_silver_ingested_at':None}
    async def saved(session): return rows
    async def gold(session_id): return []
    monkeypatch.setattr(cloud_session,'progress',progress)
    monkeypatch.setattr(cloud_session,'get_silver_session',saved)
    monkeypatch.setattr(cloud_session,'get_gold',gold)
    result=asyncio.run(cloud_session.get_session('saved-session'))
    assert result['datasets']==['ppg_dalia'] and result['context_complete']
    assert result['data_kind']=='recorded_replay'
    rows[-1]['recording']=None
    assert not asyncio.run(cloud_session.get_session('saved-session'))['context_complete']

def test_progress_counts_are_bound_to_selected_session(monkeypatch):
    async def execute(statement,parameters):
        assert 'get_json_object' in statement
        assert "quoted'session" not in statement
        assert parameters[1]['value']=="quoted'session"
        return [{'layer':name,'records':str(n),'latest_ingested_at':None}
                for name,n in [('bronze',4),('silver',3),('gold',0)]]
    monkeypatch.setattr(cloud_session,'execute',execute)
    state=asyncio.run(cloud_session.progress("quoted'session"))
    assert state['counts']=={'bronze':4,'silver':3,'gold':0}

def test_analysis_reads_the_displayed_snapshot_cutoff(monkeypatch):
    monkeypatch.setenv('STREAM_MODE','external')
    rows=events()
    async def saved(session,through_sequence):
        assert session=='saved-session' and through_sequence==2
        return rows
    async def serving(payload,prompt):
        assert payload==rows
        return {'mode':'llm_agent','message':'Snapshot analysis'}
    monkeypatch.setattr(databricks_sql,'get_silver_session',saved)
    monkeypatch.setattr(model_serving,'invoke_agent',serving)
    with TestClient(gateway.app) as client:
        response=client.post('/api/databricks/analyze',json={'session_id':'saved-session','through_sequence':2})
        assert response.status_code==200
        body=response.json()
        assert body['analysis_scope']['first_sequence']==0
        assert body['analysis_scope']['last_sequence']==2
        assert body['analyzed_at']
        assert client.post('/api/databricks/analyze',json={'session_id':'s','through_sequence':-1}).status_code==422

def test_snapshot_cutoff_is_a_bound_sql_parameter(monkeypatch):
    async def execute(statement,parameters):
        assert 'sequence <= :through_sequence' in statement
        assert parameters[-1]=={'name':'through_sequence','value':'42','type':'BIGINT'}
        return []
    monkeypatch.setattr(databricks_sql,'execute',execute)
    assert asyncio.run(databricks_sql.get_silver_session('s',through_sequence=42))==[]

def test_processed_session_errors_are_actionable(monkeypatch):
    monkeypatch.setenv('STREAM_MODE','external')
    async def unavailable(session):
        raise databricks_sql.DatabricksQueryError('Authentication or permissions failed.')
    monkeypatch.setattr(cloud_session,'get_session',unavailable)
    with TestClient(gateway.app) as client:
        response=client.get('/api/databricks/session?session_id=s')
        assert response.status_code==503
        assert 'Authentication' in response.json()['detail']
        assert client.get('/api/databricks/session?session_id=').status_code==422

def test_late_arrivals_cannot_silently_change_previewed_analysis(monkeypatch):
    monkeypatch.setenv('STREAM_MODE','external')
    original=events()
    expected=cloud_session.snapshot_id(original)
    changed=[dict(row) for row in original]
    changed[-1]['heart_rate'] += 1
    async def saved(session,through_sequence): return changed
    async def serving(*args,**kwargs): raise AssertionError('Changed snapshot must not invoke LLM')
    monkeypatch.setattr(databricks_sql,'get_silver_session',saved)
    monkeypatch.setattr(model_serving,'invoke_agent',serving)
    with TestClient(gateway.app) as client:
        response=client.post('/api/databricks/analyze',json={'session_id':'saved-session','through_sequence':2,'expected_snapshot_id':expected})
        assert response.status_code==409
        assert 'Refresh the cloud session' in response.json()['detail']

def test_snapshot_hash_is_stable_and_sensitive_to_actual_recording():
    rows=events()
    first=cloud_session.snapshot_id(rows)
    reordered=[dict(reversed(list(row.items()))) for row in rows]
    assert cloud_session.snapshot_id(reordered)==first
    assert cloud_session.snapshot_id(rows[:-1])!=first
