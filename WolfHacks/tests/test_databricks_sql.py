import asyncio
import json
import httpx
import pytest
from agent.databricks_sql import execute, DatabricksQueryError, table_prefix

def configure(monkeypatch):
    monkeypatch.setenv('DATABRICKS_HOST','https://example.cloud.databricks.com')
    monkeypatch.setenv('DATABRICKS_TOKEN','test-only-token')
    monkeypatch.setenv('DATABRICKS_WAREHOUSE_ID','test-warehouse')

def test_statement_result(monkeypatch):
    configure(monkeypatch)
    def handler(request):
        assert json.loads(request.content)['warehouse_id'] == 'test-warehouse'
        assert request.headers['Authorization'] == 'Bearer test-only-token'
        return httpx.Response(200,json={'status':{'state':'SUCCEEDED'},
            'manifest':{'schema':{'columns':[{'name':'layer'},{'name':'records'}]}},
            'result':{'data_array':[['silver','360']]}})
    assert asyncio.run(execute('SELECT 1',httpx.MockTransport(handler))) == [{'layer':'silver','records':'360'}]

def test_auth_failure_is_actionable(monkeypatch):
    configure(monkeypatch)
    with pytest.raises(DatabricksQueryError,match='Authentication'):
        asyncio.run(execute('SELECT 1',httpx.MockTransport(lambda r:httpx.Response(401))))

def test_failed_sql_is_not_success(monkeypatch):
    configure(monkeypatch)
    with pytest.raises(DatabricksQueryError,match='did not finish'):
        asyncio.run(execute('SELECT 1',httpx.MockTransport(lambda r:httpx.Response(200,json={'status':{'state':'FAILED'}}))))

def test_identifier_validation(monkeypatch):
    monkeypatch.setenv('DATABRICKS_SCHEMA','bad; DROP TABLE x')
    with pytest.raises(DatabricksQueryError): table_prefix()

def test_gold_order_numbers_and_bound_identity(monkeypatch):
    from agent import databricks_sql
    async def fake_execute(statement, **kwargs):
        assert ':user_id' in statement
        assert kwargs['parameters'][0]['value'] == "athlete'quoted"
        return [dict(session_id='s',window_start='later',window_end='end',avg_hr='140.2',min_hr='136',max_hr='144',notification_count='60'),
                dict(session_id='s',window_start='earlier',window_end='end',avg_hr='113',min_hr='110',max_hr='114',notification_count='39')]
    monkeypatch.setenv('DATABRICKS_CATALOG','workspace')
    monkeypatch.setenv('DATABRICKS_SCHEMA','pacepilot')
    monkeypatch.setattr(databricks_sql,'execute',fake_execute)
    rows = asyncio.run(databricks_sql.get_gold("athlete'quoted"))
    assert rows[0]['window_start'] == 'earlier'
    assert rows[1]['avg_hr'] == 140.2
    assert rows[0]['notification_count'] == 39
