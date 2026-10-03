"""Read-only SQL Statement Execution API adapter. Credentials stay server-side."""
import os
import re
import httpx

class DatabricksQueryError(RuntimeError):
    pass

def connection():
    host = os.getenv('DATABRICKS_HOST', '').rstrip('/')
    token = os.getenv('DATABRICKS_TOKEN', '')
    warehouse = os.getenv('DATABRICKS_WAREHOUSE_ID', '')
    if not host.startswith('https://') or not token or not warehouse:
        raise DatabricksQueryError('Set DATABRICKS_HOST, DATABRICKS_WAREHOUSE_ID, and DATABRICKS_TOKEN in your local .env')
    return host, token, warehouse

async def execute(statement: str, transport=None, parameters=None):
    host, token, warehouse = connection()
    async with httpx.AsyncClient(timeout=60, transport=transport) as client:
        response = await client.post(f'{host}/api/2.0/sql/statements',
            headers={'Authorization':f'Bearer {token}'},
            json={'warehouse_id':warehouse, 'statement':statement, 'wait_timeout':'50s',
                  'on_wait_timeout':'CANCEL', 'disposition':'INLINE', 'format':'JSON_ARRAY', 'row_limit':1000,
                  'parameters': parameters or []})
    if response.status_code in (401, 403):
        raise DatabricksQueryError('Authentication or permissions failed. Check the local token and warehouse/table access.')
    if not response.is_success:
        raise DatabricksQueryError(f'Databricks request failed (HTTP {response.status_code}).')
    body = response.json()
    if body.get('status', {}).get('state') != 'SUCCEEDED':
        raise DatabricksQueryError('SQL did not finish successfully. Check the warehouse is running and tables exist; inspect Databricks query history.')
    if body.get('manifest', {}).get('truncated') or body.get('result', {}).get('next_chunk_index') is not None:
        raise DatabricksQueryError('Result exceeds the supported single-chunk limit.')
    columns = [c['name'] for c in body['manifest']['schema']['columns']]
    return [dict(zip(columns, row)) for row in body.get('result', {}).get('data_array', [])]

def table_prefix():
    catalog = os.getenv('DATABRICKS_CATALOG', 'workspace')
    schema = os.getenv('DATABRICKS_SCHEMA', 'pacepilot')
    if any(not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', value) for value in (catalog,schema)):
        raise DatabricksQueryError('Catalog and schema must be simple SQL identifiers.')
    return f'`{catalog}`.`{schema}`'

async def get_counts():
    prefix = table_prefix()
    return await execute(' UNION ALL '.join(
        f"SELECT '{layer}' AS layer, COUNT(*) AS records FROM {prefix}.`{table}`"
        for layer,table in [('bronze','wearable_bronze'),('silver','wearable_silver'),('gold','wearable_gold_1m')]))

async def get_gold(user_id='demo-athlete'):
    table = f'{table_prefix()}.`wearable_gold_1m`'
    rows = await execute(f'''SELECT session_id,
        date_format(window_start, "yyyy-MM-dd'T'HH:mm:ss.SSSXXX") AS window_start,
        date_format(window_end, "yyyy-MM-dd'T'HH:mm:ss.SSSXXX") AS window_end,
        avg_hr, min_hr, max_hr, notification_count
        FROM {table}
        WHERE user_id = :user_id AND session_id = (
            SELECT session_id FROM {table} WHERE user_id = :user_id
            ORDER BY window_end DESC, session_id DESC LIMIT 1)
        ORDER BY window_start DESC LIMIT 120''',
        parameters=[{'name':'user_id','value':user_id,'type':'STRING'}])
    for row in rows:
        for key in ('avg_hr','min_hr','max_hr'):
            row[key] = float(row[key])
        row['notification_count'] = int(row['notification_count'])
    return list(reversed(rows))

async def get_silver_session(session_id: str, user_id='demo-athlete'):
    import json
    from shared.schemas import TelemetryEvent
    table = f'{table_prefix()}.`wearable_silver`'
    rows = await execute(f'''SELECT to_json(named_struct(
        'schema_version', schema_version, 'user_id', user_id, 'device_id', device_id,
        'session_id', session_id, 'sequence', sequence,
        'timestamp', date_format(timestamp, "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
        'heart_rate', heart_rate, 'rr_intervals_ms', rr_intervals_ms,
        'source', source, 'sensor_contact', sensor_contact)) AS event_json
        FROM {table} WHERE user_id = :user_id AND session_id = :session_id
        ORDER BY sequence DESC LIMIT 600''', parameters=[
            {'name':'user_id','value':user_id,'type':'STRING'},
            {'name':'session_id','value':session_id,'type':'STRING'}])
    return [TelemetryEvent.model_validate(json.loads(row['event_json'])).model_dump(mode='json')
            for row in reversed(rows)]
