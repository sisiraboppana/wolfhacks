"""Cloud session progress and the exact data slice used by the hosted agent."""
import asyncio
import hashlib
import json
from datetime import datetime, timezone
from agent.databricks_sql import execute, table_prefix, get_gold, get_silver_session

def snapshot_id(events):
    return hashlib.sha256(json.dumps(events,sort_keys=True,separators=(',',':')).encode()).hexdigest() if events else None

def scope(events):
    if not events or 'sequence' not in events[0]:
        return None
    return {'first_sequence':events[0]['sequence'], 'last_sequence':events[-1]['sequence'],
            'first_event_at':events[0]['timestamp'], 'last_event_at':events[-1]['timestamp'],
            'event_count':len(events)}

async def progress(session_id, user_id='demo-athlete'):
    prefix = table_prefix()
    params = [{'name':'user_id','value':user_id,'type':'STRING'},
              {'name':'session_id','value':session_id,'type':'STRING'}]
    rows = await execute(f"""
        SELECT 'bronze' AS layer, COUNT(*) AS records,
          date_format(MAX(ingested_at), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX") AS latest_ingested_at
        FROM {prefix}.wearable_bronze
        WHERE get_json_object(raw_json, '$.user_id') = :user_id
          AND get_json_object(raw_json, '$.session_id') = :session_id
        UNION ALL
        SELECT 'silver' AS layer, COUNT(*) AS records,
          date_format(MAX(ingested_at), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX") AS latest_ingested_at
        FROM {prefix}.wearable_silver
        WHERE user_id = :user_id AND session_id = :session_id
        UNION ALL
        SELECT 'gold' AS layer, COUNT(*) AS records, CAST(NULL AS STRING) AS latest_ingested_at
        FROM {prefix}.wearable_gold_1m
        WHERE user_id = :user_id AND session_id = :session_id
    """, parameters=params)
    counts = {row['layer']:int(row['records']) for row in rows}
    arrivals = {row['layer']:row.get('latest_ingested_at') for row in rows}
    return {'counts':counts, 'latest_silver_ingested_at':arrivals.get('silver'),
            'latest_bronze_ingested_at':arrivals.get('bronze')}

async def get_session(session_id):
    state, events, windows = await asyncio.gather(
        progress(session_id), get_silver_session(session_id), get_gold(session_id=session_id))
    sources = {event['source'] for event in events}
    data_kind = ('mixed' if len(sources) > 1 else
        {'mock':'synthetic','replay':'recorded_replay','polar_h10':'device'}.get(next(iter(sources),''),'unknown'))
    recordings = [e['recording'] for e in events if e.get('recording')]
    status = 'ready' if events else 'processing' if state['counts'].get('bronze',0) else 'waiting_for_upload'
    return {'source':'databricks_session', 'session_id':session_id, 'data_kind':data_kind,
            'context_complete': bool(events) and all(e.get('recording') for e in events),
            'datasets':sorted({r['dataset'] for r in recordings}),
            'hr_methods':sorted({r['hr_method'] for r in recordings}), 'status':status,
            'fetched_at':datetime.now(timezone.utc).isoformat(), **state,
            'events':events, 'windows':windows, 'analysis_scope':scope(events), 'snapshot_id':snapshot_id(events), 'analysis_limit':600}
