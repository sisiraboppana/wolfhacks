"""Paced real-recording replay to local SSE and/or Databricks UC volume micro-batches."""
import argparse
import json
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4
import httpx
from dotenv import load_dotenv
from shared.schemas import TelemetryEvent

def batches(events, seconds):
    """Use recording time, never number of rows, for variable beat cadence."""
    first = events[0].timestamp
    batch,bucket = [],0
    for event in events:
        current = int((event.timestamp-first).total_seconds()//seconds)
        if batch and current != bucket:
            yield batch
            batch = []
        batch.append(event)
        bucket = current
    if batch:
        yield batch

def upload(client, host, token, volume_path, name, payload):
    response = client.put(f'{host}/api/2.0/fs/files{volume_path}/{name}',
        headers={'Authorization':f'Bearer {token}','Content-Type':'application/octet-stream'},content=payload)
    if response.status_code in (401,403):
        raise RuntimeError('Volume upload denied: local token needs files scope and USE CATALOG, USE SCHEMA, WRITE VOLUME.')
    if not response.is_success:
        raise RuntimeError(f'Volume upload failed (HTTP {response.status_code}); local batch is retained for retry.')

def preflight(client, host, token, volume_path):
    response = client.get(f'{host}/api/2.0/fs/directories{volume_path}',
        headers={'Authorization':f'Bearer {token}'})
    if response.status_code in (401,403):
        raise RuntimeError('Databricks volume access denied. The LOCAL .env token needs the files scope, plus USE CATALOG, USE SCHEMA and volume access. Keep SQL and serving scopes. Replay has not started.')
    if not response.is_success:
        raise RuntimeError(f'Cannot access incoming volume (HTTP {response.status_code}). Replay has not started.')

def main():
    load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',required=True)
    parser.add_argument('--sink',choices=['file','api','volume','both'],default='file')
    parser.add_argument('--speed',type=float,default=1)
    parser.add_argument('--batch-seconds',type=float,default=20)
    parser.add_argument('--url',default='http://127.0.0.1:8000/api/telemetry')
    parser.add_argument('--volume-path',default='/Volumes/workspace/pacepilot/telemetry/incoming')
    parser.add_argument('--no-wait',action='store_true',help='Export files immediately; cannot be combined with remote sinks')
    parser.add_argument('--start-time',help='Explicit ISO replay clock with timezone; must be newer than existing pipeline watermark')
    args = parser.parse_args()
    if not 0 < args.speed <= 100 or not 1 <= args.batch_seconds <= 120:
        parser.error('speed must be 0..100 (exclusive zero), batch-seconds 1..120')
    if args.no_wait and args.sink != 'file':
        parser.error('--no-wait is only for local file export')
    # Entire recordings may exceed the serving limit of 600; validate chunks and global identity/order here.
    events = [TelemetryEvent.model_validate(json.loads(line)) for line in Path(args.input).read_text(encoding='utf-8').splitlines() if line.strip()]
    if not events:
        parser.error('Recording has no events')
    if len({(e.user_id,e.session_id,e.device_id) for e in events}) != 1 or any(
        b.sequence != a.sequence+1 or b.timestamp <= a.timestamp for a,b in zip(events,events[1:])):
        parser.error('One ordered session with consecutive sequences is required')
    host,token = os.getenv('DATABRICKS_HOST','').rstrip('/'),os.getenv('DATABRICKS_TOKEN','')
    if args.sink in ('volume','both'):
        if not host.startswith('https://') or not token:
            parser.error('Set DATABRICKS_HOST and DATABRICKS_TOKEN in .env')
        if args.volume_path != '/Volumes/workspace/pacepilot/telemetry/incoming':
            parser.error('Use the current pipeline incoming volume path')
    with httpx.Client(timeout=30) as client:
        if args.sink in ('volume','both'):
            preflight(client,host,token,args.volume_path)
        if args.sink in ('api','both'):
            health = client.get(args.url.rsplit('/telemetry',1)[0]+'/health')
            if not health.is_success:
                raise RuntimeError('Local backend is unavailable. Start it from the Git checkout before replay.')
            state = health.json()
            if state.get('telemetry_mode') != 'external' or not state.get('recording_context_supported'):
                raise RuntimeError('Restart the updated backend from the Git checkout with STREAM_MODE=external. Replay has not started.')
    run_id = uuid4().hex[:12]
    session = f'{events[0].recording.dataset if events[0].recording else "replay"}-replay-{run_id}'
    root = Path('data/replay_runs')/session
    root.mkdir(parents=True)
    original_start = events[0].timestamp
    duration = (events[-1].timestamp-original_start).total_seconds()
    # Reserve a monotonic virtual event clock across local replay runs. Accelerating arrival
    # never compresses physiological intervals, and replaying another subject cannot start
    # behind the preceding run's watermark. This clock can be ahead of wall-clock time.
    clock_path = root.parent/'replay_clock.json'
    lock_path = root.parent/'replay_clock.lock'
    try:
        lock = lock_path.open('x')
    except FileExistsError:
        raise RuntimeError('Another replay is reserving the clock. Retry; if a crashed process left the lock, remove data/replay_runs/replay_clock.lock.')
    try:
        replay_start = datetime.fromisoformat(args.start_time.replace('Z','+00:00')) if args.start_time else datetime.now(timezone.utc)
        if replay_start.tzinfo is None:
            parser.error('start-time must include a timezone')
        if clock_path.exists():
            reserved_end = datetime.fromisoformat(json.loads(clock_path.read_text())['reserved_end'])
            if args.start_time and replay_start <= reserved_end:
                parser.error('start-time must be after the previously reserved replay end')
            replay_start = max(replay_start,reserved_end+timedelta(seconds=1))
        clock_path.write_text(json.dumps({'reserved_end':(replay_start+timedelta(seconds=duration)).isoformat()}))
    finally:
        lock.close(); lock_path.unlink()
    events = [e.model_copy(update={'session_id':session,'timestamp':replay_start+(e.timestamp-original_start)}) for e in events]
    started = time.monotonic()
    print(f'REPLAY SESSION: {session}; {len(events)} events; {args.speed}x; outputs {root.resolve()}',flush=True)
    print(f'Virtual event clock starts {replay_start.isoformat()}; physiological timing preserved; arrival speed changes only.',flush=True)
    pending,index = [],0
    with httpx.Client(timeout=45) as client:
        def flush():
            nonlocal index
            path = root/f'{session}-{index:06d}.json'
            payload = ('\n'.join(e.model_dump_json(exclude_none=True) for e in pending)+'\n').encode()
            path.write_bytes(payload)
            if args.sink in ('volume','both'):
                upload(client,host,token,args.volume_path,path.name,payload)
            print(f'Batch {index}: {len(pending)} recorded events'+(' uploaded' if args.sink in ('volume','both') else ' saved'),flush=True)
            pending.clear(); index += 1
        bucket = 0
        for event in events:
            elapsed = (event.timestamp-replay_start).total_seconds()
            if not args.no_wait:
                time.sleep(max(0,started+elapsed/args.speed-time.monotonic()))
            if args.sink in ('api','both'):
                response = client.post(args.url,json=event.model_dump(mode='json',exclude_none=True))
                if not response.is_success:
                    raise RuntimeError(f'Local ingestion failed (HTTP {response.status_code}); set STREAM_MODE=external and restart backend')
            current = int(elapsed//args.batch_seconds)
            if pending and current != bucket:
                flush()
            bucket = current; pending.append(event)
        if pending:
            flush()
    print('Replay completed. Volume upload is not confirmation of pipeline ingestion; check Silver and Gold.',flush=True)

if __name__ == '__main__':
    main()
