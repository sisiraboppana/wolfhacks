"""Build an optional cloud-only replay notebook; does not invoke any cloud APIs locally."""
import json
from pathlib import Path

cells=[]
def cell(kind,source):
    item={'cell_type':kind,'metadata':{},'source':source.splitlines(keepends=True)}
    if kind=='code':
        item.update(execution_count=None,outputs=[])
    cells.append(item)

cell('markdown','''# Replay a real recording from Databricks
Upload one prepared `events.jsonl` to the volume ROOT, outside `incoming`.
Keep the updated pipeline active, or run triggered updates separately. This notebook uploads paced batches; it does not start the pipeline or change endpoint configuration.
Default 1x speed, roughly ten minutes. 5x finishes in roughly two minutes while preserving physiological intervals.
This is recorded-data replay, not live sensor acquisition. Coordinate runs with teammates sharing the same pipeline.
''')
cell('code','%pip install "databricks-sdk>=0.57,<1"\n')
cell('code','''import io
import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4
from databricks.sdk import WorkspaceClient

SOURCE_PATH = '/Volumes/workspace/pacepilot/telemetry/events.jsonl'
DESTINATION = '/Volumes/workspace/pacepilot/telemetry/incoming'
SPEED = 1.0
BATCH_SECONDS = 20

if not 0 < SPEED <= 100:
    raise ValueError('SPEED must be positive and <=100')
events = [json.loads(line) for line in Path(SOURCE_PATH).read_text().splitlines() if line.strip()]
if not events or any(e.get('source') != 'replay' or not e.get('recording') for e in events):
    raise ValueError('Upload a prepared real recording, not mock events or a manifest')
if len({(e['user_id'],e['session_id'],e['device_id']) for e in events}) != 1:
    raise ValueError('One recording per replay')
times = [datetime.fromisoformat(e['timestamp'].replace('Z','+00:00')) for e in events]
if any(b <= a for a,b in zip(times,times[1:])):
    raise ValueError('Recording timestamps must strictly increase')

# Account for all already ingested raw event times, including rows Silver excluded.
latest = spark.sql("SELECT MAX(to_timestamp(get_json_object(raw_json, '$.timestamp'))) AS latest FROM workspace.pacepilot.wearable_bronze").first()['latest']
start = datetime.now(timezone.utc)
if latest is not None:
    latest = latest.replace(tzinfo=timezone.utc) if latest.tzinfo is None else latest
    start = max(start, latest+timedelta(seconds=1))
session = events[0]['recording']['dataset']+'-cloud-replay-'+uuid4().hex[:12]
offsets = [(t-times[0]).total_seconds() for t in times]
for event,offset in zip(events,offsets):
    event['timestamp'] = (start+timedelta(seconds=offset)).isoformat()
    event['session_id'] = session

batches=[]
for event,offset in zip(events,offsets):
    bucket = int(offset//BATCH_SECONDS)
    if not batches or batches[-1][0] != bucket:
        batches.append((bucket,[],offset))
    batches[-1][1].append(event)
    batches[-1] = (bucket,batches[-1][1],offset)

w=WorkspaceClient()
started=time.monotonic()
print('REPLAY SESSION:',session)
print('Virtual event clock:',start.isoformat(),'Speed:',SPEED)
for index,(_,batch,last_offset) in enumerate(batches):
    time.sleep(max(0,started+last_offset/SPEED-time.monotonic()))
    payload=('\\n'.join(json.dumps(e) for e in batch)+'\\n').encode()
    w.files.upload(f'{DESTINATION}/{session}-{index:06d}.json',io.BytesIO(payload),overwrite=False)
    print(f'Uploaded batch {index}: {len(batch)} real recorded events')
print('Uploads complete. Check Silver ingestion and select this session in the cloud dashboard.')
''')
notebook={'cells':cells,'metadata':{'kernelspec':{'display_name':'Python 3','language':'python','name':'python3'}},'nbformat':4,'nbformat_minor':5}
path=Path(__file__).resolve().parents[1]/'notebooks/replay_recording.ipynb'
path.write_text(json.dumps(notebook,indent=2)+'\n',encoding='utf-8')
print('Prepared',path)
