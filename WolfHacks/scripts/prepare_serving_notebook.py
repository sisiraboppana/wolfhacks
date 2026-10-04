"""Build a small upload bundle and Databricks registration notebook."""
import json
import argparse
from pathlib import Path
from zipfile import ZipFile

root = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser()
parser.add_argument('--generation', choices=['2','3'], default='3')
generation = parser.parse_args().generation
output = root / 'output' / 'serving'
output.mkdir(parents=True, exist_ok=True)
with ZipFile(output / f'pacepilot-agent-v{generation}-code.zip', 'w') as archive:
    for name in ['agent/__init__.py', 'agent/serving_model.py', 'agent/session_tools.py', 'agent/llm_agent.py', 'shared/__init__.py', 'shared/schemas.py', 'shared/metrics.py']:
        archive.write(root / name, name)
    if generation == '3':
        rows = (root/'data/real/ppg-dalia-transitions/events.jsonl').read_text(encoding='utf-8').splitlines()
        archive.writestr('real_sample.json',json.dumps([json.loads(row) for row in rows[:600]]))

cells = []
def cell(kind, source):
    item = {'cell_type':kind,'metadata':{},'source':source.splitlines(keepends=True)}
    if kind == 'code': item.update(execution_count=None, outputs=[])
    cells.append(item)

cell('markdown', '''# Register PacePilot tool-calling agent v2
DeepSeek chooses read-only tools for HR trends, sustained changes, and RR quality, then answers your question.
Upload `pacepilot-agent-v2-code.zip` to the ROOT of `/Volumes/workspace/pacepilot/telemetry` before running.
The archive must not be in `incoming`, which is reserved for telemetry JSON.
This notebook registers a model. It does not create a serving endpoint.
''')
cell('code', '%pip install "mlflow[databricks]>=3,<4" "pydantic>=2.10,<3" "databricks-sdk>=0.57,<1" "httpx>=0.28,<1"\n')
cell('code', 'dbutils.library.restartPython()\n')
cell('code', '''import sys
import tempfile
import zipfile
from pathlib import Path

bundle = Path('/Volumes/workspace/pacepilot/telemetry/pacepilot-agent-v2-code.zip')
if not bundle.exists():
    raise FileNotFoundError('Upload pacepilot-agent-v2-code.zip to the telemetry volume root first')
code_root = Path(tempfile.mkdtemp(prefix='pacepilot-agent-'))
allowed = {'agent/__init__.py','agent/serving_model.py','agent/session_tools.py','agent/llm_agent.py','shared/__init__.py','shared/schemas.py','shared/metrics.py'}
if bundle.name == 'pacepilot-agent-v3-code.zip':
    allowed.add('real_sample.json')
with zipfile.ZipFile(bundle) as archive:
    if set(archive.namelist()) != allowed:
        raise ValueError('Unexpected archive contents; use the generated project bundle')
    archive.extractall(code_root)
sys.path.insert(0, str(code_root))
from agent.serving_model import WearableAgentModel
print('Agent code loaded')
''')
cell('code', '''import json
import os
import pandas as pd
import mlflow
from mlflow.models import infer_signature

model_name = 'workspace.pacepilot.wearable_agent'
mlflow.set_registry_uri('databricks-uc')
user = spark.sql('SELECT current_user() AS username').first()['username']
mlflow.set_experiment(f'/Users/{user}/pacepilot-agent')

events = [
    {'schema_version':'1.0','user_id':'demo-athlete','device_id':'mock-polar-h10',
     'session_id':'serving-smoke-test','sequence':i,'timestamp':f'2026-10-03T12:00:0{i}Z',
     'heart_rate':75,'rr_intervals_ms':rr,'source':'mock','sensor_contact':True}
    for i,rr in enumerate([[800,810],[790,805]])
]
example = pd.DataFrame([{'prompt':'Summarize my session','telemetry_json':json.dumps(events)}])
model = WearableAgentModel()
os.environ['PACEPILOT_PACKAGING_OFFLINE'] = '1'
expected = model.predict(None, example)
assert json.loads(expected[0]['response_json'])['metrics']['rr_count'] == 4
with mlflow.start_run():
    info = mlflow.pyfunc.log_model(
        name='agent', python_model=model,
        code_paths=[str(code_root / 'agent'), str(code_root / 'shared')],
        input_example=example, signature=infer_signature(example, expected),
        pip_requirements=[f'mlflow=={mlflow.__version__}', 'pydantic>=2.10,<3', f'pandas=={pd.__version__}', 'databricks-sdk>=0.57,<1', 'httpx>=0.28,<1'])
    loaded = mlflow.pyfunc.load_model(info.model_uri)
    prediction = loaded.predict(example)
    assert json.loads(prediction[0]['response_json'])['metrics']['rr_count'] == 4
    version = mlflow.register_model(info.model_uri, model_name)
print(f'REGISTERED MODEL: {model_name}')
print(f'REGISTERED VERSION: {version.version}')
os.environ.pop('PACEPILOT_PACKAGING_OFFLINE', None)
print('Offline packaging check passed. This does not verify LLM access.')
''')
cell('code', '''# Live test uses notebook identity; no token is embedded in the registered model.
from datetime import datetime, timedelta, timezone
os.environ.pop('PACEPILOT_PACKAGING_OFFLINE', None)
start = datetime(2026,10,3,12,0,tzinfo=timezone.utc)
events = [dict(events[0], sequence=i, timestamp=(start+timedelta(seconds=i)).isoformat(),
               heart_rate=110 if i < 60 else 140, rr_intervals_ms=[545,550] if i < 60 else [425,435])
          for i in range(180)]
if (code_root/'real_sample.json').exists():
    events = json.loads((code_root/'real_sample.json').read_text())
    print('Live test uses real PPG-DaLiA S10 recording, with dataset-provided activity annotations.')
example = pd.DataFrame([{'prompt':'Did heart rate briefly spike or stay elevated? What should I check next?',
                         'telemetry_json':json.dumps(events)}])
result = json.loads(model.predict(None, example)[0]['response_json'])
print(json.dumps(result, indent=2))
if result['mode'] != 'llm_agent':
    print('LLM ACCESS NOT VERIFIED: use the error field to resolve authentication, permissions or quota.')
''')
cell('markdown', '''Registration preserves the existing endpoint until you edit it.
After a successful live test, update `pacepilot-agent` to the printed model version.
For hosted credentials, follow `docs/llm-agent-v2.md`: the notebook identity is not automatically the serving identity.
Set `PACEPILOT_LLM_MODEL=system.ai.deepseek-v4-flash-0731` and secret-backed Databricks credentials.
Never set PACEPILOT_PACKAGING_OFFLINE on the endpoint.
Request format stays `{"dataframe_records":[{"prompt":"Summarize my session","telemetry_json":"[...]"}]}`.
''')
notebook = {'cells':cells,'metadata':{'kernelspec':{'display_name':'Python 3','language':'python','name':'python3'}},'nbformat':4,'nbformat_minor':5}
serialized = json.dumps(notebook,indent=2).replace('agent-v2','agent-v'+generation).replace('agent v2','agent v'+generation)
(root / 'notebooks' / f'register_agent_v{generation}.ipynb').write_text(serialized+'\n',encoding='utf-8')
print(f'Prepared notebooks/register_agent_v{generation}.ipynb and output/serving/pacepilot-agent-v{generation}-code.zip')
