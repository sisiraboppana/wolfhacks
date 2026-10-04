# PacePilot

Initial wearable streaming platform, synthesized from `geminiPlan.pdf` and
`chatgptPlan.pdf`. Supports seeded mock telemetry and real HRV-ACC / PPG-DaLiA recorded-data replay.

For real recordings, deployment updates, and paced replay into the existing Databricks volume, follow [the real-data guide](docs/real-data.md). Adapters retain recorded RR or derive it from the authors' corrected ECG R-peaks; they never manufacture RR from averaged BPM.

## Layout

- `frontend/`: React, TypeScript, Recharts, runtime Zod validation, SSE dashboard
- `kinesis_producer/`: seeded Polar H10 mock notifications; stdout/file/API/Kinesis sinks
- `datasets/`: real-recording adapters, provenance, and paced API/volume replay
- `shared/`: Pydantic event schema, exported JSON schema, ordered HRV helpers
- `databricks_pipelines/`: DLT Bronze/Silver/Gold scaffold, Unity Catalog SQL, config template
- `agent/`: mock FastAPI gateway, Model Serving transport, full-agent packaging scaffold
- `infra/`: IAM and serving configuration templates
- `scripts/`: schema export and optional model registration
- `tests/`: contract, mock scenarios, and hand-worked metrics checks

## Run locally (PowerShell, Python 3.11+ and Node 20.19+)

```powershell
python -m venv .venv
.venv/Scripts/python.exe -m pip install -r requirements.txt
Copy-Item .env.example .env
npm install
.venv/Scripts/python.exe -m uvicorn agent.app:app --host 127.0.0.1 --port 8000
```

In another terminal run `npm run dev`, then open http://127.0.0.1:5173.
The gateway starts an automatic normal mock stream. Analysis is deterministic;
AWS and Databricks credentials are unnecessary. The Vite dev proxy forwards /api.
Production hosting needs its own same-origin /api proxy.

## Stream scenarios

For producer-controlled scenarios stop the backend, set `STREAM_MODE=external`
in `.env`, and restart it. Then:

```powershell
.venv/Scripts/python.exe -m kinesis_producer.producer --sink api --scenario rise --session-id rise-demo
.venv/Scripts/python.exe -m kinesis_producer.producer --sink stdout --scenario missing_rr --count 5 --interval 0
.venv/Scripts/python.exe -m kinesis_producer.producer --sink file --output data/mock.jsonl
```

Scenarios: normal, sustained HR rise, brief spike, missing RR. The UI displays
the latest session and calls analysis for that session. Use a new session ID
on every replay; resetting sequence numbers within a session is a duplicate.
Mock HR rise is not cardiovascular drift without speed/power context.

## Cloud scaffold

### Databricks Free Edition route (selected)

Use `/Volumes/workspace/pacepilot/telemetry/incoming` instead of AWS Kinesis.
In Windows Command Prompt, run:

```bat
.venv\Scripts\python.exe scripts\prepare_volume_demo.py
```

The script prints a unique local folder containing six JSON batches (360 mock
notifications with event timestamps spaced one second apart). Upload these files
into an `incoming` folder in the managed volume using Catalog Explorer.
Upload order is not event order; Silver uses timestamps and event identity.
Use `databricks_pipelines/pipeline.volume.example.json` for a triggered serverless
pipeline targeting `workspace.pacepilot`. This setup replaces real-time Kinesis
with incremental file ingestion; cloud execution remains to be verified.

### Read Databricks Gold results locally

Set DATABRICKS_HOST, DATABRICKS_WAREHOUSE_ID, DATABRICKS_TOKEN,
DATABRICKS_CATALOG=workspace and DATABRICKS_SCHEMA=pacepilot in your local `.env`.
Keep the token server-side. Restart the backend after configuration changes.
`/api/databricks/check` verifies table counts; `/api/databricks/gold` returns the
latest finalized session windows for the demo athlete. In React, click **Load
cloud results** in the Databricks panel. Refresh is manual to conserve warehouse
usage. These are processed synthetic file batches, separate from the local
live mock stream and its deterministic assistant.

Set `DATABRICKS_AGENT_ENDPOINT=pacepilot-agent` after deploying registered model
version 1. Restart the backend, load cloud results, and click **Analyze with
hosted agent**. The backend reads up to 600 most recent Silver notifications
for the selected demo session, validates them, and invokes the custom model
using `dataframe_records`. The panel shows hosted HRV and tool execution. A
SQL-only token may need the Model Serving API scope and endpoint CAN QUERY
permission. The first request after scale-to-zero may take longer. This remains
deterministic analysis; hosted LLM reasoning is not yet implemented.

### Register the agent preview in Free Edition

Run `scripts/prepare_serving_notebook.py` to regenerate the upload assets.
Upload `output/serving/pacepilot-agent-code.zip` to the telemetry volume root,
outside `incoming`. Import `notebooks/register_agent.ipynb` into Workspace,
attach serverless compute, and run the cells in order. The notebook packages,
loads, smoke-tests, and registers `workspace.pacepilot.wearable_agent`, printing
the registered version. Cloud execution and serving must be verified in your
workspace. This is deterministic tool execution; hosted LLM reasoning is next.

### Optional AWS route

1. Configure AWS SDK credentials in your environment or an IAM role. Render
   `infra/kinesis-iam.example.json` with your account and stream ARN. Provision
   the stream separately; no resources are created automatically.
2. Set AWS_REGION and KINESIS_STREAM_NAME, then run the producer with
   `--sink kinesis`. This writes real AWS records and may incur charges.
3. Confirm Databricks compute supports the Kinesis connector and selected IAM
   authentication. Upload the pipeline file; replace config placeholders and
   grant the pipeline principal access to the catalog/schema. Validate in the
   workspace before running. This template targets classic AWS pipeline compute.
4. Create catalog/schema, start the pipeline, then register the UC SQL function.
   Inspect Bronze, Silver and Gold; Gold windows require event-time watermark progress.
5. To package the full-agent preview, install `agent/requirements-serving.txt`
   in configured Databricks development compute and run `scripts/register_agent.py`
   from the repo root. Register in your catalog and use the exact registered
   version in `infra/serving-endpoint.example.json` to create an endpoint manually.
   The custom request contract is described in `docs/architecture.md`.

Cloud deployment, hosted inference, and telemetry readback have not been verified.
FastAPI currently provides local development behavior; production user/session
authentication must be added before exposing uploads or personalized data.
Custom anomaly ML and Vector Search are deferred by decision. LLM tool reasoning
and external actions remain next-stage work within the hosted agent.

## Validation

```powershell
.venv/Scripts/python.exe -m pytest -q
npm run build
.venv/Scripts/python.exe scripts/export_schema.py
```

See `docs/architecture.md` for both-plan differences, resolved choices, corrected
examples, processing limits, and official references.
