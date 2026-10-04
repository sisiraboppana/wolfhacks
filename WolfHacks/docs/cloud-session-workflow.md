# Cloud results and hosted analysis

The dashboard has one recording workflow:

1. **Incoming recording** shows the events arriving at the local FastAPI gateway. This does not prove an upload succeeded.
2. **Cloud session** reads the selected session from Databricks. Bronze counts raw events ingested, Silver counts saved validated events, and Gold counts finalized minute windows. Real recordings, dataset names, and older synthetic sessions are labeled explicitly.
3. **Session insights** sends the displayed Silver slice (up to 600 events) to the existing hosted agent. The request includes a sequence cutoff and a fingerprint of the displayed events. Later events are excluded, and any late-arriving or changed data within the slice causes a refresh request instead of silently substituting a different recording. The response records the analyzed sequence/time range.

Connect once. The cloud panel follows the incoming replay by default; choose another recording to inspect an older session. It refreshes at most once per minute while idle and the tab is visible, with a longer interval after failures. Refresh does not call the LLM. An existing explanation remains visible for that session and is marked older when new events arrive. The local deterministic preview is collapsed to distinguish it from hosted reasoning.

The saved HR preview is available as soon as Silver has data, even without Gold windows. Minute summaries are optional details. Gold's two-minute event-time watermark means the final windows of a finite replay can remain pending; they are not evidence that analysis is unimplemented.

Missing recording/activity context is shown explicitly and activity-aware analysis is disabled until the pipeline is updated. Upload/ingestion/validation errors are not presented as successful cloud processing.

## Verified workspace state on October 4, 2026

A read-only cloud check found only the original synthetic session in Silver: 360 Bronze rows, 360 Silver rows, and 4 Gold windows. The new real recording had not reached Silver. The new session snapshot endpoint was verified against those actual tables.

The local token failed the Files API with HTTP 403: it lacks the `files` scope. SQL access works. Create/update a local token with files, SQL, and serving access and put it in the Git checkout's `.env`. The secret used by DeepSeek does not need to change for volume upload access.

Replay now checks volume access and updated local backend compatibility before publishing events for `--sink both`, so this problem is reported before the chart starts.

## Resume

From `C:\Users\punee\git\wolfhacks\WolfHacks`, stop the old backend and restart:

```bat
.venv\Scripts\python.exe -m uvicorn agent.app:app --host 127.0.0.1 --port 8000
```

Restart the frontend if needed:

```bat
npm run dev
```

Keep the pipeline running, then start a real recording:

```bat
.venv\Scripts\python.exe -m datasets.replay --input data/real/ppg-dalia-transitions/events.jsonl --sink both --speed 1
```

Select **Connect to Databricks**. The incoming replay's session ID will be selected automatically. Expect progress from raw ingestion to validated data, then click **Analyze this saved slice**. Existing model version 3 is compatible; these dashboard/gateway changes require no model registration or endpoint redeployment.

Validation commands:

```bat
.venv\Scripts\python.exe -m pytest -q -p no:cacheprovider
npm run test:cloud
npm run build
```
