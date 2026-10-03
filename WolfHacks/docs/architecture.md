# Synthesis and decisions

Both PDFs were reviewed visually in full: Gemini 27 pages, ChatGPT 15 pages.
Several code blocks in the Gemini export are horizontally clipped in the source;
their hidden fragments cannot be recovered from this PDF. Implementations here
use explicit contracts and official documentation rather than copying those fragments.

## Agreed architecture

Polar H10 / mock producer -> backend gateway -> AWS Kinesis -> DLT Bronze ->
Silver notifications -> Gold window summaries in Unity Catalog -> full agent
on Databricks Model Serving -> gateway -> React.

The local development path is mock generator -> FastAPI -> SSE -> React,
with deterministic HRV analysis. It does not claim to run Kinesis or Databricks.
The producer can separately send the identical JSON contract to real Kinesis.
Cloud telemetry readback, authentication, and hosted-agent proxy remain future work.

## Major differences and resolved choices

| Topic | Gemini plan | ChatGPT plan | Resolution |
| --- | --- | --- | --- |
| Ingestion | Direct Kinesis | Files + Auto Loader | User explicitly selected Kinesis; direct source scaffold |
| Agent hosting | Full agent in Model Serving | FastAPI worker + served LLM | User selected full agent in Model Serving |
| Custom ML / RAG | Isolation Forest/autoencoder, Vector Search | Exclude custom training and vector database | User selected deferral |
| Wearable first | BLE integration | Replay initially, corrected to physical BLE on pp. 13–15 | Current user request overrides both: mock first; reusable BLE decoder included |
| Credentials | Browser AWS keys in one example | Server-only secrets | Server credential chain; no secrets in Vite |
| Drift | Broad physiological inference | Explicit proxy at steady workload | HR-only mock cannot establish drift; display unavailable |

No remaining major plan conflict blocks this scaffold. Deployment requires the
workspace host, AWS account/region, catalog permissions, supported pipeline
compute, serving entitlement, and sensor/browser details when hardware is added.

## Shared contract and processing

Version 1.0 retains Gemini's user_id, device_id, timestamp, heart_rate and
rr_intervals_ms, adding session_id, sequence, source, and nullable sensor_contact.
Timestamps are aware ISO 8601; UTC in processing. RR order is beat order within
each notification. Empty arrays are valid when no RR measurements arrive.
BLE RR ticks convert with ticks * 1000 / 1024; never treat ticks as milliseconds.
One-second mock notifications are not 1000 Hz HR acquisition. Speed, power,
cadence and SpO2 are not fabricated or attributed to the H10 heart-rate service.

Wire schemas preserve device values; Silver quality rules accept 30–230 BPM.
Bronze preserves invalid JSON for inspection. Silver deduplicates by
user/session/sequence within a two-minute watermark; old retries can fall outside
that guarantee. Sequence numbers must not restart within the same session.
Gold finalizes 1-minute tumbling HR windows after watermark progress. Stream
inactivity can delay finalization. Do not promise subsecond Gold or agent updates.

Local RMSSD and sample SDNN use the latest continuous segment of ordered RR
measurements. Missing packets and missing RR break that segment. No artifact
correction, validated recovery score, or resting 14-day baseline is claimed.
Cloud HRV needs ordered beat reconstruction; the pipeline avoids unordered
collect_list/flatten, which would compute incorrect successive differences.
Five-minute windows and daily contextual baselines are later extensions.

## Serving boundary

The full agent is packaged as an MLflow PythonModel with a custom tabular input:
`dataframe_records: [{prompt, telemetry_json}]`. Output predictions contain
`response_json`. This is not the foundation-model messages/choices contract.
`agent/model_serving.py` is an independent adapter for an existing chat-compatible
endpoint, usable by a future LLM inside the hosted agent, not by the browser.
The first serving model runs read-only deterministic HRV tools; it is clearly a
preview, not yet an LLM agent. UC tool queries, bounded LLM tool loops, durable
action IDs/cooldowns, and calendar/nutrition integrations are not implemented.

## Documentation checked

- [Kinesis connector](https://docs.databricks.com/aws/en/connect/streaming/kinesis/)
- [Kinesis authentication](https://docs.databricks.com/aws/en/connect/streaming/kinesis/authentication)
- [Polar BLE SDK](https://github.com/polarofficial/polar-ble-sdk)
- [Polar RR format](https://github.com/polarofficial/polar-ble-sdk/issues/343)
- [Custom Python serving](https://docs.databricks.com/aws/en/machine-learning/model-serving/deploy-custom-python-code)
- [Model Serving query contracts](https://docs.databricks.com/api/model-serving-query/v1)
