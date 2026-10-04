# Real recordings and paced replay

Both adapters keep the existing React → FastAPI → Databricks volume → Auto Loader → Bronze/Silver/Gold → hosted-agent workflow. The current AWS-free setup stays in place. Measurements are real recorded data; replay arrival is paced locally. This is not a live physiological feed.

## Sources and attribution

* **HRV-ACC v1**, Kamil Książek et al., DOI [10.5281/zenodo.8171266](https://zenodo.org/records/8171266), CC BY 4.0 (verified from the Zenodo API). The official RR archive's MD5 is checked before extraction. Start with `control_2.csv`. It contains recorded Polar H10 RR milliseconds and phone time-of-day. There is no date/timezone or independent measured BPM field; the adapter preserves relative phone times, handles midnight rollover, assigns a replay date and derives integer BPM as rounded `60000/RR`. Each row is a beat event; the adapter does not claim to reproduce original BLE packet grouping. Contact status is unknown. This is a clinical research dataset with rest/walking instructions, not a labeled workout benchmark. No participant diagnoses or PANSS scores are ingested.
* **PPG-DaLiA**, Attila Reiss, Ina Indlekofer and Philip Schmidt, DOI [10.24432/C53890](https://archive.ics.uci.edu/dataset/495/ppg+dalia). UCI currently lists CC BY 4.0; this differs from the non-commercial restriction in the supplied proposal. Cite the dataset and Reiss et al., *Deep PPG: Large-Scale Heart Rate Estimation with Convolutional Neural Networks*, Sensors 2019. The authors' [readme](https://archive.ics.uci.edu/ml/machine-learning-databases/00495/readme.pdf) documents manually corrected R-peak indices at 700 Hz, ECG-derived HR windows (8-second width / 2-second shift), and activity annotations at 4 Hz. Default adapter derives RR from adjacent corrected R-peaks. The optional `--dalia-mode hr-windows` uses provided averaged BPM, emits at window end, and leaves RR empty. An overlapping HR window crossing multiple activity labels is marked transition rather than assigning a misleading single activity.

PPG-DaLiA is recorded with RespiBAN and Empatica E4, not a Polar H10. Provenance preserves that difference. The first runnable demo uses **S10**, which is the first actual subject file in the official ZIP's entry order. UCI's direct download did not support byte ranges during our check, so the fetcher streams ZIP entries and stops after that subject. It verifies its ZIP CRC and uncompressed length rather than downloading all 2.7 GB. The legacy pickle is opened only through a restricted NumPy unpickler; the resulting small `.npz` is preferred for subsequent conversions. Only use official source files. The adapter does not consume questionnaires or the raw ECG/PPG arrays.

## Prepared recordings

The initial implementation has downloaded and prepared:

| Input | Slice | Events / RR values | Context |
| --- | --- | --- | --- |
| `data/real/hrv-acc-first/events.jsonl` | First 600 seconds of control 2 | 1135 / 1135 | Polar H10 RR; activity unlabeled |
| `data/real/ppg-dalia-transitions/events.jsonl` | S10, seconds 1000–1600 | 946 / 946 | Sitting → transition → stairs |

There is also a first-ten-minutes S10 slice in `data/real/ppg-dalia-first`. Each prepared directory contains a source SHA-256, citation, transformation method and selected interval in `manifest.json`. These manifests stay outside the ingestion folder.

To reproduce from the project root in **Windows Command Prompt**:

```bat
.venv\Scripts\python.exe -m pip install -r requirements-data.txt
.venv\Scripts\python.exe scripts\fetch_real_data.py --dataset hrv_acc
.venv\Scripts\python.exe scripts\fetch_real_data.py --dataset ppg_dalia --subject S10
.venv\Scripts\python.exe -m datasets.prepare --dataset hrv_acc --input data/raw/hrv_acc/control_2.csv --duration 600
.venv\Scripts\python.exe -m datasets.prepare --dataset ppg_dalia --input data/raw/ppg_dalia/S10.npz --subject S10 --start-seconds 1000 --duration 600
```

If converting for the first time, use `S10.pkl`; conversion writes the `.npz`. The commands print newly generated input paths. Preparation never overwrites an existing named recording directory.

## Update cloud code before hosted analysis

The canonical heart-rate/RR fields remain unchanged. An optional `recording` context adds dataset, subject, derivation method, original offset and activity label. Existing mock notifications remain valid.

1. Replace the Databricks pipeline source with the updated `databricks_pipelines/pipeline.py`. Keep your catalog/schema, incoming path and existing table names. Run a normal update; do not reset or full-refresh the tables just to add the nullable context column.
2. Upload `output/serving/pacepilot-agent-v3-code.zip` to the **volume root**, outside `incoming`. Import/run `notebooks/register_agent_v3.ipynb`. Its offline packaging check still uses a tiny synthetic contract fixture; its final live LLM test uses **real PPG-DaLiA S10 data embedded in the ZIP**.
3. Update `pacepilot-agent` to the printed registered version. This may be version 3 or higher after repeated registrations. Preserve the working `DATABRICKS_HOST`, secret-backed `DATABRICKS_TOKEN`, and `PACEPILOT_LLM_MODEL` environment variables. Wait for Ready. Version 2 does not accept the new recording context.
4. Restart the local backend/frontend with the updated source.

The hosted model still calls the same three tools. The trend tool now supplies activity annotations and dataset/HR-method evidence. There is no new activity classifier, validated quality score, or custom anomaly model. RR plausibility filters flag values without diagnosing ectopic beats, and raw measurements remain preserved in Bronze. Existing Silver HR bounds can exclude artifact-derived BPM outside 30–230; excluded events then appear as sequence gaps in the supplied slice. No interpolation or silent replacement with synthetic values is performed.

## Replay into the existing volume

In `.env`, set `STREAM_MODE=external` and restart the backend. For automatic uploads, your **local token** needs the `files` scope, plus `USE CATALOG`, `USE SCHEMA`, and `WRITE VOLUME` on `workspace.pacepilot.telemetry`. Keep its existing SQL and serving scopes. The secret used by DeepSeek does not need to change for volume uploads.

Run one recording at a time in a third terminal:

```bat
.venv\Scripts\python.exe -m datasets.replay --input data/real/ppg-dalia-transitions/events.jsonl --sink both --speed 1
```

`both` sends each event to the local FastAPI stream and uploads immutable JSON batches to `/Volumes/workspace/pacepilot/telemetry/incoming` every approximately 20 seconds of recording time. `--sink volume` uploads without requiring the local backend. `--sink api` only drives the local chart. `--speed 5` accelerates arrivals while preserving the original event-time and RR intervals; a 600-second recording plays in about 120 seconds.

Each run gets a fresh session ID and saves batches under `data/replay_runs/<session>`. Failed uploads leave the batch locally and do not report a successful cloud completion. Files are not overwritten and old sessions are not deleted. Local receipt does not prove cloud ingestion.

**Event clock:** replay timestamps are mapped to a virtual UTC clock. A local reservation file ensures subsequent replays start after prior replay end times, preventing our sequential sessions from starting behind the pipeline's global watermark. Accelerated replay can put this virtual clock ahead of wall time. It never squeezes physiological seconds into shorter event intervals. If other teammates have advanced this same pipeline clock, use `--start-time <ISO-UTC-time>` newer than their latest event time, or coordinate replay runs. Do not feed historical-date backfills into the watermarked demo pipeline expecting all rows to survive. Raw data remains in Bronze, but late rows/windows may be excluded downstream.

For manual upload, export instantly:

```bat
.venv\Scripts\python.exe -m datasets.replay --input data/real/hrv-acc-first/events.jsonl --sink file --no-wait
```

Upload only its printed batch `.json` files to `incoming`. Do not upload manifests, ZIPs, pickles, PDFs or raw CSVs there.

## Processing and display cadence

Auto Loader incrementally processes new files in Unity Catalog volumes; see [official documentation](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/). Uploading a file does not automatically start a stopped pipeline.

For a short demo, enable **Continuous** pipeline mode if your Free Edition workspace exposes/supports it, then stop it after the replay to conserve quota. Otherwise keep Triggered mode and run updates periodically (manually or through a scheduled pipeline job). Triggered runs ingest files available during that update; they are not a permanent listener. Do not launch one update per beat or per uploaded file.

The local chart follows arrivals immediately. The cloud panel lists sessions from Silver and can refresh while idle every 60 seconds. It supports hosted analysis before Gold has finalized windows. Gold has a two-minute event-time watermark: the last roughly two minutes of a finite recording may not finalize until genuinely later event-time data arrives. Do not add fabricated tail beats just to flush windows. Cloud analysis uses only the **latest 600 Silver events**, not an entire 1–2-hour recording; the agent is instructed to describe that observed slice.

## Further extensions

* Add HRV-ACC accelerometer data as a separate high-rate Bronze/Silver stream, joined by recorded offset. Compute motion magnitude features without calling them speed or power. Keep raw samples out of the existing low-rate notification envelope.
* Add PPG-DaLiA wrist PPG/ACC only for measured artifact-stress evaluation; it should not replace chest ECG-derived reference RR.
* A Databricks notebook can replay the compact prepared recording directly into the same volume if you want the demo to run without your laptop. It still needs active pipeline processing and consumes Free Edition quota.
  `notebooks/replay_recording.ipynb` implements this option: upload one prepared `events.jsonl` to the volume root and import/run the notebook. The local live chart is separate; this option updates the cloud panel after pipeline ingestion.
* A future BLE device can replace the replay producer while keeping the envelope, volume ingestion, tables and dashboard. No new AWS account is needed for this route.
