# Deploy the tool-calling agent

The LLM runs inside the custom Databricks model. It requests three read-only tools, receives their computed results, and returns an explanation, next step, and context question. Maximum four LLM requests per analysis. No vector search or custom anomaly model is involved.

## Register and test

1. Upload `output/serving/pacepilot-agent-v2-code.zip` to `/Volumes/workspace/pacepilot/telemetry/`, outside `incoming`.
2. Import `notebooks/register_agent_v2.ipynb` into Databricks and run it with serverless compute.
3. Record the printed registered version of `workspace.pacepilot.wearable_agent`. Do not assume it is version 2 if registration has been repeated.
4. The final cell tests DeepSeek with a synthetic 180-second recording. Look for `mode: llm_agent` and three tool traces with `requested_by: llm`. `llm_unavailable` means deterministic fallback; the safe error field identifies the failure category. Registration alone does not prove LLM access.

## Configure hosted credentials

Notebook identity and serving identity differ. Configure credentials for the model to call the Unity Gateway. Use a PAT with access to the selected model service and the gateway inference API. Gateway access uses the `ai-gateway` scope; SQL and serving endpoint scopes alone may be insufficient. Keep tokens out of notebook source, React, model artifacts, screenshots, and chat.

Run this cell separately in a Databricks notebook to store a token using a masked prompt. The token itself is never printed. If the notebook UI cannot accept a masked prompt, create the secret with the Databricks CLI instead; do not paste a token into source code.

```python
from getpass import getpass
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import ResourceAlreadyExists

w = WorkspaceClient()
try:
    w.secrets.create_scope(scope='pacepilot')
except ResourceAlreadyExists:
    pass
token = getpass('Token for Unity Gateway inference: ')
if not token.strip():
    raise ValueError('A token is required')
try:
    w.secrets.put_secret(scope='pacepilot', key='llm-token', string_value=token)
finally:
    del token
print('Secret stored; value not displayed')
```

In **Serving → pacepilot-agent → Edit**, select the newly registered version and configure these environment variables on its served model:

| Variable | Value |
| --- | --- |
| `DATABRICKS_HOST` | `https://dbc-ea461be4-2b21.cloud.databricks.com` |
| `DATABRICKS_TOKEN` | `{{secrets/pacepilot/llm-token}}` |
| `PACEPILOT_LLM_MODEL` | `system.ai.deepseek-v4-flash-0731` |

The endpoint creator needs READ access to the secret. Never set `PACEPILOT_PACKAGING_OFFLINE` on the endpoint. If secret/environment controls are unavailable in your Free Edition workspace, stop at the tested registered model and report the missing control rather than embedding a token in the model.

Save, wait for Ready, and query it using the existing request format. Confirm `response_json` contains `mode: llm_agent`, `next_step`, `question`, and three completed LLM-requested tools. A 401/403 usually needs credential/scope/model-service access checks; a 429 needs quota/rate-limit checks. The existing version remains a rollback option.

## Run the updated dashboard

Restart the backend and frontend after updating the endpoint. In Windows Command Prompt, from the project root:

```bat
.venv\Scripts\python.exe -m uvicorn agent.app:app --host 127.0.0.1 --port 8000
```

In a second terminal, use the project's usual `npm run dev` command. Select the cloud session, enter a question in the hosted analysis panel, and analyze it. The frontend displays the mode, explanation, next step, follow-up question, tool traces, and expandable evidence. Tokens stay on the server.

The tools compare the first and last recorded minutes, detect demo-rule sustained increases, and compute HRV over the latest continuous usable RR segment. These describe synthetic telemetry and cannot establish the physiological cause of a change. Historical baselines and workload data remain unavailable.

References: [query model services](https://docs.databricks.com/aws/en/ai-gateway/query-model-services), [Unity Gateway scopes](https://developers.databricks.com/docs/unity-gateway/overview), [secret-backed serving environment variables](https://docs.databricks.com/aws/en/machine-learning/model-serving/store-env-variable-model-serving).
