"""Version 2: full hosted tool loop with Databricks model-service reasoning."""
import json
import os
import mlflow.pyfunc
from agent.session_tools import normalize_events
from agent.llm_agent import run_agent

class WearableAgentModel(mlflow.pyfunc.PythonModel):
    def predict(self, context, model_input, params=None):
        results = []
        for row in model_input.to_dict(orient='records'):
            events = normalize_events(json.loads(row['telemetry_json']))
            completion = None
            if os.getenv('PACEPILOT_PACKAGING_OFFLINE') == '1':
                def completion(*args):
                    raise RuntimeError('Offline packaging check')
            output = run_agent(events,row['prompt'],completion=completion)
            results.append({'response_json':json.dumps(output)})
        return results
