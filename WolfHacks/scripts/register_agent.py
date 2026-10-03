"""Run manually in configured Databricks development compute from repo root.
Logs/registers the initial agent model; creates no serving endpoint.
"""
import os
import json
import pandas as pd
import mlflow
from mlflow.models import infer_signature
from agent.serving_model import WearableAgentModel
from kinesis_producer.mock import MockGenerator

def main():
    mlflow.set_registry_uri('databricks-uc')
    name = os.environ.get('DATABRICKS_AGENT_MODEL', 'workspace.pacepilot.wearable_agent')
    example = pd.DataFrame([{'prompt':'Summarize session', 'telemetry_json':json.dumps([MockGenerator().next().model_dump(mode='json')])}])
    model = WearableAgentModel()
    output = model.predict(None, example)
    with mlflow.start_run():
        info = mlflow.pyfunc.log_model(name='agent', python_model=model,
            code_paths=['agent', 'shared'], input_example=example, signature=infer_signature(example, output),
            pip_requirements=[f'mlflow=={mlflow.__version__}', 'pydantic>=2.10,<3'],
            registered_model_name=name)
        print(info.model_uri)

if __name__ == '__main__':
    main()
