import json
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from shared.schemas import TelemetryEvent
Path('shared/telemetry.schema.json').write_text(json.dumps(TelemetryEvent.model_json_schema(), indent=2)+'\n', encoding='utf-8')
