"""Generate timestamped mock JSON batches for manual upload to a UC volume."""
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from kinesis_producer.mock import MockGenerator

def main():
    output = Path('data/volume_demo') / uuid4().hex[:12]
    output.mkdir(parents=True, exist_ok=False)
    session = 'volume-demo-' + output.name
    generator = MockGenerator(scenario='rise', session_id=session)
    start = datetime.now(timezone.utc) - timedelta(minutes=6)
    for batch in range(6):
        records = [generator.next(timestamp=start + timedelta(seconds=batch*60+i)) for i in range(60)]
        path = output / f'{session}-batch-{batch:02d}.json'
        path.write_text('\n'.join(e.model_dump_json() for e in records)+'\n', encoding='utf-8')
    print(f'Created 360 synthetic notifications in six JSON files: {output.resolve()}')
    print('Upload the six files to /Volumes/workspace/pacepilot/telemetry/incoming')

if __name__ == '__main__':
    main()
