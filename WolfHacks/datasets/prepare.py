"""Convert verified recordings into the existing notification envelope plus provenance."""
import argparse
import hashlib
import json
import pickle
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4
from datasets.adapters import hrv_acc, dalia_beats, dalia_windows

class NumpyDatasetUnpickler(pickle.Unpickler):
    """Restrict the official legacy pickle to the NumPy array constructors it needs."""
    def find_class(self, module, name):
        allowed = {('numpy','ndarray'), ('numpy','dtype'),
                   ('numpy.core.multiarray','_reconstruct'), ('numpy.core.multiarray','scalar'),
                   ('numpy._core.multiarray','_reconstruct'), ('numpy._core.multiarray','scalar')}
        if (module,name) not in allowed:
            raise pickle.UnpicklingError(f'Unexpected dataset pickle global: {module}.{name}')
        return super().find_class(module,name)

def load_dalia(path):
    import numpy as np
    path = Path(path)
    if path.suffix == '.npz':
        with np.load(path,allow_pickle=False) as arrays:
            return {k:arrays[k] for k in ('rpeaks','activity','label')}
    with path.open('rb') as stream:
        data = NumpyDatasetUnpickler(stream,encoding='latin1').load()
    compact = {k:np.asarray(data[k]).reshape(-1) for k in ('rpeaks','activity','label')}
    if any(array.dtype.hasobject for array in compact.values()):
        raise ValueError('Object arrays are not accepted')
    np.savez_compressed(path.with_suffix('.npz'),**compact)
    return compact

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset',choices=['hrv_acc','ppg_dalia'],required=True)
    parser.add_argument('--input',required=True)
    parser.add_argument('--subject',default='S10')
    parser.add_argument('--dalia-mode',choices=['beats','hr-windows'],default='beats')
    parser.add_argument('--start-seconds',type=float,default=0)
    parser.add_argument('--duration',type=float,default=600)
    parser.add_argument('--session-id')
    args = parser.parse_args()
    if args.start_seconds < 0 or args.duration <= 0:
        parser.error('start-seconds must be nonnegative and duration positive')
    session = args.session_id or f'{args.dataset}-{uuid4().hex[:12]}'
    if any(c not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_' for c in session):
        parser.error('session-id must use letters, digits, underscores and hyphens')
    output = Path('data/real')/session
    output.mkdir(parents=True,exist_ok=False)
    source = Path(args.input)
    start = datetime.now(timezone.utc)
    options = dict(start=start,session_id=session,start_seconds=args.start_seconds,duration=args.duration)
    if args.dataset == 'hrv_acc':
        events = list(hrv_acc(source,**options))
    else:
        data = load_dalia(source)
        function = dalia_beats if args.dalia_mode == 'beats' else dalia_windows
        events = list(function(data['rpeaks'] if args.dalia_mode=='beats' else data['label'],
            data['activity'],subject=args.subject,**options))
    if not events:
        raise ValueError('Selected interval contains no events')
    (output/'events.jsonl').write_text('\n'.join(e.model_dump_json(exclude_none=True) for e in events)+'\n',encoding='utf-8')
    sha = hashlib.sha256()
    with source.open('rb') as stream:
        for block in iter(lambda:stream.read(1024*1024),b''):
            sha.update(block)
    manifest = {'dataset':args.dataset,'session_id':session,'source_file':source.name,
        'source_sha256':sha.hexdigest(),'event_count':len(events),'rr_count':sum(len(e.rr_intervals_ms) for e in events),
        'heart_rate_method':events[0].recording.hr_method,'activity_labels':sorted({e.recording.activity_label for e in events if e.recording.activity_label}),
        'selected_start_seconds':args.start_seconds,'selected_duration_seconds':args.duration,
        'timing':'Recorded relative timing preserved; dates assigned for replay; no measurement date or timezone inferred.',
        'license':'CC BY 4.0; see docs/real-data.md for attribution',
        'record_url':'https://zenodo.org/records/8171266' if args.dataset=='hrv_acc' else 'https://archive.ics.uci.edu/dataset/495/ppg+dalia'}
    (output/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n',encoding='utf-8')
    print(json.dumps(manifest,indent=2))
    print(f'REPLAY INPUT: {(output/"events.jsonl").resolve()}')

if __name__ == '__main__':
    main()
