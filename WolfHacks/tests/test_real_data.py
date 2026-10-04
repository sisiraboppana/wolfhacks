import io
import json
import pickle
from datetime import datetime,timezone
import httpx
import pytest
from datasets.adapters import hrv_acc,dalia_beats,dalia_windows
from datasets.prepare import NumpyDatasetUnpickler
from datasets.replay import batches,upload
from agent.session_tools import normalize_events,summarize_hr_trend,inspect_rr_continuity

START = datetime(2026,10,3,12,tzinfo=timezone.utc)

def test_hrv_midnight_and_recorded_rr_preserved(tmp_path):
    path = tmp_path/'control.csv'
    path.write_text('Phone timestamp;RR-interval [ms]\n23:59:59.500000;800\n00:00:00.300000;790\n00:00:01.090000;810\n')
    events = list(hrv_acc(path,START,'session'))
    assert [e.rr_intervals_ms for e in events] == [[800],[790],[810]]
    assert (events[-1].timestamp-events[0].timestamp).total_seconds()==1.59
    assert events[0].sensor_contact is None
    assert events[0].heart_rate == 75
    assert events[0].source == 'replay'

def test_hrv_rejects_backwards_time_instead_of_inventing_day(tmp_path):
    path=tmp_path/'record.csv'
    path.write_text('Phone timestamp;RR-interval [ms]\n12:00:02.000000;800\n12:00:01.000000;800\n')
    with pytest.raises(ValueError,match='Out-of-order'):
        list(hrv_acc(path,START,'s'))

def test_dalia_rr_comes_from_corrected_peaks_and_labels():
    events = list(dalia_beats([0,700,1260],[1]*4+[7]*4,START,'s','S10'))
    assert [e.rr_intervals_ms for e in events]==[[1000],[800]]
    assert [e.recording.activity_label for e in events]==['walking','walking']
    assert events[0].recording.hr_method=='ecg_rpeaks_derived'
    assert (events[1].timestamp-events[0].timestamp).total_seconds()==0.8
    with pytest.raises(ValueError,match='strictly increasing'):
        list(dalia_beats([0,700,700],[1]*100,START,'s','S10'))

def test_dalia_window_end_cadence_and_no_fabricated_rr():
    events=list(dalia_windows([80,90],[1]*32+[7]*8,START,'s','S10'))
    assert (events[0].timestamp-START).total_seconds()==8
    assert (events[1].timestamp-events[0].timestamp).total_seconds()==2
    assert [e.recording.activity_label for e in events]==['sitting','transition']
    assert all(e.rr_intervals_ms==[] for e in events)
    result=inspect_rr_continuity(events)
    assert result['metrics']['rmssd_ms'] is None
    assert result['rr_availability']=='not_provided_by_hr_window_adapter'

def test_provenance_reaches_tools_and_prevents_mixing_subjects():
    events=list(dalia_beats([0,700,1260],[1]*100,START,'s','S10'))
    payload=[e.model_dump(mode='json') for e in events]
    assert summarize_hr_trend(normalize_events(payload))['datasets']==['ppg_dalia']
    payload[-1]['recording']['subject']='S11'
    with pytest.raises(ValueError,match='consistent recording'):
        normalize_events(payload)

def test_pickle_rejects_arbitrary_globals():
    import os
    data=pickle.dumps(os.system)
    with pytest.raises(pickle.UnpicklingError,match='Unexpected'):
        NumpyDatasetUnpickler(io.BytesIO(data),encoding='latin1').load()

def test_volume_batch_contents_and_permission_error():
    events=list(dalia_windows([80]*6,[1]*100,START,'s','S10'))
    assert [len(b) for b in batches(events,5)]==[3,2,1]
    def handler(request):
        assert request.url.path=='/api/2.0/fs/files/Volumes/workspace/pacepilot/telemetry/incoming/test.json'
        assert json.loads(request.content)['source']=='replay'
        return httpx.Response(403)
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(RuntimeError,match='files scope'):
            upload(client,'https://example.com','test-only','/Volumes/workspace/pacepilot/telemetry/incoming',
                   'test.json',events[0].model_dump_json().encode())

def test_volume_preflight_catches_missing_scope_before_replay():
    from datasets.replay import preflight
    def handler(request):
        assert request.method=='GET'
        return httpx.Response(403,json={'message':'missing files scope'})
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(RuntimeError,match='LOCAL .env token needs the files scope'):
            preflight(client,'https://example.com','test-only','/Volumes/workspace/pacepilot/telemetry/incoming')
