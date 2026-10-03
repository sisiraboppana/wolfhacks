from datetime import datetime, timezone
import math
import pytest
from pydantic import ValidationError
from kinesis_producer.mock import MockGenerator
from shared.schemas import TelemetryEvent
from shared.metrics import hrv

def test_roundtrip_and_missing_rr():
    event = MockGenerator(scenario='missing_rr').next()
    assert TelemetryEvent.model_validate_json(event.model_dump_json()) == event
    assert event.rr_intervals_ms == []
    assert hrv([])['rmssd_ms'] is None

def test_invalid_timezone_and_rr():
    payload = MockGenerator().next().model_dump()
    payload['timestamp'] = datetime(2026,1,1)
    with pytest.raises(ValidationError): TelemetryEvent(**payload)
    payload['timestamp'] = datetime.now(timezone.utc)
    payload['rr_intervals_ms'] = [-1]
    with pytest.raises(ValidationError): TelemetryEvent(**payload)

def test_hrv_hand_worked():
    result = hrv([800,810,790])
    assert result['rmssd_ms'] == round(math.sqrt(250),2)
    assert result['sdnn_ms'] == 10

def test_mock_scenarios():
    normal, rise, spike = [MockGenerator(scenario=s) for s in ['normal','rise','spike']]
    a,b,c = [[g.next() for _ in range(100)] for g in [normal,rise,spike]]
    assert b[-1].heart_rate > a[-1].heart_rate + 20
    assert c[30].heart_rate > a[30].heart_rate + 40
    assert c[33].heart_rate == a[33].heart_rate
    assert [e.sequence for e in a] == list(range(100))
