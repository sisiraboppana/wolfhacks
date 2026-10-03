import json
from datetime import datetime, timedelta, timezone
import pytest
from agent.session_tools import normalize_events, summarize_hr_trend, detect_sustained_changes, inspect_rr_continuity
from agent.llm_agent import run_agent
from kinesis_producer.mock import MockGenerator

def session(scenario='normal',count=180):
    generator = MockGenerator(scenario=scenario)
    start = datetime(2026,10,3,tzinfo=timezone.utc)
    return [generator.next(timestamp=start+timedelta(seconds=i)) for i in range(count)]

@pytest.mark.parametrize('scenario,sustained',[('normal',False),('rise',True),('spike',False)])
def test_sustained_and_spike(scenario,sustained):
    result = detect_sustained_changes(session(scenario))
    assert result['sustained_change'] is sustained
    if scenario == 'spike':
        assert result['episodes'][0]['duration_seconds'] == 2

def test_trend_uses_event_time_not_array_count():
    events = session('rise')
    trend = summarize_hr_trend(events)
    assert trend['duration_seconds'] == 179
    assert trend['mean_hr_change_percent'] > 10
    assert summarize_hr_trend(events[:30])['mean_hr_change_percent'] is None

def test_missing_rr_and_sequence_gap_break_continuity():
    events = session(count=10)
    events[7].rr_intervals_ms = []
    result = inspect_rr_continuity(events)
    assert result['missing_rr_notifications'] == 1
    assert result['metrics']['rr_count'] == sum(len(e.rr_intervals_ms) for e in events[8:])
    result = inspect_rr_continuity(events[:4]+events[6:])
    assert result['sequence_or_time_gaps'] == 1

def test_contact_and_implausible_rr():
    events = session(count=5)
    events[-1].sensor_contact = False
    assert inspect_rr_continuity(events)['metrics']['rr_count'] == 0
    events[-1].sensor_contact = True
    events[-1].rr_intervals_ms = [10]
    assert inspect_rr_continuity(events)['implausible_rr_notifications'] == 1

def test_mixed_identity_and_duplicate_conflict_rejected():
    payload = [e.model_dump(mode='json') for e in session(count=2)]
    payload[1]['session_id'] = 'other'
    with pytest.raises(ValueError,match='One user'): normalize_events(payload)
    payload = [e.model_dump(mode='json') for e in session(count=1)]
    payload.append(dict(payload[0],heart_rate=120))
    with pytest.raises(ValueError,match='Conflicting'): normalize_events(payload)

def test_actual_tool_results_feed_llm_and_order_is_model_selected():
    calls = []
    def fake(messages,tools,choice):
        calls.append(choice)
        if choice == 'required':
            name = tools[-1]['function']['name']
            return {'content':None,'tool_calls':[{'id':f'call-{len(calls)}','type':'function',
                'function':{'name':name,'arguments':'{}'}}]}
        evidence = [json.loads(m['content']) for m in messages if m['role']=='tool']
        assert len(evidence)==3
        assert any(e.get('sustained_change') is True for e in evidence)
        return {'content':json.dumps({'summary':'Heart rate rose and stayed elevated.',
            'next_step':'Compare workout effort with this time range.','question':'Did you deliberately increase effort?'})}
    result = run_agent(session('rise'),'Explain the pattern',fake)
    assert result['mode']=='llm_agent'
    assert result['tool_trace'][0]['tool']=='inspect_rr_continuity'
    assert all(t['requested_by']=='llm' for t in result['tool_trace'])
    assert len(calls)==4

@pytest.mark.parametrize('kind',['timeout','unknown','arguments','invalid_final','duplicate'])
def test_failures_are_explicit_not_successful_reasoning(kind):
    def fake(messages,tools,choice):
        if kind=='timeout': raise TimeoutError()
        if choice=='none': return {'content':'not valid JSON'}
        name = 'delete_user' if kind=='unknown' else tools[0]['function']['name']
        args = '{"threshold":0}' if kind=='arguments' else '{}'
        call = {'id':'a','type':'function','function':{'name':name,'arguments':args}}
        return {'tool_calls':[call,call] if kind=='duplicate' else [call]}
    result = run_agent(session(),'summary',fake)
    assert result['mode']=='llm_unavailable'
    assert result['error']
    assert len(result['tool_trace'])==3
