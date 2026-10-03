"""Read-only tools over one ordered session. Thresholds are demo rules."""
from statistics import mean
from shared.metrics import hrv
from shared.schemas import TelemetryEvent

def normalize_events(payload):
    if not isinstance(payload, list) or not 1 <= len(payload) <= 600:
        raise ValueError('Provide 1 to 600 notifications per request')
    events = [TelemetryEvent.model_validate(item) for item in payload]
    if len({(e.user_id,e.session_id,e.device_id) for e in events}) != 1:
        raise ValueError('One user, session, and device per request')
    unique = {}
    for event in events:
        if event.sequence in unique and unique[event.sequence] != event:
            raise ValueError('Conflicting duplicate sequence number')
        unique[event.sequence] = event
    events = sorted(unique.values(), key=lambda e:e.sequence)
    if any(b.timestamp <= a.timestamp for a,b in zip(events,events[1:])):
        raise ValueError('Event timestamps must increase with sequence')
    return events

def usable(events):
    return [e for e in events if 30 <= e.heart_rate <= 230 and e.sensor_contact is not False]

def summarize_hr_trend(events):
    good = usable(events)
    if not good:
        return {'status':'insufficient_data','valid_notifications':0}
    start,end = good[0].timestamp,good[-1].timestamp
    duration = (end-start).total_seconds()
    windows = {}
    for event in good:
        bucket = int((event.timestamp-start).total_seconds()//60)
        windows.setdefault(bucket,[]).append(event.heart_rate)
    summary = [{'start_seconds':i*60,'mean_bpm':round(mean(v),2),'notifications':len(v)} for i,v in sorted(windows.items())]
    early = [e.heart_rate for e in good if (e.timestamp-start).total_seconds() < 60]
    late = [e.heart_rate for e in good if (end-e.timestamp).total_seconds() < 60]
    first,last = mean(early),mean(late)
    return {'status':'ok' if duration >= 120 else 'short_recording','duration_seconds':duration,
        'valid_notifications':len(good),'excluded_notifications':len(events)-len(good),
        'first_minute_mean_bpm':round(first,2),'last_minute_mean_bpm':round(last,2),
        'first_reading_bpm':good[0].heart_rate,'last_reading_bpm':good[-1].heart_rate,
        'mean_hr_change_percent':round(100*(last-first)/first,2) if duration >= 120 else None,
        'windows':summary,'workload_comparison':'unavailable',
        'comparison':'First and last recorded minutes; not a resting or post-warm-up baseline.'}

def detect_sustained_changes(events):
    good = usable(events)
    if not good:
        return {'status':'insufficient_data'}
    start = good[0].timestamp
    baseline_samples = [e.heart_rate for e in good if (e.timestamp-start).total_seconds() < 30]
    if len(baseline_samples) < 20 or (good[-1].timestamp-start).total_seconds() < 60:
        return {'status':'insufficient_data','reason':'Need at least 20 baseline notifications and 60 seconds of observation.'}
    baseline = mean(baseline_samples)
    threshold = baseline*1.10
    runs,active,previous = [],[],None
    def finish():
        if active:
            duration = (active[-1].timestamp-active[0].timestamp).total_seconds()
            runs.append({'start_seconds':(active[0].timestamp-start).total_seconds(),
                'duration_seconds':duration,'peak_bpm':max(e.heart_rate for e in active),
                'sustained':duration >= 30})
            active.clear()
    for event in events:
        if (event.timestamp-start).total_seconds() < 30:
            continue
        continuous = previous is None or (event.sequence == previous.sequence+1 and
            0 < (event.timestamp-previous.timestamp).total_seconds() <= 2.5)
        if not continuous:
            finish()
        if 30 <= event.heart_rate <= 230 and event.sensor_contact is not False and event.heart_rate > threshold:
            active.append(event)
        else:
            finish()
        previous = event
    finish()
    return {'status':'ok','reference_mean_bpm':round(baseline,2),'threshold_bpm':round(threshold,2),
        'demo_threshold_percent':10,'required_duration_seconds':30,
        'sustained_change':any(r['sustained'] for r in runs),'episodes':runs,
        'reference':'First 30 recorded seconds; not a physiological baseline.',
        'interpretation':'Detects sustained HR rise only; workload, causes and recovery are unknown.'}

def inspect_rr_continuity(events):
    segment,previous = [],None
    gaps,missing,bad_contact,implausible = 0,0,0,0
    for event in events:
        if previous and (event.sequence != previous.sequence+1 or
                (event.timestamp-previous.timestamp).total_seconds() > 2.5):
            gaps += 1
            segment = []
        if event.sensor_contact is False:
            bad_contact += 1
            segment = []
        elif not event.rr_intervals_ms:
            missing += 1
            segment = []
        elif any(not 250 <= rr <= 2500 for rr in event.rr_intervals_ms):
            implausible += 1
            segment = []
        else:
            segment.extend(event.rr_intervals_ms)
        previous = event
    return {'metrics':hrv(segment),'sequence_or_time_gaps':gaps,'missing_rr_notifications':missing,
        'bad_contact_notifications':bad_contact,'implausible_rr_notifications':implausible,'notification_count':len(events),
        'scope':'Latest continuous valid RR segment only; demo plausibility filter 250–2500 ms.',
        'limitations':'No artifact correction or stationary/resting validation. Changing HR can affect SDNN.'}

TOOL_FUNCTIONS = {'summarize_hr_trend':summarize_hr_trend,
    'detect_sustained_changes':detect_sustained_changes,'inspect_rr_continuity':inspect_rr_continuity}
TOOL_DESCRIPTIONS = {
    'summarize_hr_trend':'Calculate first/last minute HR means and recorded session windows.',
    'detect_sustained_changes':'Distinguish sustained HR rise from a brief spike using a 10%/30-second demo rule.',
    'inspect_rr_continuity':'Check RR continuity and quality; compute HRV on the latest valid continuous segment.',
}
