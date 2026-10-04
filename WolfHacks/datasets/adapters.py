"""Preserve recorded RR; never reconstruct RR from averaged heart rate."""
import csv
import math
from datetime import datetime, timedelta
from pathlib import Path
from shared.schemas import TelemetryEvent, RecordingContext

ACTIVITIES = {0:'transition', 1:'sitting', 2:'stairs', 3:'table_soccer',
              4:'cycling', 5:'driving', 6:'lunch', 7:'walking', 8:'working'}

def hrv_acc(path, start, session_id, user_id='demo-athlete', start_seconds=0, duration=None):
    """Phone times have no recording date/timezone; map elapsed time to an explicit replay clock.

    Each CSV row becomes one beat event, not a fabricated BLE notification.
    RR values remain unchanged. BPM is rounded 60000/RR, not device-reported HR.
    """
    if start.tzinfo is None:
        raise ValueError('Replay start must have a timezone')
    first = previous = None
    day = 0
    sequence = 0
    with Path(path).open(encoding='utf-8-sig', newline='') as stream:
        sample = stream.read(4096)
        stream.seek(0)
        dialect = csv.Sniffer().sniff(sample, delimiters=',;\t')
        reader = csv.DictReader(stream, dialect=dialect)
        if not {'Phone timestamp','RR-interval [ms]'}.issubset(reader.fieldnames or []):
            raise ValueError('Expected HRV-ACC Phone timestamp and RR-interval [ms] columns')
        for row_number,row in enumerate(reader,2):
            time_of_day = datetime.strptime(row['Phone timestamp'].strip(), '%H:%M:%S.%f')
            if previous and time_of_day < previous:
                if (previous-time_of_day).total_seconds() < 12*3600:
                    raise ValueError(f'Out-of-order phone timestamp at row {row_number}')
                day += 1
            timestamp = time_of_day+timedelta(days=day)
            if first is None:
                first = timestamp
            elapsed = (timestamp-first).total_seconds()
            if previous == time_of_day:
                raise ValueError(f'Duplicate phone timestamp at row {row_number}')
            previous = time_of_day
            if elapsed < start_seconds:
                continue
            if duration is not None and elapsed >= start_seconds+duration:
                break
            rr = float(row['RR-interval [ms]'])
            if not math.isfinite(rr) or not 0 < rr <= 64000:
                raise ValueError(f'Invalid recorded RR at row {row_number}; raw file remains unchanged')
            event = TelemetryEvent(user_id=user_id, device_id='polar-h10-hrv-acc',
                session_id=session_id, sequence=sequence, timestamp=start+timedelta(seconds=elapsed-start_seconds),
                heart_rate=round(60000/rr), rr_intervals_ms=[rr], source='replay', sensor_contact=None,
                recording=RecordingContext(dataset='hrv_acc', subject=Path(path).stem,
                    hr_method='rr_derived',original_offset_seconds=elapsed))
            yield event
            sequence += 1

def dalia_windows(heart_rates, labels, start, session_id, subject, user_id='demo-athlete',
                  start_seconds=0, duration=None):
    """Provided 8s ECG-derived HR windows, shifted 2s; emit at window END to avoid look-ahead.

    Activity annotations are at 4 Hz. Only label a window when all its annotations agree;
    otherwise mark it transition. No RR, contact assessment or artifact diagnosis is invented.
    """
    if start.tzinfo is None:
        raise ValueError('Replay start must have a timezone')
    sequence = 0
    for i,hr in enumerate(heart_rates):
        begin,end = 2*i,2*i+8
        if end < start_seconds:
            continue
        if duration is not None and end >= start_seconds+duration:
            break
        hr = float(hr)
        if not math.isfinite(hr) or not 0 <= hr <= 65535:
            raise ValueError(f'Invalid HR window {i}')
        window_labels = [int(v) for v in labels[begin*4:end*4]]
        if len(window_labels) != 32 or any(v not in ACTIVITIES for v in window_labels):
            raise ValueError(f'Invalid or incomplete activity labels for HR window {i}')
        unique = set(window_labels)
        activity = ACTIVITIES[next(iter(unique))] if len(unique)==1 else 'transition'
        yield TelemetryEvent(user_id=user_id, device_id='respiban-ppg-dalia', session_id=session_id,
            sequence=sequence, timestamp=start+timedelta(seconds=end-start_seconds),
            heart_rate=round(hr), rr_intervals_ms=[], source='replay', sensor_contact=None,
            recording=RecordingContext(dataset='ppg_dalia',subject=subject,
                hr_method='windowed_ecg_ground_truth',original_offset_seconds=end,
                activity_label=activity,hr_window_seconds=8))
        sequence += 1

def dalia_beats(rpeaks, labels, start, session_id, subject, user_id='demo-athlete',
                start_seconds=0, duration=None):
    """RR from authors' corrected R-peak indices at 700Hz; no local peak detector needed."""
    if start.tzinfo is None:
        raise ValueError('Replay start must have a timezone')
    peaks = [int(x) for x in rpeaks]
    if any(float(original) != peak for original,peak in zip(rpeaks,peaks)) or any(
            b <= a for a,b in zip(peaks,peaks[1:])) or (peaks and peaks[0] < 0):
        raise ValueError('R-peaks must be nonnegative strictly increasing integer ECG indices')
    sequence = 0
    for a,b in zip(peaks,peaks[1:]):
        end = b/700
        if end < start_seconds:
            continue
        if duration is not None and end >= start_seconds+duration:
            break
        index = int(end*4)
        if index >= len(labels) or int(labels[index]) not in ACTIVITIES:
            raise ValueError('Missing or invalid activity annotation for ECG beat')
        rr = (b-a)*1000/700
        yield TelemetryEvent(user_id=user_id, device_id='respiban-ppg-dalia', session_id=session_id,
            sequence=sequence,timestamp=start+timedelta(seconds=end-start_seconds),
            heart_rate=round(60000/rr),rr_intervals_ms=[rr],source='replay',sensor_contact=None,
            recording=RecordingContext(dataset='ppg_dalia',subject=subject,hr_method='ecg_rpeaks_derived',
                original_offset_seconds=end,activity_label=ACTIVITIES[int(labels[index])]))
        sequence += 1
