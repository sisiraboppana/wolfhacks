import math
import random
from datetime import datetime, timezone
from shared.schemas import TelemetryEvent

class MockGenerator:
    """One notification per second; emit RR intervals only when beats occur."""
    def __init__(self, seed=42, scenario="normal", user_id="demo-athlete", session_id="demo-session"):
        self.rng = random.Random(seed)
        self.scenario, self.user_id, self.session_id = scenario, user_id, session_id
        self.sequence, self.beat_credit = 0, 0.0

    def next(self, timestamp=None):
        n = self.sequence
        rise = min(30, max(0, n-30)*0.5) if self.scenario == "rise" else 0
        spike = 45 if self.scenario == "spike" and 30 <= n < 33 else 0
        bpm = round(110 + 4*math.sin(n/12) + rise + spike)
        self.beat_credit += bpm/60
        beats = int(self.beat_credit)
        self.beat_credit -= beats
        rr = [round(60000/bpm + self.rng.gauss(0, 12), 3) for _ in range(beats)]
        event = TelemetryEvent(user_id=self.user_id, device_id="mock-polar-h10", session_id=self.session_id,
            sequence=n, timestamp=timestamp or datetime.now(timezone.utc), heart_rate=bpm,
            rr_intervals_ms=[] if self.scenario == "missing_rr" else rr, source="mock", sensor_contact=True)
        self.sequence += 1
        return event
