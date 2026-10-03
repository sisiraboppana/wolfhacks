import math
from statistics import stdev

def hrv(rr: list[float]) -> dict:
    """Sample SDNN and RMSSD. Caller must preserve beat order and continuity."""
    if len(rr) < 2:
        return {"rmssd_ms": None, "sdnn_ms": None, "rr_count": len(rr)}
    return {
        "rmssd_ms": round(math.sqrt(sum((b-a)**2 for a,b in zip(rr, rr[1:])) / (len(rr)-1)), 2),
        "sdnn_ms": round(stdev(rr), 2), "rr_count": len(rr),
    }
