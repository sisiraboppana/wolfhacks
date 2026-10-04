"""Canonical normalized Polar H10 notification; RR values are milliseconds."""
from typing import Annotated, Literal
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

RR = Annotated[float, Field(gt=0, le=64000, allow_inf_nan=False)]

class RecordingContext(BaseModel):
    model_config = ConfigDict(extra='forbid')
    dataset: Literal['hrv_acc', 'ppg_dalia']
    subject: str = Field(min_length=1, max_length=128)
    hr_method: Literal['rr_derived', 'ecg_rpeaks_derived', 'windowed_ecg_ground_truth']
    original_offset_seconds: float = Field(ge=0, allow_inf_nan=False)
    activity_label: Literal['transition', 'sitting', 'stairs', 'table_soccer', 'cycling', 'driving', 'lunch', 'walking', 'working'] | None = None
    hr_window_seconds: float | None = Field(default=None, gt=0, allow_inf_nan=False)

class TelemetryEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")
    schema_version: Literal["1.0"] = "1.0"
    user_id: str = Field(min_length=1, max_length=128)
    device_id: str = Field(min_length=1, max_length=128)
    session_id: str = Field(min_length=1, max_length=128)
    sequence: int = Field(ge=0)
    timestamp: AwareDatetime
    heart_rate: int = Field(ge=0, le=65535)
    rr_intervals_ms: list[RR] = Field(default_factory=list, max_length=256)
    source: Literal["mock", "polar_h10", "replay"] = "mock"
    sensor_contact: bool | None = None
    recording: RecordingContext | None = None

class ChatRequest(BaseModel):
    prompt: str = Field(min_length=1, max_length=2000)
    user_id: str = "demo-athlete"
    session_id: str = "demo-session"
