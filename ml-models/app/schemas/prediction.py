from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field


Severity = Literal["low", "medium", "high", "critical"]


class PredictionEvent(BaseModel):
    prediction_id: UUID = Field(default_factory=uuid4)
    source_event_id: str | None = None
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    model_name: str
    model_version: str
    event_type: str
    entity_id: str
    anomaly_score: float = Field(ge=0.0, le=1.0)
    is_anomaly: bool
    ml_risk_score: float = Field(ge=0.0, le=100.0)
    confidence_score: float = Field(ge=0.0, le=1.0)
    severity: Severity
    explanation: str
    features_used: dict[str, float]
    feature_contract_version: str | None = None
    entity_type: str | None = None
    source_event_ids: list[str] = Field(default_factory=list)
    window_start: datetime | None = None
    window_end: datetime | None = None
    first_event_at: datetime | None = None
    last_event_at: datetime | None = None
    calculated_at: datetime | None = None

    model_config = ConfigDict(
        json_encoders={datetime: lambda value: value.isoformat(), UUID: str},
        protected_namespaces=(),
    )

    def to_json_bytes(self) -> bytes:
        return self.model_dump_json().encode("utf-8")


class DeadLetterEvent(BaseModel):
    event_id: UUID = Field(default_factory=uuid4)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    source_topic: str
    reason: str
    payload: Any
    event_type: Literal["deadletter"] = "deadletter"

    model_config = ConfigDict(json_encoders={datetime: lambda value: value.isoformat(), UUID: str})

    def to_json_bytes(self) -> bytes:
        return self.model_dump_json().encode("utf-8")
