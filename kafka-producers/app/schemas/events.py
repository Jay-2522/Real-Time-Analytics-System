from datetime import UTC, datetime
from typing import Annotated, Literal, Union
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field


class BaseTelemetryEvent(BaseModel):
    event_id: UUID = Field(default_factory=uuid4)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    risk_score: float = Field(ge=0.0, le=100.0)
    event_type: str

    model_config = ConfigDict(
        extra="forbid",
        json_encoders={datetime: lambda value: value.isoformat(), UUID: str},
    )

    def to_json_bytes(self) -> bytes:
        return self.model_dump_json().encode("utf-8")


class LoginEvent(BaseTelemetryEvent):
    user_id: str | None = None
    username: str
    source_ip: str
    country: str | None = None
    device_type: str | None = None
    login_success: bool
    failure_reason: str | None = None
    event_type: Literal["login"] = "login"


class ApiEvent(BaseTelemetryEvent):
    user_id: str
    endpoint: str
    method: Literal["GET", "POST", "PUT", "PATCH", "DELETE"]
    status_code: int = Field(ge=100, le=599)
    response_time_ms: int = Field(ge=1)
    source_ip: str
    request_size: int = Field(ge=0)
    event_type: Literal["api"] = "api"


class NetworkEvent(BaseTelemetryEvent):
    source_ip: str
    destination_ip: str
    protocol: Literal["TCP", "UDP", "ICMP"]
    bytes_sent: int | None = Field(default=None, ge=0)
    bytes_received: int | None = Field(default=None, ge=0)
    port: int = Field(ge=1, le=65535)
    packet_count: int | None = Field(default=None, ge=1)
    event_type: Literal["network"] = "network"


class AnomalyEvent(BaseTelemetryEvent):
    anomaly_type: str
    severity: Literal["low", "medium", "high", "critical"]
    source_ip: str
    user_id: str | None = None
    description: str
    event_type: Literal["anomaly"] = "anomaly"


TelemetryEvent = Annotated[
    Union[LoginEvent, ApiEvent, NetworkEvent, AnomalyEvent],
    Field(discriminator="event_type"),
]
