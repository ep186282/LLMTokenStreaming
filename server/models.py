from __future__ import annotations

from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator


GenerationStatus = Literal[
    "running",
    "completed",
    "cancelled",
    "failed",
    "interrupted",
]


class GenerationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    model: str | None = None
    messages: list[dict[str, Any]] = Field(min_length=1)

    @field_validator("model")
    @classmethod
    def validate_model(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        if not value:
            raise ValueError("model must not be empty")
        return value


class GenerationAccepted(BaseModel):
    generation_id: UUID


class GenerationView(BaseModel):
    id: UUID
    status: GenerationStatus
    seq: int
    final_seq: int | None
    text: str
    finish_reason: str | None
    usage: dict[str, Any] | None
    error: str | None
    cancel_requested: bool


class CancelResult(BaseModel):
    status: Literal["cancelled", "already_terminal"]


class HealthResult(BaseModel):
    status: Literal["ok"]
    provider: Literal["openrouter"]
