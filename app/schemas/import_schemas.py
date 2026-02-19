"""
Import-related Pydantic Schemas
"""
from datetime import datetime
from typing import List, Optional, Dict, Any,  Literal
from uuid import UUID

from pydantic import BaseModel, Field, field_validator, ConfigDict, model_validator, RootModel


class ValidationError(BaseModel):
    """Validation error detail."""
    field: str
    message: str
    value: Optional[Any] = None

##########################################################################################

class Evidence(BaseModel):
    associatedEventIds: Optional[List[str]] = None

    model_config = ConfigDict(extra="forbid")

class Highlight(BaseModel):
    insight: str
    start: float
    end: float
    confidenceScore: Optional[float] = Field(default=None, ge=0, le=100)
    eventMeta: Evidence

    @model_validator(mode="after")
    def validate_time_range(self):
        if self.end <= self.start:
            raise ValueError("end must be greater than start")
        return self

    model_config = ConfigDict(extra="forbid")


class Asset(BaseModel):
    repo_guid: str
    fullPath: str

    model_config = ConfigDict(extra="forbid")

class ImportWorkOrder(BaseModel):
    schemaVersion: Literal["sdna.spark.import.v1"]
    asset: Asset
    highlights: List[Highlight]

    @field_validator("highlights")
    @classmethod
    def validate_highlights_not_empty(cls, v: List[Highlight]):
        if not v:
            raise ValueError("highlights must contain at least one item")
        return v

    model_config = ConfigDict(extra="forbid")


class ImportJobResponse(BaseModel):
    """Response schema for import job."""
    import_id: UUID
    repo_guid: str
    status: str
    items_processed: int = 0
    items_created: int = 0
    items_updated: int = 0
    items_skipped: int = 0
    validation_errors: Optional[List[ValidationError]] = None
    error_message: Optional[str] = None
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    class Config:
        from_attributes = True
        
class ImportJobListResponse(BaseModel):
    """Response schema for listing export jobs."""
    total: int
    jobs: List[ImportJobResponse]        
