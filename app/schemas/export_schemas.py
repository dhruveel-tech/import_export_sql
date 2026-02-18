"""
Export-related Pydantic Schemas
"""
from datetime import datetime
from typing import List, Optional, Dict, Any
from uuid import UUID

from pydantic import BaseModel, Field, field_validator


class OutputFormats(BaseModel):
    """Output format configuration for a specific data type."""
    formats: List[str] = Field(..., min_length=1)
    isSingleSegment: Optional[bool] = False  
    
    @field_validator("formats")
    @classmethod
    def validate_formats(cls, v: List[str]) -> List[str]:
        allowed = {"json", "csv", "srt", "vtt", "fcpxml", "edl"}
        for fmt in v:
            if fmt not in allowed:
                raise ValueError(f"Invalid format: {fmt}. Allowed: {allowed}")
        return v


class SelectsConfig(BaseModel):
    """Selects/assembly configuration."""
    enabled: bool = False
    formats: List[str] = Field(default=["edl", "fcpxml"])

    @field_validator("formats")
    @classmethod
    def validate_formats(cls, v: List[str]) -> List[str]:
        allowed = {"edl", "fcpxml"}
        for fmt in v:
            if fmt not in allowed:
                raise ValueError(f"Invalid selects format: {fmt}. Allowed: {allowed}")
        return v


class GroundingConfig(BaseModel):
    """Grounding prompt configuration."""
    enabled: bool = True


class SubclipsConfig(BaseModel):
    """Subclips configuration."""
    enabled: bool = True
    source: List[str] = Field(default=["comments", "markers"])
    handle_seconds: int = Field(default=2, ge=0, le=30)

    @field_validator("source")
    @classmethod
    def validate_source(cls, v: List[str]) -> List[str]:
        allowed = {"comments", "markers"}
        for src in v:
            if src not in allowed:
                raise ValueError(f"Invalid subclip source: {src}. Allowed: {allowed}")
        return v


class MediaConfig(BaseModel):
    """Media export configuration."""
    include_full_proxy: bool = False
    subclips: SubclipsConfig = Field(default_factory=SubclipsConfig)


class ExportInputs(BaseModel):
    """Export input data selection."""
    event_ids: List[str] = Field(default_factory=list)

class UserInputs(BaseModel):
    """User input configuration for export."""
    prompt: Optional[str] = None
    llm_instructions: Optional[bool] = True 

class ExportOutputs(BaseModel):
    """Export output configuration."""
    transcript: Optional[OutputFormats] = None
    events: Optional[OutputFormats] = None
    comments: Optional[OutputFormats] = None
    markers: Optional[OutputFormats] = None
    selects: SelectsConfig = Field(default_factory=SelectsConfig)
    grounding: GroundingConfig = Field(default_factory=GroundingConfig)


class ExportMetadata(BaseModel):
    """Export metadata."""
    requested_by: Optional[str] = None
    requested_at: datetime = Field(default_factory=datetime.utcnow)
    export_preset: str = "editorial"
    export_mode: str = Field(default="editorial", pattern="^(editorial|llm|review|promo|custom)$")


class ExportWorkOrderCreate(BaseModel):
    """Schema for creating an export work order."""
    spark_version: str = Field(default="1.0")
    repo_guid: str = Field(..., min_length=1, max_length=255)
    inputs: ExportInputs = Field(default_factory=ExportInputs)
    user_inputs: UserInputs = Field(default_factory=UserInputs)
    outputs: ExportOutputs = Field(default_factory=ExportOutputs)
    media: MediaConfig = Field(default_factory=MediaConfig)
    metadata: ExportMetadata = Field(default_factory=ExportMetadata)


class ArtifactResponse(BaseModel):
    """Response schema for an artifact."""
    artifact_type: str
    format: str
    filename: str
    url: str
    file_size: Optional[int] = None

    class Config:
        from_attributes = True


class ExportManifest(BaseModel):
    """Export package manifest."""
    spark_id: UUID
    repo_guid: str
    status: str
    created_at: datetime
    artifacts: List[ArtifactResponse]


class ExportJobResponse(BaseModel):
    """Response schema for export job."""
    spark_id: UUID
    repo_guid: str
    status: str
    export_mode: str
    export_preset: Optional[str]
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    manifest: Optional[ExportManifest] = None

    class Config:
        from_attributes = True


class ExportJobStatusResponse(BaseModel):
    """Response schema for export job status."""
    spark_id: UUID
    status: str
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None

    class Config:
        from_attributes = True


class ExportJobListResponse(BaseModel):
    """Response schema for listing export jobs."""
    total: int
    jobs: List[ExportJobResponse]

class UpdateLLmPrompt(BaseModel):
    spark_id: UUID
    prompt: str = Field(..., min_length=1)
    
class UpdateLLmPromptResponse(BaseModel):
    spark_id: UUID
    status: str
    file_path: str
    error_message: Optional[str] = None

    class Config:
        from_attributes = True
        
class GetLLmPromptResponse(BaseModel):
    spark_id: UUID
    status: str
    file_path: str
    error_message: Optional[str] = None

    class Config:
        from_attributes = True