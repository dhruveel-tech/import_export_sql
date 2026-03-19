"""
Video Split Schemas - Request and Response Models
Changes vs original:
  - VideoSplitStatusResponse: + progress, progress_stage, current_file
  - VideoSplitJobResponse:    + encoding, work_order, requested_by, results,
                                segments_processed/successful/failed, error_details
                                (fields that were silently missing before)
  - Everything else is unchanged.
"""
from typing import Any, Dict, List, Optional
from uuid import UUID
from datetime import datetime
from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Input / work-order schemas
# ---------------------------------------------------------------------------

# class ExportAllData(BaseModel):
#     is_all_transcript: bool = False
#     is_all_events: bool = False
#     is_all_insights: bool = False
#     exclude_list: Optional[List[str]] = Field(default_factory=list)
    
class ExportVideoInputs(BaseModel):
    """Export input data selection."""
    event_ids: List[str] = Field(default_factory=list)
    # is_all: Optional[ExportAllData] = None 
    video_path: str = Field(..., min_length=1, description="Absolute path of source video")
    # source_path: str = Field(..., min_length=1, description="Absolute path of source video") 


class ResizeVideoConfig(BaseModel):
    is_enabled: bool = False
    width: Optional[int] = 0
    height: Optional[int] = 0
    position: Optional[str] = "center"

class FeatureToggle(BaseModel):
    is_enabled: bool = False
    is_resize_enabled: Optional[ResizeVideoConfig] = Field(default_factory=ResizeVideoConfig)
    duration_threshold: Optional[float] = 0


class ExportVideoOutputs(BaseModel):
    full_video: Optional[FeatureToggle] = Field(default_factory=FeatureToggle)
    individual_segments: Optional[FeatureToggle] = Field(default_factory=FeatureToggle)
    merge_segments: Optional[FeatureToggle] = Field(default_factory=FeatureToggle)
    custom_segments: Optional[FeatureToggle] = Field(default_factory=FeatureToggle)


class VideoSplitWorkOrderCreate(BaseModel):
    """Request to split a video into segments."""
    schemaVersion: str = Field(default="1.0")
    repo_guid: str = Field(..., min_length=1, max_length=255)
    video_split_job_name: Optional[str] = Field(None, max_length=255)
    inputs:  ExportVideoInputs  = Field(default_factory=ExportVideoInputs)
    outputs: ExportVideoOutputs = Field(default_factory=ExportVideoOutputs)
    handle_seconds: float = Field(default=0.0, ge=0, description="Seconds to add before/after each segment")
    encoding: Optional[str] = Field(default="copy", description="'copy' for fast copy or codec like 'h264'")
    requested_by: Optional[str] = Field(None, max_length=255)


# ---------------------------------------------------------------------------
# Artifact / manifest
# ---------------------------------------------------------------------------

class ArtifactVideoSplitResponse(BaseModel):
    artifact_type: str
    format: str
    file_name: str
    file_path: str
    file_size: Optional[int] = None

    class Config:
        from_attributes = True


class VideoSplitManifest(BaseModel):
    split_job_id: UUID
    repo_guid:    str
    status:       str
    created_at:   datetime
    artifacts:    List[ArtifactVideoSplitResponse]


class ExportVideoSplitManifest(BaseModel):
    split_job_id: UUID
    repo_guid:    str
    status:       str
    created_at:   datetime
    artifacts:    List[ArtifactVideoSplitResponse]


# ---------------------------------------------------------------------------
# Job response
# ---------------------------------------------------------------------------

class VideoSplitJobResponse(BaseModel):
    """Full job detail response."""
    split_job_id:         UUID
    repo_guid:            str
    video_split_job_name: Optional[str] = None
    status:               str
    zip_file_path:        Optional[str] = None
    video_file_path:      str
    handle_seconds:       float
    encoding:             Optional[str] = None
    # work_order:           Optional[Dict[str, Any]] = None
    requested_by:         Optional[str] = None
    # results:              Optional[List[Dict[str, Any]]] = None
    segments_processed:   Optional[int] = None
    segments_successful:  Optional[int] = None
    segments_failed:      Optional[int] = None
    error_message:        Optional[str] = None
    error_details:        Optional[Dict[str, Any]] = None
    created_at:           datetime
    started_at:           Optional[datetime] = None
    completed_at:         Optional[datetime] = None
    manifest:             Optional[VideoSplitManifest] = None

    class Config:
        from_attributes = True


# ---------------------------------------------------------------------------
# Status response — per-section progress
# ---------------------------------------------------------------------------

class SectionProgressResponse(BaseModel):
    """Progress snapshot for one output section."""
    section:      str   # "full_video" | "individual_segments" | "merge_segments" | "custom_segments"
    label:        str   # human label, e.g. "Resizing full video (16x9)"
    progress:     int = Field(ge=0, le=100)   # 0-100 for this section only
    status:       str   # "running" | "done" | "failed"
    current_file: str = ""


class VideoSplitStatusResponse(BaseModel):
    """
    Lightweight status + live per-section progress response.

    overall_progress — rolled-up 0-100 across ALL active sections
    sections         — one entry per enabled output type with its own 0-100 progress
    """
    split_job_id:         str
    video_split_job_name: Optional[str] = None
    status:               str
    error_message:        Optional[str] = None
    created_at:           datetime
    started_at:           Optional[datetime] = None
    completed_at:         Optional[datetime] = None

    overall_progress: int = Field(default=0, ge=0, le=100,
                                  description="Average progress across all active sections (0-100)")
    sections: List[SectionProgressResponse] = Field(
        default_factory=list,
        description="Per-section progress — one entry per enabled output type"
    )


# ---------------------------------------------------------------------------
# List response
# ---------------------------------------------------------------------------

class VideoSplitJobListResponse(BaseModel):
    total: int
    jobs:  List[VideoSplitJobResponse]