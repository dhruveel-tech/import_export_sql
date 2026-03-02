"""
Video Split Schemas - Request and Response Models
"""
from typing import Optional, List
from uuid import UUID
from datetime import datetime
from pydantic import BaseModel, Field, field_validator



class ExportVideoInputs(BaseModel):
    """Export input data selection."""
    event_ids: List[str] = Field(default_factory=list)
    video_path: str = Field(..., min_length=1, description="Absolute path of source video")

class FeatureToggle(BaseModel):
    is_enabled: bool = False


class ResizeVideoConfig(BaseModel):
    is_enabled: bool = False
    width: Optional[int] = None
    height: Optional[int] = None


class ExportVideoOutputs(BaseModel):
    """Export output configuration."""
    full_video: Optional[FeatureToggle] = None
    individual_segments: Optional[FeatureToggle] = None
    merge_segments: Optional[FeatureToggle] = None
    resize_video: Optional[ResizeVideoConfig] = None
    
class VideoSplitWorkOrderCreate(BaseModel):
    """Request to split a video into segments."""
    schemaVersion: str = Field(default="1.0")
    repo_guid: str = Field(..., min_length=1, max_length=255)
    inputs: ExportVideoInputs = Field(default_factory=ExportVideoInputs)
    outputs: ExportVideoOutputs = Field(default_factory=ExportVideoOutputs)
    handle_seconds: float = Field(default=0.0, ge=0, description="Number of seconds to add before/after each segment (handles)")
    output_folder: Optional[str] = Field(None, description="Output folder path")
    encoding: Optional[str] = Field(default="copy", description="Encoding method: 'copy' for fast copy, or codec like 'h264'")
    requested_by: Optional[str] = Field(None, max_length=255)
    
    model_config = {
        "json_schema_extra": {
            "example": {
                "schemaVersion": "1.0",
                "repo_guid": "repo-123",
                "inputs": {
                    "event_ids": [
                        "event-001",
                        "event-002"
                    ],
                    "video_path": "/media/videos/interview.mp4"
                },
                "outputs": {
                    "full_video": {
                        "is_enabled": True
                    },
                    "individual_segments": {
                        "is_enabled": True
                    },
                    "merge_segments": {
                        "is_enabled": True
                    },
                    "resize_video": {
                        "is_enabled": True,
                        "width": 1280,
                        "height": 720
                    }
                },
                "handle_seconds": 2.0,
                "output_folder": "/media/exports",
                "encoding": "copy",
                "requested_by": "editor@company.com"
            }
        }
    }

class ArtifactVideoSplitResponse(BaseModel):
    """Response schema for an artifact."""
    artifact_type: str
    format: str
    file_name: str
    file_path: str
    file_size: Optional[int] = None

    class Config:
        from_attributes = True
        
class VideoSplitManifest(BaseModel):
    """Export package manifest."""
    split_job_id: UUID
    repo_guid: str
    status: str
    created_at: datetime
    artifacts: List[ArtifactVideoSplitResponse]
    
class VideoSplitJobResponse(BaseModel):
    """Response for video split job."""
    split_job_id: UUID
    repo_guid: str
    status: str  
    zip_file_path: Optional[str] = None
    video_file_path: str
    handle_seconds: float
    error_message: Optional[str] = None
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    manifest: Optional[VideoSplitManifest]
    
    class Config:
        from_attributes = True

class ExportVideoSplitManifest(BaseModel):
    """Export package manifest."""
    split_job_id: UUID
    repo_guid: str
    status: str
    created_at: datetime
    artifacts: List[ArtifactVideoSplitResponse]

class VideoSplitStatusResponse(BaseModel):
    """Status response for video split job."""
    split_job_id: str
    status: str
    error_message: Optional[str] = None
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

class VideoSplitJobListResponse(BaseModel):
    total: int
    jobs: List[VideoSplitJobResponse]