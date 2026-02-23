"""
SQLite ORM Models using SQLAlchemy
"""
import json
from datetime import datetime
from enum import Enum
from uuid import uuid4

from sqlalchemy import (
    Column, String, Float, Integer, DateTime, Text, Boolean, Enum as SAEnum
)
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class JobStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ExportMode(str, Enum):
    EDITORIAL = "editorial"
    LLM = "llm"
    REVIEW = "review"
    PROMO = "promo"
    CUSTOM = "custom"


class ImportType(str, Enum):
    EVENTS = "events"
    COMMENTS = "comments"
    MARKERS = "markers"
    TRANSCRIPT = "transcript"


# ---------------------------------------------------------------------------
# Helper: store dicts/lists as JSON text
# ---------------------------------------------------------------------------

def _to_json(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return json.dumps(value)


def _from_json(value: str | None):
    if value is None:
        return None
    try:
        return json.loads(value)
    except Exception:
        return value


# ---------------------------------------------------------------------------
# ExportJob
# ---------------------------------------------------------------------------

class ExportJob(Base):
    __tablename__ = "export_jobs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    export_id = Column(String, unique=True, index=True, default=lambda: str(uuid4()))
    repo_guid = Column(String, index=True, nullable=False)
    export_mode = Column(String, default=ExportMode.EDITORIAL.value)
    export_preset = Column(String, nullable=True)
    work_order = Column(Text, nullable=False)          # JSON
    status = Column(String, index=True, default=JobStatus.PENDING.value)
    zip_file_path = Column(String, nullable=True)
    requested_by = Column(String, nullable=True)
    export_path = Column(String, nullable=True)
    manifest = Column(Text, nullable=True)             # JSON
    error_message = Column(Text, nullable=True)        # JSON list
    error_details = Column(Text, nullable=True)        # JSON
    created_at = Column(DateTime, default=datetime.utcnow)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # ------------------------------------------------------------------
    # Helpers for JSON fields
    # ------------------------------------------------------------------
    def set_work_order(self, value): self.work_order = _to_json(value)
    def get_work_order(self): return _from_json(self.work_order)
    def set_manifest(self, value): self.manifest = _to_json(value)
    def get_manifest(self): return _from_json(self.manifest)
    def set_error_message(self, value): self.error_message = _to_json(value)
    def get_error_message(self): return _from_json(self.error_message)
    def set_error_details(self, value): self.error_details = _to_json(value)
    def get_error_details(self): return _from_json(self.error_details)

    def to_dict(self):
        return {
            "export_id": self.export_id,
            "repo_guid": self.repo_guid,
            "export_mode": self.export_mode,
            "export_preset": self.export_preset,
            "work_order": _from_json(self.work_order),
            "status": self.status,
            "zip_file_path": self.zip_file_path,
            "requested_by": self.requested_by,
            "export_path": self.export_path,
            "manifest": _from_json(self.manifest),
            "error_message": _from_json(self.error_message),
            "error_details": _from_json(self.error_details),
            "created_at": self.created_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "updated_at": self.updated_at,
        }


# ---------------------------------------------------------------------------
# Artifact
# ---------------------------------------------------------------------------

class Artifact(Base):
    __tablename__ = "artifacts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    export_job_id = Column(String, index=True, nullable=False)
    artifact_type = Column(String, nullable=False)
    format = Column(String, nullable=False)
    filename = Column(String, nullable=False)
    file_path = Column(String, nullable=False)
    url = Column(String, nullable=False)
    file_size = Column(Integer, nullable=True)
    checksum = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    def to_dict(self):
        return {
            "export_job_id": self.export_job_id,
            "artifact_type": self.artifact_type,
            "format": self.format,
            "filename": self.filename,
            "file_path": self.file_path,
            "url": self.url,
            "file_size": self.file_size,
            "checksum": self.checksum,
            "created_at": self.created_at,
        }


# ---------------------------------------------------------------------------
# ImportLLmJob
# ---------------------------------------------------------------------------

class ImportLLmJob(Base):
    __tablename__ = "import_jobs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    import_id = Column(String, unique=True, index=True, default=lambda: str(uuid4()))
    asset = Column(Text, nullable=False)               # JSON {repo_guid, fullPath}
    validation_errors = Column(Text, nullable=True)    # JSON
    status = Column(String, index=True, default=JobStatus.PENDING.value)
    submitted_by = Column(String, nullable=True)
    items_processed = Column(Integer, default=0)
    items_created = Column(Integer, default=0)
    items_updated = Column(Integer, default=0)
    items_skipped = Column(Integer, default=0)
    error_message = Column(Text, nullable=True)
    error_details = Column(Text, nullable=True)        # JSON
    created_at = Column(DateTime, default=datetime.utcnow)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def set_asset(self, value): self.asset = _to_json(value)
    def get_asset(self): return _from_json(self.asset)
    def set_validation_errors(self, value): self.validation_errors = _to_json(value)
    def get_validation_errors(self): return _from_json(self.validation_errors)
    def set_error_details(self, value): self.error_details = _to_json(value)
    def get_error_details(self): return _from_json(self.error_details)

    def to_dict(self):
        return {
            "import_id": self.import_id,
            "asset": _from_json(self.asset),
            "validation_errors": _from_json(self.validation_errors),
            "status": self.status,
            "submitted_by": self.submitted_by,
            "items_processed": self.items_processed,
            "items_created": self.items_created,
            "items_updated": self.items_updated,
            "items_skipped": self.items_skipped,
            "error_message": self.error_message,
            "error_details": _from_json(self.error_details),
            "created_at": self.created_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "updated_at": self.updated_at,
        }


# ---------------------------------------------------------------------------
# ImportHighlight
# ---------------------------------------------------------------------------

class ImportHighlight(Base):
    __tablename__ = "import_highlights"

    id = Column(Integer, primary_key=True, autoincrement=True)
    import_id = Column(String, index=True, nullable=False)
    insight = Column(Text, nullable=False)
    start = Column(Float, nullable=False)
    end = Column(Float, nullable=False)
    confidence_score = Column(Integer, nullable=False)
    event_meta = Column(Text, nullable=True)           # JSON
    created_at = Column(DateTime, default=datetime.utcnow)

    def set_event_meta(self, value): self.event_meta = _to_json(value)
    def get_event_meta(self): return _from_json(self.event_meta)

    def to_dict(self):
        return {
            "import_id": self.import_id,
            "insight": self.insight,
            "start": self.start,
            "end": self.end,
            "confidenceScore": self.confidence_score,
            "eventMeta": _from_json(self.event_meta),
            "created_at": self.created_at,
        }


# ---------------------------------------------------------------------------
# VideoSplitJob
# ---------------------------------------------------------------------------

class VideoSplitJob(Base):
    __tablename__ = "video_split_jobs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    split_job_id = Column(String, unique=True, index=True, default=lambda: str(uuid4()))
    repo_guid = Column(String, index=True, nullable=False)
    video_file_path = Column(String, nullable=False)
    handle_seconds = Column(Float, default=0.0)
    encoding = Column(String, default="copy")
    output_folder = Column(String, nullable=False)
    work_order = Column(Text, nullable=False)          # JSON
    status = Column(String, index=True, default=JobStatus.PENDING.value)
    zip_file_path = Column(String, nullable=True)
    requested_by = Column(String, nullable=True)
    results = Column(Text, nullable=True)              # JSON list
    segments_processed = Column(Integer, default=0)
    segments_successful = Column(Integer, default=0)
    segments_failed = Column(Integer, default=0)
    manifest = Column(Text, nullable=True)             # JSON
    error_message = Column(Text, nullable=True)
    error_details = Column(Text, nullable=True)        # JSON
    created_at = Column(DateTime, default=datetime.utcnow)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def set_work_order(self, value): self.work_order = _to_json(value)
    def get_work_order(self): return _from_json(self.work_order)
    def set_manifest(self, value): self.manifest = _to_json(value)
    def get_manifest(self): return _from_json(self.manifest)
    def set_results(self, value): self.results = _to_json(value)
    def get_results(self): return _from_json(self.results)
    def set_error_details(self, value): self.error_details = _to_json(value)
    def get_error_details(self): return _from_json(self.error_details)

    def to_dict(self):
        return {
            "split_job_id": self.split_job_id,
            "repo_guid": self.repo_guid,
            "video_file_path": self.video_file_path,
            "handle_seconds": self.handle_seconds,
            "encoding": self.encoding,
            "output_folder": self.output_folder,
            "work_order": _from_json(self.work_order),
            "status": self.status,
            "zip_file_path": self.zip_file_path,
            "requested_by": self.requested_by,
            "results": _from_json(self.results),
            "segments_processed": self.segments_processed,
            "segments_successful": self.segments_successful,
            "segments_failed": self.segments_failed,
            "manifest": _from_json(self.manifest),
            "error_message": self.error_message,
            "error_details": _from_json(self.error_details),
            "created_at": self.created_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "updated_at": self.updated_at,
        }
