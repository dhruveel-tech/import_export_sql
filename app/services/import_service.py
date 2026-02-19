"""
Import Service - Business Logic for Import Operations (SQLite)
"""
import json
from datetime import datetime
from typing import Optional, List
from uuid import UUID, uuid4

from sqlalchemy import select, desc

from app.db.session import AsyncSessionLocal
from app.models.models import ImportHighlight, ImportLLmJob, JobStatus
from app.schemas.import_schemas import (
    ImportJobResponse,
    ValidationError,
    ImportWorkOrder,
    Highlight,
)
from app.core.logging_config import logger


class ImportService:
    """Service for managing LLM highlight import operations."""

    # ------------------------------------------------------------------
    # CREATE IMPORT JOB
    # ------------------------------------------------------------------
    async def create_import_job(
        self, work_order: ImportWorkOrder,
    ) -> Optional[ImportJobResponse]:
        """Create a new Custom Timeline Event import job."""
        try:
            logger.info("---------------- LLM IMPORT JOB ----------------")

            validation_errors = self._validate_highlights(work_order.highlights)

            async with AsyncSessionLocal() as session:
                import_id = str(uuid4())
                asset_json = json.dumps(work_order.asset.model_dump(mode="json"))

                if validation_errors:
                    job = ImportLLmJob(
                        import_id=import_id,
                        asset=asset_json,
                        status=JobStatus.FAILED.value,
                        validation_errors=json.dumps([e.model_dump() for e in validation_errors]),
                        error_message="Validation failed",
                    )
                else:
                    job = ImportLLmJob(
                        import_id=import_id,
                        asset=asset_json,
                        status=JobStatus.PENDING.value,
                    )

                session.add(job)
                await session.flush()  # get PK before adding highlights

                # Insert highlights
                for h in work_order.highlights:
                    h_obj = h if isinstance(h, Highlight) else Highlight(**h)
                    highlight = ImportHighlight(
                        import_id=import_id,
                        insight=h_obj.insight,
                        start=h_obj.start,
                        end=h_obj.end,
                        confidence_score=h_obj.confidenceScore if h_obj.confidenceScore is not None else 0,
                        event_meta=json.dumps(h_obj.eventMeta.model_dump() if h_obj.eventMeta else None),
                    )
                    session.add(highlight)

                await session.commit()
                await session.refresh(job)

            logger.info(
                f"LLM import job created : import_id={job.import_id} , "
                f"status={job.status} , validation_errors={len(validation_errors)}"
            )

            asset = json.loads(job.asset)
            return ImportJobResponse(
                import_id=job.import_id,
                repo_guid=asset.get("repo_guid"),
                status=job.status,
                items_processed=job.items_processed,
                items_created=job.items_created,
                items_updated=job.items_updated,
                items_skipped=job.items_skipped,
                validation_errors=json.loads(job.validation_errors) if job.validation_errors else None,
                error_message=job.error_message,
                created_at=job.created_at,
                started_at=job.started_at,
                completed_at=job.completed_at,
            )

        except Exception as e:
            logger.exception(f"Failed to create LLM import job : error={e}")
            return None

    # ------------------------------------------------------------------
    # VALIDATION
    # ------------------------------------------------------------------
    def _validate_highlights(self, highlights: List[Highlight]) -> List[ValidationError]:
        errors: List[ValidationError] = []
        try:
            highlights = [
                Highlight(**h) if isinstance(h, dict) else h
                for h in highlights
            ]
            for i, h in enumerate(highlights):
                if h.start < 0:
                    errors.append(ValidationError(
                        field=f"highlights[{i}].start",
                        message="start timestamp cannot be negative",
                        value=h.start,
                    ))
                if h.end <= h.start:
                    errors.append(ValidationError(
                        field=f"highlights[{i}]",
                        message="end must be greater than start",
                        value=h.model_dump(),
                    ))
                if h.confidenceScore is not None and h.confidenceScore == 0:
                    errors.append(ValidationError(
                        field=f"highlights[{i}].confidence",
                        message="confidence must be > 0 for LLM highlights",
                        value=h.confidenceScore,
                    ))
        except Exception as e:
            logger.exception(f"Highlight validation failure : error={e}")
            errors.append(ValidationError(
                field="highlights",
                message="Unexpected validation failure",
                value=str(e),
            ))
        return errors

    # ------------------------------------------------------------------
    # GET JOB
    # ------------------------------------------------------------------
    async def get_import_job(self, import_id: UUID) -> Optional[ImportJobResponse]:
        try:
            async with AsyncSessionLocal() as session:
                result = await session.execute(
                    select(ImportLLmJob).where(ImportLLmJob.import_id == str(import_id))
                )
                job = result.scalar_one_or_none()
                if not job:
                    return None

                asset = json.loads(job.asset)
                return ImportJobResponse(
                    import_id=job.import_id,
                    repo_guid=asset.get("repo_guid"),
                    status=job.status,
                    items_processed=job.items_processed,
                    items_created=job.items_created,
                    items_updated=job.items_updated,
                    items_skipped=job.items_skipped,
                    validation_errors=json.loads(job.validation_errors) if job.validation_errors else None,
                    error_message=job.error_message,
                    created_at=job.created_at,
                    started_at=job.started_at,
                    completed_at=job.completed_at,
                )

        except Exception as e:
            logger.exception(f"Failed to fetch LLM import job : id={import_id} , error={e}")
            return None

    # ------------------------------------------------------------------
    # UPDATE STATUS
    # ------------------------------------------------------------------
    async def update_job_status(
        self,
        import_id: UUID,
        status: JobStatus,
        items_processed: int = 0,
        items_created: int = 0,
        items_updated: int = 0,
        items_skipped: int = 0,
        error_message: Optional[str] = None,
        error_details: Optional[dict] = None,
    ) -> None:
        try:
            async with AsyncSessionLocal() as session:
                result = await session.execute(
                    select(ImportLLmJob).where(ImportLLmJob.import_id == str(import_id))
                )
                job = result.scalar_one_or_none()
                if not job:
                    logger.warning(f"LLM import job not found : id={import_id}")
                    return

                job.status = status.value
                job.items_processed = items_processed
                job.items_created = items_created
                job.items_updated = items_updated
                job.items_skipped = items_skipped
                job.error_message = error_message
                job.error_details = json.dumps(error_details) if error_details else None
                job.updated_at = datetime.utcnow()

                if status == JobStatus.PROCESSING and not job.started_at:
                    job.started_at = datetime.utcnow()
                elif status in (JobStatus.COMPLETED, JobStatus.FAILED):
                    job.completed_at = datetime.utcnow()

                await session.commit()

            logger.info(f"LLM import job updated : id={import_id} , status={status}")

        except Exception as e:
            logger.exception(f"Failed to update LLM import job : id={import_id} , error={e}")

    # ------------------------------------------------------------------
    # LIST IMPORT JOBS
    # ------------------------------------------------------------------
    async def get_import_jobs_by_repo(
        self, repo_guid: str, limit: int = 100
    ) -> List[ImportJobResponse]:
        try:
            async with AsyncSessionLocal() as session:
                result = await session.execute(
                    select(ImportLLmJob)
                    .order_by(desc(ImportLLmJob.created_at))
                    .limit(limit)
                )
                jobs = result.scalars().all()

                out = []
                for job in jobs:
                    asset = json.loads(job.asset) if job.asset else {}
                    if asset.get("repo_guid") != repo_guid:
                        continue
                    out.append(ImportJobResponse(
                        import_id=job.import_id,
                        repo_guid=asset.get("repo_guid"),
                        status=job.status,
                        items_processed=job.items_processed,
                        items_created=job.items_created,
                        items_updated=job.items_updated,
                        items_skipped=job.items_skipped,
                        validation_errors=json.loads(job.validation_errors) if job.validation_errors else None,
                        error_message=job.error_message,
                        created_at=job.created_at,
                        started_at=job.started_at,
                        completed_at=job.completed_at,
                    ))
                return out

        except Exception as exc:
            logger.error(f"Failed to list import jobs : repo_guid={repo_guid} , error={exc}", exc_info=True)
            return []
