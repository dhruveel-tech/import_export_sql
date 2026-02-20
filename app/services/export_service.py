"""
Export Service - Business Logic for Export Operations (SQLite)
"""
import json
from datetime import datetime
from typing import List, Optional
from uuid import UUID, uuid4
from pathlib import Path

from sqlalchemy import select, desc
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import AsyncSessionLocal
from app.models.models import ExportJob, Artifact, JobStatus, ExportMode
from app.schemas.export_schemas import (
    ExportWorkOrderCreate,
    ExportJobResponse,
    ExportManifest,
    GetLLmPromptResponse,
    UpdateLLmPromptResponse,
)
from app.core.logging_config import logger
from app.core.config import settings


class ExportService:
    """Service for managing export operations with SQLite."""

    # ------------------------------------------------------------------
    # CREATE EXPORT JOB
    # ------------------------------------------------------------------
    async def create_export_job(
        self, work_order: ExportWorkOrderCreate
    ) -> Optional[ExportJobResponse]:
        """Create a new export job."""
        try:
            logger.info("-------------------------------  EXPORT JOB -------------------------------")
            logger.info(
                f"Creating export job : repo_guid={work_order.repo_guid} , "
                f"export_mode={work_order.metadata.export_mode}"
            )

            valid = await self._validate_edl_usage(work_order)
            if not valid:
                logger.error("EDL validation failed")
                return None

            async with AsyncSessionLocal() as session:
                job = ExportJob(
                    export_id=str(uuid4()),
                    repo_guid=work_order.repo_guid,
                    export_mode=work_order.metadata.export_mode,
                    export_preset=work_order.metadata.export_preset,
                    work_order=json.dumps(work_order.model_dump(mode="json")),
                    requested_by=work_order.metadata.requested_by,
                    status=JobStatus.PENDING.value,
                )
                session.add(job)
                await session.commit()
                await session.refresh(job)

            logger.info(f"Export job created : export_id={job.export_id} , status={job.status}")
            return self._to_response(job)

        except Exception as exc:
            logger.error(f"Failed to create export job : error={exc}", exc_info=True)
            return None

    # ------------------------------------------------------------------
    # VALIDATE EDL USAGE
    # ------------------------------------------------------------------
    async def _validate_edl_usage(self, work_order: ExportWorkOrderCreate) -> bool:
        try:
            outputs = work_order.outputs
            if outputs.transcript and "edl" in outputs.transcript.formats:
                logger.error("EDL format not allowed for transcript")
                return False
            return True
        except Exception as exc:
            logger.error(f"EDL validation crashed : error={exc}", exc_info=True)
            return False

    # ------------------------------------------------------------------
    # GET SINGLE EXPORT JOB
    # ------------------------------------------------------------------
    async def get_export_job(self, export_id: UUID) -> Optional[ExportJobResponse]:
        try:
            async with AsyncSessionLocal() as session:
                result = await session.execute(
                    select(ExportJob).where(ExportJob.export_id == str(export_id))
                )
                job = result.scalar_one_or_none()
                if not job:
                    return None

                manifest = None
                if job.status == JobStatus.COMPLETED.value and job.manifest:
                    manifest = ExportManifest(**json.loads(job.manifest))

                response = self._to_response(job)
                response.manifest = manifest
                return response

        except Exception as exc:
            logger.error(f"Failed to fetch export job : export_id={export_id} , error={exc}", exc_info=True)
            return None

    # ------------------------------------------------------------------
    # LIST EXPORT JOBS
    # ------------------------------------------------------------------
    async def get_export_jobs_by_repo(
        self, repo_guid: str, limit: int = 100
    ) -> List[ExportJobResponse]:
        try:
            async with AsyncSessionLocal() as session:
                result = await session.execute(
                    select(ExportJob)
                    .where(ExportJob.repo_guid == repo_guid)
                    .order_by(desc(ExportJob.created_at))
                    .limit(limit)
                )
                jobs = result.scalars().all()
                return [self._to_response(job) for job in jobs]

        except Exception as exc:
            logger.error(f"Failed to list export jobs : repo_guid={repo_guid} , error={exc}", exc_info=True)
            return []

    # ------------------------------------------------------------------
    # UPDATE JOB STATUS
    # ------------------------------------------------------------------
    async def update_job_status(
        self,
        export_id: UUID,
        status: JobStatus,
        error_message: Optional[str] = None,
        error_details: Optional[dict] = None,
    ) -> bool:
        try:
            async with AsyncSessionLocal() as session:
                result = await session.execute(
                    select(ExportJob).where(ExportJob.export_id == str(export_id))
                )
                job = result.scalar_one_or_none()
                if not job:
                    logger.error(f"Export job not found : export_id={export_id}")
                    return False

                job.status = status.value
                job.error_message = error_message  # store as plain string
                job.error_details = json.dumps(error_details) if error_details else None
                job.updated_at = datetime.utcnow()

                if status == JobStatus.PROCESSING and not job.started_at:
                    job.started_at = datetime.utcnow()
                elif status in [JobStatus.COMPLETED, JobStatus.FAILED]:
                    job.completed_at = datetime.utcnow()

                await session.commit()

            logger.info(f"Export job status updated : export_id={export_id} , status={status}")
            return True

        except Exception as exc:
            logger.error(f"Failed to update job status : export_id={export_id} , error={exc}", exc_info=True)
            return False

    # ------------------------------------------------------------------
    # SAVE MANIFEST
    # ------------------------------------------------------------------
    async def save_manifest(self, export_id: UUID, manifest: ExportManifest) -> bool:
        try:
            async with AsyncSessionLocal() as session:
                result = await session.execute(
                    select(ExportJob).where(ExportJob.export_id == str(export_id))
                )
                job = result.scalar_one_or_none()
                if not job:
                    logger.error(f"Export job not found for manifest : export_id={export_id}")
                    return False

                job.manifest = json.dumps(manifest.model_dump(mode="json"))
                job.updated_at = datetime.utcnow()
                await session.commit()

            logger.info(f"Export manifest saved : export_id={export_id}")
            return True

        except Exception as exc:
            logger.error(f"Failed to save export manifest : export_id={export_id} , error={exc}", exc_info=True)
            return False

    # ------------------------------------------------------------------
    # SAVE ARTIFACTS
    # ------------------------------------------------------------------
    async def save_artifacts(self, export_job_id: UUID, artifacts: List[dict]) -> bool:
        try:
            if not artifacts:
                return True

            async with AsyncSessionLocal() as session:
                for artifact_data in artifacts:
                    artifact = Artifact(
                        export_job_id=str(export_job_id),
                        artifact_type=artifact_data.get("artifact_type", ""),
                        format=artifact_data.get("format", ""),
                        filename=artifact_data.get("filename", ""),
                        file_path=artifact_data.get("file_path", artifact_data.get("filename", "")),
                        url=artifact_data.get("url", ""),
                        file_size=artifact_data.get("file_size"),
                        checksum=artifact_data.get("checksum"),
                    )
                    session.add(artifact)
                await session.commit()

            logger.info(f"Artifacts saved : export_job_id={export_job_id} , count={len(artifacts)}")
            return True

        except Exception as exc:
            logger.error(f"Failed to save artifacts : export_job_id={export_job_id} , error={exc}", exc_info=True)
            return False
        
    # ------------------------------------------------------------------
    # LLM PROMPT (file-based, unchanged)
    # ------------------------------------------------------------------
    async def update_llm_prompt(self, export_job_id: UUID, prompt: str) -> UpdateLLmPromptResponse:
        output_folder_path: Path = Path(settings.EXPORT_BASE_PATH)
        job_folder = output_folder_path / str(export_job_id)
        llm_prompt_file_path = job_folder / "sdna_ai_spark_llm_instructions.md"

        try:
            job_folder.mkdir(parents=True, exist_ok=True)
            with open(llm_prompt_file_path, "w", encoding="utf-8") as f:
                f.write(prompt)

            return UpdateLLmPromptResponse(
                export_id=export_job_id,
                status="success",
                file_path=str(llm_prompt_file_path),
                error_message=None,
            )

        except Exception as e:
            logger.error(f"Failed to update LLM prompt for {export_job_id}: {e}")
            return UpdateLLmPromptResponse(
                export_id=export_job_id,
                status="failed",
                file_path=str(llm_prompt_file_path),
                error_message=str(e),
            )

    async def get_llm_prompt(self, export_job_id: UUID) -> GetLLmPromptResponse:
        output_folder_path: Path = Path(settings.EXPORT_BASE_PATH)
        job_folder = output_folder_path / str(export_job_id)
        llm_prompt_file_path = job_folder / "sdna_ai_spark_llm_instructions.md"

        try:
            job_folder.mkdir(parents=True, exist_ok=True)
            if not llm_prompt_file_path.exists():
                with open(llm_prompt_file_path, "w", encoding="utf-8") as f:
                    f.write("")

            return GetLLmPromptResponse(
                export_id=export_job_id,
                status="success",
                file_path=str(llm_prompt_file_path),
                error_message=None,
            )

        except Exception as e:
            logger.error(f"Failed to fetch LLM instruct file for {export_job_id}: {e}")
            return GetLLmPromptResponse(
                export_id=export_job_id,
                status="failed",
                file_path=str(llm_prompt_file_path),
                error_message=str(e),
            )

    # ------------------------------------------------------------------
    # HELPER
    # ------------------------------------------------------------------
    @staticmethod
    def _to_response(job: ExportJob) -> ExportJobResponse:
        return ExportJobResponse.model_validate({
            "export_id": job.export_id,
            "repo_guid": job.repo_guid,
            "export_mode": job.export_mode,
            "export_preset": job.export_preset,
            "work_order": json.loads(job.work_order) if job.work_order else {},
            "status": job.status,
            "zip_file_path": job.zip_file_path,
            "requested_by": job.requested_by,
            "export_path": job.export_path,
            "manifest": json.loads(job.manifest) if job.manifest else None,
            "error_message": ExportService._parse_error_message(job.error_message),
            "error_details": json.loads(job.error_details) if job.error_details else None,
            "created_at": job.created_at,
            "started_at": job.started_at,
            "completed_at": job.completed_at,
            "updated_at": job.updated_at,
        })

    @staticmethod
    def _parse_error_message(raw: str | None) -> str | None:
        """Convert stored error_message (may be JSON list or plain string) to Optional[str]."""
        if not raw:
            return None
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                joined = " | ".join(str(e) for e in parsed if e)
                return joined if joined else None
            if isinstance(parsed, str):
                return parsed or None
            return str(parsed)
        except (json.JSONDecodeError, TypeError):
            return raw or None