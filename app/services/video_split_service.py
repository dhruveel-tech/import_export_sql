"""
Video Split Service - Video Segmentation Logic (SQLite)
"""
import json
from typing import List, Optional
from uuid import UUID, uuid4
from datetime import datetime

from sqlalchemy import select, desc

from app.db.session import AsyncSessionLocal
from app.schemas.video_split_schemas import (
    ExportVideoSplitManifest,
    VideoSplitWorkOrderCreate,
    VideoSplitJobResponse,
)
from app.models.models import JobStatus, VideoSplitJob
from app.core.config import settings
from app.core.logging_config import logger


class VideoSplitService:
    """Service for managing video split operations safely."""

    async def create_video_split_export_job(
        self, work_order: VideoSplitWorkOrderCreate
    ) -> Optional[VideoSplitJobResponse]:
        """Create a new video split job."""
        try:
            logger.info(f"Creating video split job for repo_guid={work_order.repo_guid}")
            output_folder = work_order.output_folder or settings.EXPORT_VIDEO_SPIT_PATH

            async with AsyncSessionLocal() as session:
                job = VideoSplitJob(
                    split_job_id=str(uuid4()),
                    repo_guid=work_order.repo_guid,
                    video_file_path=work_order.inputs.video_path,
                    handle_seconds=work_order.handle_seconds,
                    encoding=work_order.encoding,
                    output_folder=output_folder,
                    work_order=json.dumps(work_order.model_dump(mode="json")),
                    status=JobStatus.PENDING.value,
                )
                session.add(job)
                await session.commit()
                await session.refresh(job)

            return self._to_response(job)

        except Exception as e:
            logger.exception(f"Failed to create video split job : error={e}")
            return None

    async def get_video_split_export_job(
        self, split_job_id: UUID
    ) -> Optional[VideoSplitJobResponse]:
        try:
            async with AsyncSessionLocal() as session:
                result = await session.execute(
                    select(VideoSplitJob).where(VideoSplitJob.split_job_id == str(split_job_id))
                )
                job = result.scalar_one_or_none()
                if not job:
                    return None

                manifest = None
                if job.status == JobStatus.COMPLETED.value and job.manifest:
                    manifest = ExportVideoSplitManifest(**json.loads(job.manifest))

                response = self._to_response(job)
                response.manifest = manifest
                return response

        except Exception as e:
            logger.exception(f"Failed to fetch video split job : split_job_id={split_job_id} , error={e}")
            return None

    async def get_video_split_export_jobs_by_repo(
        self, repo_guid: str, limit: int = 100
    ) -> List[VideoSplitJobResponse]:
        try:
            async with AsyncSessionLocal() as session:
                result = await session.execute(
                    select(VideoSplitJob)
                    .where(VideoSplitJob.repo_guid == repo_guid)
                    .order_by(desc(VideoSplitJob.created_at))
                    .limit(limit)
                )
                jobs = result.scalars().all()
                return [self._to_response(job) for job in jobs]

        except Exception as e:
            logger.exception(f"Failed to list video split jobs for : repo_guid={repo_guid} , error={e}")
            return []

    async def update_video_split_job_status(
        self,
        split_job_id: UUID,
        status: JobStatus,
        error_message: Optional[str] = None,
        error_details: Optional[dict] = None,
    ) -> None:
        try:
            async with AsyncSessionLocal() as session:
                result = await session.execute(
                    select(VideoSplitJob).where(VideoSplitJob.split_job_id == str(split_job_id))
                )
                job = result.scalar_one_or_none()
                if not job:
                    logger.warning(f"Video split job not found while updating : split_job_id={split_job_id}")
                    return

                job.status = status.value
                job.error_message = error_message
                job.error_details = json.dumps(error_details) if error_details else None
                job.updated_at = datetime.utcnow()

                if status == JobStatus.PROCESSING and not job.started_at:
                    job.started_at = datetime.utcnow()
                elif status in [JobStatus.COMPLETED, JobStatus.FAILED]:
                    job.completed_at = datetime.utcnow()

                await session.commit()

            logger.info(f"Video split job status updated: split_job_id={split_job_id}, status={status}")

        except Exception as e:
            logger.exception(f"Failed to update video split job status : split_job_id={split_job_id} | error={e}")

    async def save_video_split_manifest(
        self, split_job_id: UUID, manifest: ExportVideoSplitManifest
    ) -> None:
        try:
            async with AsyncSessionLocal() as session:
                result = await session.execute(
                    select(VideoSplitJob).where(VideoSplitJob.split_job_id == str(split_job_id))
                )
                job = result.scalar_one_or_none()
                if not job:
                    logger.warning(f"Video split job not found while saving manifest split_job_id={split_job_id}")
                    return

                job.manifest = json.dumps(manifest.model_dump(mode="json"))
                job.updated_at = datetime.utcnow()
                await session.commit()

            logger.info(f"Video split manifest saved for split_job_id={split_job_id}")

        except Exception as e:
            logger.exception(f"Failed to save video split manifest split_job_id={split_job_id} | error={e}")

    @staticmethod
    def _to_response(job: VideoSplitJob) -> VideoSplitJobResponse:
        return VideoSplitJobResponse.model_validate({
            "split_job_id": job.split_job_id,
            "repo_guid": job.repo_guid,
            "video_file_path": job.video_file_path,
            "handle_seconds": job.handle_seconds,
            "encoding": job.encoding,
            "output_folder": job.output_folder,
            "work_order": json.loads(job.work_order) if job.work_order else {},
            "status": job.status,
            "zip_file_path": job.zip_file_path,
            "requested_by": job.requested_by,
            "results": json.loads(job.results) if job.results else None,
            "segments_processed": job.segments_processed,
            "segments_successful": job.segments_successful,
            "segments_failed": job.segments_failed,
            "manifest": json.loads(job.manifest) if job.manifest else None,
            "error_message": job.error_message,
            "error_details": json.loads(job.error_details) if job.error_details else None,
            "created_at": job.created_at,
            "started_at": job.started_at,
            "completed_at": job.completed_at,
            "updated_at": job.updated_at,
        })
