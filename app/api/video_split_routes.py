"""
Video Split API Routes (SQLite)
"""
import traceback
from uuid import UUID

from fastapi import APIRouter, Query, BackgroundTasks
from fastapi.responses import JSONResponse
from sqlalchemy import select

from app.db.session import AsyncSessionLocal
from app.schemas.video_split_schemas import (
    VideoSplitWorkOrderCreate,
    VideoSplitJobResponse,
    VideoSplitStatusResponse,
    VideoSplitJobListResponse,
)
from app.models.models import VideoSplitJob, JobStatus
from app.services.video_split_service import VideoSplitService
from app.background.tasks import process_video_split_task
from app.core.logging_config import logger

router = APIRouter()


@router.post("", response_model=VideoSplitJobResponse, status_code=202)
async def create_video_split(
    work_order: VideoSplitWorkOrderCreate,
    background_tasks: BackgroundTasks,
):
    """Create a new video split job."""
    service = VideoSplitService()

    try:
        logger.info("-------------------------------  VIDEO SPLIT JOB -------------------------------")
        logger.info("")
        logger.info(f"Received video split work order : repo_guid={work_order.repo_guid}")
        job = await service.create_video_split_export_job(work_order)
        logger.info(f"Created video split job : split_job_id={job.split_job_id}")

        background_tasks.add_task(process_video_split_task, str(job.split_job_id))

        return job

    except ValueError as e:
        logger.error(f"Validation error : error={str(e)} , repo_guid={work_order.repo_guid}")
        return JSONResponse(
            status_code=400,
            content={
                "status": "error",
                "message": f"Failed to Create Video Spkit Job : {str(e)}"
            }
        )

    except Exception as e:
        logger.error(f"Failed to create video split job : error={str(e)} , repo_guid={work_order.repo_guid}")
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "message": f"Failed to create video split job: {str(e)}"
            }
        )


@router.get("/{split_job_id}/status", response_model=VideoSplitStatusResponse)
async def get_video_split_status(split_job_id: str):
    """Get the status of a video split job."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(VideoSplitJob).where(VideoSplitJob.split_job_id == split_job_id)
        )
        job = result.scalar_one_or_none()

    if not job:
        return JSONResponse(
            status_code=404,
            content={
                "status": "error",
                "message": f"Video split job not found: {split_job_id}"
            }
        )

    return VideoSplitStatusResponse(
        split_job_id=job.split_job_id,
        status=job.status,
        error_message=job.error_message,
        created_at=job.created_at,
        started_at=job.started_at,
        completed_at=job.completed_at,
    )


@router.get("/{split_job_id}", response_model=VideoSplitJobResponse)
async def get_video_split(split_job_id: str):
    """Get complete video split job details."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(VideoSplitJob).where(VideoSplitJob.split_job_id == split_job_id)
        )
        job = result.scalar_one_or_none()

    if not job:
        return JSONResponse(
            status_code=404,
            content={
                "status": "error",
                "message": f"Video split job not found: {split_job_id}"
            }
        )

    return VideoSplitService._to_response(job)


@router.get("", response_model=VideoSplitJobListResponse)
async def list_video_split_jobs(
    repo_guid: str = Query(..., description="Repository ID to filter by"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of results"),
):
    """List video split jobs for a repository."""
    service = VideoSplitService()

    try:
        jobs = await service.get_video_split_export_jobs_by_repo(repo_guid, limit)
        return VideoSplitJobListResponse(total=len(jobs), jobs=jobs)

    except Exception as exc:
        logger.error(
            f"Failed to list export jobs : repo_guid={repo_guid} , limit={limit} , error={str(exc)} , traceback={traceback.format_exc()}"
        )
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "message": "Failed to list export jobs"
            }
        )
