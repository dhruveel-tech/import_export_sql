"""
Video Split API Routes — fixed for Windows progress tracking
Changes:
  - status endpoint always reads progress_tracker (not just when status=PROCESSING)
  - Falls back safely to {0, "", ""} for unknown job IDs (pending / new jobs)
"""
import traceback

from fastapi import APIRouter, Query, BackgroundTasks
from fastapi.responses import JSONResponse
from sqlalchemy import select

from app.core.progress_tracker import progress_tracker
from app.db.session import AsyncSessionLocal
from app.models.models import JobStatus, VideoSplitJob
from app.schemas.video_split_schemas import (
    VideoSplitJobListResponse,
    VideoSplitJobResponse,
    VideoSplitStatusResponse,
    VideoSplitWorkOrderCreate,
)
from app.services.video_split_service import VideoSplitService
from app.background.tasks import process_video_split_task
from app.core.logging_config import logger
from app.schemas.video_split_schemas import SectionProgressResponse

router = APIRouter()


@router.post("", response_model=VideoSplitJobResponse, status_code=202)
async def create_video_split(
    work_order: VideoSplitWorkOrderCreate,
    background_tasks: BackgroundTasks,
):
    service = VideoSplitService()
    try:
        logger.info("---------- VIDEO SPLIT JOB ----------")
        logger.info(f"Received work order : repo_guid={work_order.repo_guid}")
        job = await service.create_video_split_export_job(work_order)
        logger.info(f"Created job : split_job_id={job.split_job_id}")
        background_tasks.add_task(process_video_split_task, str(job.split_job_id))
        return job
    except ValueError as e:
        return JSONResponse(status_code=400, content={"status": "error", "message": str(e)})
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "error", "message": str(e)})


@router.get("/{split_job_id}/status", response_model=VideoSplitStatusResponse)
async def get_video_split_status(split_job_id: str):
    """
    Returns job status and live per-section progress.
 
    Poll every 1-2 seconds while status == "processing".
 
    Response shape:
    {
      "status": "processing",
      "overall_progress": 54,
      "sections": [
        {"section": "full_video",      "label": "Resizing full video (16x9)",
         "progress": 71, "status": "running", "current_file": "...mp4"},
        {"section": "custom_segments", "label": "Exporting Custom clip 4/10",
         "progress": 38, "status": "running", "current_file": "...mp4"}
      ]
    }
    """
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(VideoSplitJob).where(VideoSplitJob.split_job_id == split_job_id)
        )
        job = result.scalar_one_or_none()
 
    if not job:
        return JSONResponse(
            status_code=404,
            content={"status": "error", "message": f"Job not found: {split_job_id}"},
        )
 
    db_status = job.status
 
    if db_status == JobStatus.FAILED.value:
        return VideoSplitStatusResponse(
            split_job_id=job.split_job_id,
            video_split_job_name=job.video_split_job_name,
            status=db_status,
            error_message=job.error_message,
            created_at=job.created_at,
            started_at=job.started_at,
            completed_at=job.completed_at,
            overall_progress=0,
            sections=[],
        )
 
    if db_status == JobStatus.PENDING.value:
        return VideoSplitStatusResponse(
            split_job_id=job.split_job_id,
            video_split_job_name=job.video_split_job_name,
            status=db_status,
            error_message=None,
            created_at=job.created_at,
            started_at=job.started_at,
            completed_at=job.completed_at,
            overall_progress=0,
            sections=[],
        )
 
    # PROCESSING or COMPLETED — read from tracker
    live = progress_tracker.get(split_job_id)
 
    return VideoSplitStatusResponse(
        split_job_id=job.split_job_id,
        video_split_job_name=job.video_split_job_name,
        status=db_status,
        error_message=job.error_message,
        created_at=job.created_at,
        started_at=job.started_at,
        completed_at=job.completed_at,
        overall_progress=live["overall_progress"],
        sections=[SectionProgressResponse(**s) for s in live["sections"]],
    )


@router.get("/{split_job_id}", response_model=VideoSplitJobResponse)
async def get_video_split(split_job_id: str):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(VideoSplitJob).where(VideoSplitJob.split_job_id == split_job_id)
        )
        job = result.scalar_one_or_none()
    if not job:
        return JSONResponse(
            status_code=404,
            content={"status": "error", "message": f"Job not found: {split_job_id}"},
        )
    return VideoSplitService._to_response(job)


@router.get("", response_model=VideoSplitJobListResponse)
async def list_video_split_jobs(
    repo_guid: str = Query(...),
    limit: int = Query(100, ge=1, le=1000),
):
    service = VideoSplitService()
    try:
        jobs = await service.get_video_split_export_jobs_by_repo(repo_guid, limit)
        return VideoSplitJobListResponse(total=len(jobs), jobs=jobs)
    except Exception as exc:
        logger.error(f"Failed to list jobs : {traceback.format_exc()}")
        return JSONResponse(status_code=500, content={"status": "error", "message": "Failed to list jobs"})