"""
Import API Routes
"""
import json
from uuid import UUID
import traceback

from fastapi import APIRouter, UploadFile, File, Form, HTTPException, Query, BackgroundTasks

from app.schemas.import_schemas import (
    ImportJobListResponse,
    ImportWorkOrder,
    Highlight,
    ImportJobResponse
)
from app.services.import_service import ImportService
from app.background.tasks import process_import_background_for_llm
from app.core.logging_config import logger

router = APIRouter()

# ------------------------------------------------------------------
# CREATE IMPORT FOR LLm
# ------------------------------------------------------------------
@router.post("", response_model=ImportJobResponse, status_code=202)
async def create_import(
    work_order: ImportWorkOrder,
    # highlights: list[Highlight],
    background_tasks: BackgroundTasks,
):
    """
    Create a new LLM highlight import job.

    • If validation passes → queued for async processing  
    • If validation fails → job stored with FAILED status
    """
    service = ImportService()

    try:
        job = await service.create_import_job(work_order)

        # Queue background processing only if validation succeeded
        if job and job.status != "failed":
            background_tasks.add_task(process_import_background_for_llm, str(job.import_id))

        return job

    except ValueError as exc:
        logger.warning(f"LLM import validation failed : error={str(exc)}")
        raise HTTPException(status_code=400, detail=str(exc))

    except Exception as exc:
        logger.error(
            f"Failed to create LLM import job : error={str(exc)} , traceback={traceback.format_exc()}"
        )
        raise HTTPException(status_code=500, detail="Failed to create import job")

@router.post("/file_uploads", response_model=ImportJobResponse, status_code=202)
async def create_import(
    schemaVersion: str = Form(...),
    repo_guid: str = Form(...),
    fullPath: str = Form(...),
    file: UploadFile = File(None),
    background_tasks: BackgroundTasks = None,
):
    """
    Create a new LLM highlight import job.

    • If validation passes → queued for async processing  
    • If validation fails → job stored with FAILED status
    """
    service = ImportService()

    try:
        # ✅ Read highlights JSON from uploaded file
        file_content = await file.read()
        highlights_list = json.loads(file_content.decode("utf-8"))

        parsed_work_order = ImportWorkOrder(
            schemaVersion=schemaVersion,
            asset={
                "repo_guid": repo_guid,
                "fullPath": fullPath,
            },
            highlights=highlights_list
        )
        job = await service.create_import_job(parsed_work_order)

        # Queue background processing only if validation succeeded
        if job and job.status != "failed":
            background_tasks.add_task(process_import_background_for_llm, str(job.import_id))

        return job

    except ValueError as exc:
        logger.warning(f"LLM import validation failed : error={str(exc)}")
        raise HTTPException(status_code=400, detail=str(exc))

    except Exception as exc:
        logger.error(
            f"Failed to create LLM import job : error={str(exc)} , traceback={traceback.format_exc()}"
        )
        raise HTTPException(status_code=500, detail="Failed to create import job")
    
# ------------------------------------------------------------------
# GET IMPORT STATUS
# ------------------------------------------------------------------
@router.get("/{import_id}/status", response_model=ImportJobResponse)
async def get_import_status(import_id: UUID):
    """
    Get status + progress metrics of an LLM import job.
    """
    service = ImportService()

    try:
        job = await service.get_import_job(import_id)

        if not job:
            raise HTTPException(status_code=404, detail=f"Import job not found: {import_id}")
        return ImportJobResponse(
            import_id=job.import_id,
            repo_guid=job.repo_guid,
            status=job.status,
            items_processed=job.items_processed,
            items_created=job.items_created,
            items_updated=job.items_updated,
            items_skipped=job.items_skipped,
            validation_errors=job.validation_errors,
            error_message=job.error_message,
            created_at=job.created_at,
            started_at=job.started_at,
            completed_at=job.completed_at,
        )

    except Exception as exc:
        logger.error(
            f"Failed to fetch LLM import status : import_id={import_id} , "
            f"error={str(exc)} , traceback={traceback.format_exc()}"
        )
        raise HTTPException(status_code=500, detail="Failed to fetch import job status")


# ------------------------------------------------------------------
# GET FULL IMPORT JOB
# ------------------------------------------------------------------
@router.get("/{import_id}", response_model=ImportJobResponse)
async def get_import(import_id: UUID):
    """
    Get full LLM import job details.
    """
    service = ImportService()

    try:
        job = await service.get_import_job(import_id)

        if not job:
            raise HTTPException(status_code=404, detail=f"Import job not found: {import_id}")

        return job

    except Exception as exc:
        logger.error(
            f"Failed to fetch LLM import job : import_id={import_id} , "
            f"error={str(exc)} , traceback={traceback.format_exc()}"
        )
        raise HTTPException(status_code=500, detail="Failed to fetch import job")
    
@router.get("", response_model=ImportJobListResponse)
async def list_exports(
    repo_guid: str = Query(..., description="Repository ID to filter by"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of results"),
):
    """
    List export jobs for a repository ordered by newest first.
    """
    service = ImportService()

    try:
        jobs = await service.get_import_jobs_by_repo(repo_guid, limit)

        return ImportJobListResponse(
            total=len(jobs),
            jobs=jobs,
        )

    except Exception as exc:
        logger.error(
            f"Failed to list import jobs : repo_guid={repo_guid} , limit={limit} , error={str(exc)} , traceback={traceback.format_exc()}",
        )
        raise HTTPException(
            status_code=500,
            detail="Failed to list export jobs",
        )