"""
Export API Routes
"""
from typing import Optional
from uuid import UUID
import traceback

from fastapi import APIRouter, Query, BackgroundTasks
from fastapi.responses import JSONResponse

from app.schemas.export_schemas import (
    ExportWorkOrderCreate,
    ExportJobResponse,
    ExportJobStatusResponse,
    ExportJobListResponse,
    GetLLmPromptResponse,
    UpdateLLmPrompt,
    UpdateLLmPromptResponse,
)
from app.services.export_service import ExportService
from app.background.tasks import process_export_background
from app.core.logging_config import logger

router = APIRouter()


@router.post("", response_model=ExportJobResponse, status_code=202)
async def create_export(
    work_order: ExportWorkOrderCreate,
    background_tasks: BackgroundTasks,
):
    """
    Create a new export work order and queue async processing.
    """
    service = ExportService()

    try:
        job = await service.create_export_job(work_order)
        # Queue background export processing
        background_tasks.add_task(process_export_background, str(job.export_id))

        return job

    except ValueError as exc:
        logger.warning(f"Export validation failed : {str(exc)}")
        return JSONResponse(
            status_code=404,
            content={
                "status": "error",
                "message": str(exc)
            }
        )

    except Exception as exc:
        logger.error(
            f"Failed to create  job : {str(exc)} , traceback={traceback.format_exc()}"
        )
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "message": f"Failed to create  job : {str(exc)}" 
            }
        )


@router.get("/{export_id}/status", response_model=ExportJobStatusResponse)
async def get_export_status(export_id: UUID):
    """
    Get current export job status and timestamps.
    """
    service = ExportService()

    try:
        job = await service.get_export_job(export_id)

        if not job:
            return JSONResponse(
                status_code=404,
                content={
                    "status": "error",
                    "message": f"Export job not found: {export_id}" 
                }
            )

        return ExportJobStatusResponse(
            export_id=job.export_id,
            status=job.status,
            created_at=job.created_at,
            started_at=job.started_at,
            completed_at=job.completed_at,
            error_message=job.error_message,
        )

    except Exception as exc:
        logger.error(
            f"Failed to fetch export job status : export_id={str(export_id)} , error={str(exc)} , traceback={traceback.format_exc()}"
        )
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "message": "Failed to fetch export job status" 
            }
        )
    
@router.get("/{export_id}", response_model=ExportJobResponse)
async def get_export(export_id: UUID):
    """
    Get full export job details including manifest and artifact URLs.
    """
    service = ExportService()

    try:
        job = await service.get_export_job(export_id)

        if not job:
            return JSONResponse(
                status_code=404,
                content={
                    "status": "error",
                    "message": f"Export job not found: {export_id}"
                }
            )

        return job

    except Exception as exc:
        logger.error(
            f"Failed to fetch export job : export_id={str(export_id)} , "
            f"error={str(exc)} , traceback={traceback.format_exc()}",
        )
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "message": "Failed to fetch export job"
            }
        )


@router.get("", response_model=ExportJobListResponse)
async def list_exports(
    repo_guid: str = Query(..., description="Repository ID to filter by"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of results"),
):
    """
    List export jobs for a repository ordered by newest first.
    """
    service = ExportService()

    try:
        jobs = await service.get_export_jobs_by_repo(repo_guid, limit)

        return ExportJobListResponse(
            total=len(jobs),
            jobs=jobs,
        )

    except Exception as exc:
        logger.error(
            f"Failed to list export jobs : repo_guid={repo_guid} , limit={limit} , error={str(exc)} , traceback={traceback.format_exc()}",
        )
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "message": "Failed to list export jobs"
            }
        )

@router.get("/llm_instruct_file/{export_id}", response_model=GetLLmPromptResponse)
async def get_llm_instruct_file(
    export_id: UUID
):
    service = ExportService()

    try:
        response = await service.get_llm_prompt(export_id)

        if not response:
            return JSONResponse(
                status_code=404,
                content={
                    "status": "error",
                    "message": f"LLm Instruct File not found: {export_id}"
                }
            )
            

        return response

    except Exception as exc:
        logger.error(
            f"Failed to fetch LLm Instruct File : export_id={str(export_id)} , error={str(exc)} , traceback={traceback.format_exc()}",
        )
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "message": "Failed to fetch LLm Instruct File"
            }
        )
        
@router.put("/llm_instruct_file", response_model=UpdateLLmPromptResponse, status_code=202)
async def update_llm_instruct_file(
    data: UpdateLLmPrompt
):
    service = ExportService()

    try:
        response = await service.update_llm_prompt(data.export_id, data.prompt)

        if not response:
            return JSONResponse(
                status_code=404,
                content={
                    "status": "error",
                    "message": f"LLm Instruct File not found: {data.export_id}"
                }
            )

        return response

    except Exception as exc:
        logger.error(
            f"Failed to fetch LLm Instruct File : export_id={str(data.export_id)} , error={str(exc)} , traceback={traceback.format_exc()}",
        )
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "message": "Failed to fetch LLm Instruct File"
            }
        )