"""
Background Tasks for Export and Import Processing (SQLite)

Uses FastAPI BackgroundTasks for async job processing.
"""
import json
import traceback
from pathlib import Path
from typing import Dict, List, Optional
from uuid import UUID
from datetime import datetime

from fastapi import HTTPException
from sqlalchemy import select

from app.db.session import AsyncSessionLocal, init_db
from app.core.config import settings
from app.models.models import ImportHighlight, ImportLLmJob, JobStatus, ExportJob, VideoSplitJob
from app.schemas.export_schemas import ExportManifest, ArtifactResponse
from app.services.artifact_generator import ArtifactGenerator
from app.client.fabric_client import FabricClient
from app.client.video_split_client import VideoSplitClient
from app.schemas.video_split_schemas import ExportVideoSplitManifest, ArtifactVideoSplitResponse
from app.core.logging_config import logger


# ---------------------------------------------------------------------------
# DB ready flag - background tasks may run before lifespan sets up tables
# ---------------------------------------------------------------------------
_db_initialized = False


async def _ensure_db():
    global _db_initialized
    if not _db_initialized:
        await init_db()
        _db_initialized = True


# ---------------------------------------------------------------------------
# EXPORT
# ---------------------------------------------------------------------------

async def process_export_background(spark_id: str):
    """Process export job in background without crashing server."""
    logger.info(f"Starting export processing : spark_id={spark_id}")

    fabric_client = None
    job = None

    try:
        await _ensure_db()

        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(ExportJob).where(ExportJob.spark_id == spark_id)
            )
            job = result.scalar_one_or_none()

        if not job:
            logger.error(f"Export job not found : spark_id={spark_id}")
            return

        # Update status to processing
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(ExportJob).where(ExportJob.spark_id == spark_id)
            )
            job = result.scalar_one_or_none()
            job.status = JobStatus.PROCESSING.value
            job.started_at = datetime.utcnow()
            await session.commit()

        work_order = json.loads(job.work_order)
        repo_guid = work_order["repo_guid"]

        fabric_client = FabricClient()
        generator = ArtifactGenerator(UUID(spark_id), work_order)

        artifacts = []
        outputs = work_order.get("outputs", {})
        inputs = work_order.get("inputs", {})
        is_single_segment = outputs.get("transcript", {}).get("isSingleSegment", False)
        user_prompt = work_order.get("user_inputs", {}).get("prompt")
        llm_instructions = work_order.get("user_inputs", {}).get("llm_instructions", True)
        error_msg = []

        # TRANSCRIPT
        if outputs.get("transcript"):
            transcript_data = await fabric_client.get_transcript(repo_guid, inputs)
            if transcript_data:
                for fmt in outputs["transcript"]["formats"]:
                    filepath, status_msg = _generate_transcript_artifact(generator, transcript_data, fmt, is_single_segment)
                    if status_msg != "Success":
                        error_msg.append(status_msg)
                    artifacts.append(_create_artifact_record(filepath, "transcript", fmt, spark_id))

        # EVENTS
        if outputs.get("events"):
            events_data = await fabric_client.get_events(repo_guid, inputs)
            for fmt in outputs["events"]["formats"]:
                filepath, status_msg = _generate_events_artifact(generator, events_data, fmt)
                if status_msg != "Success":
                    error_msg.append(status_msg)
                if filepath:
                    artifacts.append(_create_artifact_record(filepath, "events", fmt, spark_id))

        # COMMENTS
        comments_data = None
        if outputs.get("comments"):
            comments_data = await fabric_client.get_comments(repo_guid, inputs)
            for fmt in outputs["comments"]["formats"]:
                filepath, status_msg = _generate_comments_artifact(generator, comments_data, fmt)
                if status_msg != "Success":
                    error_msg.append(status_msg)
                if filepath:
                    artifacts.append(_create_artifact_record(filepath, "comments", fmt, spark_id))

        # SELECTS
        if outputs.get("selects", {}).get("enabled"):
            selects_data = _create_selects_from_comments_markers(comments_data or [])
            for fmt in outputs["selects"]["formats"]:
                if fmt == "edl":
                    filepath = generator.generate_selects_edl(selects_data)
                    artifacts.append(_create_artifact_record(filepath, "selects", fmt, spark_id))

        # GROUNDING
        if outputs.get("grounding", {}).get("enabled"):
            filepath, status_msg = generator.generate_grounding_prompt(user_prompt)
            if status_msg != "Success":
                error_msg.append(status_msg)
            artifacts.append(_create_artifact_record(filepath, "grounding", "txt", spark_id))

        # LLM INSTRUCTIONS
        if llm_instructions:
            llm_prompt_path, status_msg = generator.generate_llm_instructions()
            if status_msg != "Success":
                error_msg.append(status_msg)
            artifacts.append(_create_artifact_record(llm_prompt_path, "llm_instruct", "md", spark_id))

        # MANIFEST
        manifest = ExportManifest(
            spark_id=UUID(spark_id),
            repo_guid=repo_guid,
            status="completed",
            created_at=job.created_at,
            artifacts=[ArtifactResponse(**art) for art in artifacts if art],
        )

        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(ExportJob).where(ExportJob.spark_id == spark_id)
            )
            job = result.scalar_one_or_none()
            job.manifest = json.dumps(manifest.model_dump(mode="json"))
            job.status = JobStatus.COMPLETED.value
            job.completed_at = datetime.utcnow()
            job.export_path = str(generator.export_dir)
            await session.commit()

        logger.info(f"Export processing completed : spark_id={spark_id} , artifacts_count={len(artifacts)}")
        logger.info("***************************************************************************************************************************")

    except Exception as exc:
        logger.error(f"Export processing failed : spark_id={spark_id} , error={str(exc)} , traceback={traceback.format_exc()}")
        if job:
            try:
                async with AsyncSessionLocal() as session:
                    result = await session.execute(
                        select(ExportJob).where(ExportJob.spark_id == spark_id)
                    )
                    job = result.scalar_one_or_none()
                    if job:
                        job.status = JobStatus.FAILED.value
                        job.error_message = str(exc)
                        job.error_details = json.dumps({"traceback": traceback.format_exc()})
                        job.completed_at = datetime.utcnow()
                        await session.commit()
            except Exception as update_exc:
                logger.error(f"Failed to update job status : spark_id={spark_id}, error={str(update_exc)}")

    finally:
        if fabric_client:
            await fabric_client.close()


# ---------------------------------------------------------------------------
# IMPORT
# ---------------------------------------------------------------------------

async def process_import_background_for_llm(import_id: str):
    """Process LLM highlight import job in background safely."""
    logger.info(f"Starting LLM import processing : import_id={import_id}")

    fabric_client = None
    job = None

    try:
        await _ensure_db()

        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(ImportLLmJob).where(ImportLLmJob.import_id == import_id)
            )
            job = result.scalar_one_or_none()

        if not job:
            logger.error(f"LLM import job not found : import_id={import_id}")
            return

        # Mark PROCESSING
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(ImportLLmJob).where(ImportLLmJob.import_id == import_id)
            )
            job = result.scalar_one_or_none()
            job.status = JobStatus.PROCESSING.value
            job.started_at = datetime.utcnow()
            await session.commit()

        fabric_client = FabricClient()
        asset = json.loads(job.asset)

        BATCH_SIZE = 1000
        items_created = 0
        items_updated = 0
        items_skipped = 0
        error_msg = None

        # Stream highlights in batches from SQLite
        offset = 0
        while True:
            async with AsyncSessionLocal() as session:
                result = await session.execute(
                    select(ImportHighlight)
                    .where(ImportHighlight.import_id == import_id)
                    .offset(offset)
                    .limit(BATCH_SIZE)
                )
                highlights = result.scalars().all()

            if not highlights:
                break

            batch = [h.to_dict() for h in highlights]
            result = await fabric_client.ingest_llm_highlights(
                repo_guid=asset["repo_guid"],
                full_path=asset["fullPath"],
                highlights=batch,
            )
            items_created += result.get("created", 0)
            items_updated += result.get("updated", 0)
            items_skipped += result.get("skipped", 0)
            error_msg = result.get("error_msg", "") if result.get("error_msg", "") != "success" else None

            offset += BATCH_SIZE
            if len(highlights) < BATCH_SIZE:
                break

        # Mark COMPLETED
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(ImportLLmJob).where(ImportLLmJob.import_id == import_id)
            )
            job = result.scalar_one_or_none()
            job.items_processed = items_created + items_updated + items_skipped
            job.items_created = items_created
            job.items_updated = items_updated
            job.items_skipped = items_skipped
            job.error_message = str(error_msg) if error_msg else None
            job.status = JobStatus.COMPLETED.value
            job.completed_at = datetime.utcnow()
            await session.commit()

        logger.info(
            f"LLM import completed : import_id={import_id} , "
            f"created={items_created} , updated={items_updated} , skipped={items_skipped}"
        )

    except Exception as exc:
        logger.error(f"LLM import failed : import_id={import_id} , error={str(exc)} , traceback={traceback.format_exc()}")
        if job:
            try:
                async with AsyncSessionLocal() as session:
                    result = await session.execute(
                        select(ImportLLmJob).where(ImportLLmJob.import_id == import_id)
                    )
                    job = result.scalar_one_or_none()
                    if job:
                        job.status = JobStatus.FAILED.value
                        job.error_message = str(exc)
                        job.error_details = json.dumps({"traceback": traceback.format_exc()})
                        job.completed_at = datetime.utcnow()
                        await session.commit()
            except Exception as update_exc:
                logger.error(f"Failed to update LLM job failure state : import_id={import_id} , error={str(update_exc)}")

    finally:
        if fabric_client:
            await fabric_client.close()


# ---------------------------------------------------------------------------
# VIDEO SPLIT
# ---------------------------------------------------------------------------

async def process_video_split_task(split_job_id: str):
    """Process video split job asynchronously."""
    logger.info(f"Starting video split processing : split_job_id={split_job_id}")

    await _ensure_db()
    video_service = None
    job = None

    try:
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(VideoSplitJob).where(VideoSplitJob.split_job_id == split_job_id)
            )
            job = result.scalar_one_or_none()

        if not job:
            logger.error(f"Video split job not found : split_job_id={split_job_id}")
            return

        # Mark PROCESSING
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(VideoSplitJob).where(VideoSplitJob.split_job_id == split_job_id)
            )
            job = result.scalar_one_or_none()
            job.status = JobStatus.PROCESSING.value
            job.started_at = datetime.utcnow()
            await session.commit()

        work_order = json.loads(job.work_order)
        repo_guid = work_order["repo_guid"]

        video_service = VideoSplitClient(output_base_path=job.output_folder)

        if not video_service.check_ffmpeg_available():
            raise HTTPException(status_code=503, detail="FFmpeg or FFprobe not available on the system")

        total_duration = video_service.get_video_duration(job.video_file_path)
        video_path = Path(job.video_file_path)

        if job.output_folder:
            output_folder = Path(job.output_folder)
        else:
            output_folder = Path(settings.EXPORT_BASE_PATH) / split_job_id

        output_folder.mkdir(parents=True, exist_ok=True)

        artifacts = []
        event_ids = work_order.get("inputs", {}).get("event_ids", [])
        segment_data = video_service.get_segment_data(event_ids, job.repo_guid) if event_ids else {}
        segments = segment_data.get("segments", []) if segment_data else []
        full_video = work_order.get("outputs", {}).get("full_video", False)
        individual_segments = work_order.get("outputs", {}).get("individual_segments", False)
        merge_segments = work_order.get("outputs", {}).get("merge_segments", False)
        results = []

        segments_successful = 0
        segments_failed = 0
        segments_processed = 0

        # FULL VIDEO EXPORT
        if full_video:
            try:
                output_filename = video_service.generate_output_filename(video_path, 0, total_duration, "full_video", 0)
                output_path = output_folder / split_job_id / "Full_Video" / output_filename
                result_path = video_service.split_video_segment(job.video_file_path, 0, total_duration, str(output_path), encoding=job.encoding)
                artifacts.append(_create_artifact_record(result_path, "video_full", "mp4", split_job_id))
                results.append(generate_result_for_video_split(0, "full_video", 0, round(total_duration, 2), 0, round(total_duration, 2), round(total_duration, 2), output_filename, result_path, result_path.stat().st_size, "success", None))
                segments_successful += 1
                logger.info(f"Full video exported : split_job_id={split_job_id}")
            except Exception as e:
                segments_failed += 1
                logger.error(f"Full video export failed : error={str(e)}")

        # INDIVIDUAL SEGMENTS
        if individual_segments:
            for idx, segment_config in enumerate(segments):
                try:
                    start_time = segment_config["start"]
                    end_time = segment_config["end"]
                    actual_start, actual_end, duration = video_service.calculate_segment_times(start_time, end_time, job.handle_seconds, total_duration)
                    output_filename = video_service.generate_output_filename(video_path, actual_start, actual_end, "single_seg", idx)
                    output_path = output_folder / split_job_id / "Indiv_Seg" / output_filename
                    result_path = video_service.split_video_segment(job.video_file_path, actual_start, actual_end, str(output_path), encoding=job.encoding)
                    artifacts.append(_create_artifact_record(result_path, "video_split", "mp4", split_job_id))
                    results.append(generate_result_for_video_split(idx, "single_seg", round(start_time, 2), round(end_time, 2), round(actual_start, 2), round(actual_end, 2), round(duration, 2), output_filename, result_path, result_path.stat().st_size, "success", None))
                    segments_successful += 1
                except Exception as seg_error:
                    logger.error(f"Failed to process video segment : split_job_id={split_job_id} , segment_index={idx} , error={str(seg_error)}")
                    results.append(generate_result_for_video_split(idx, "single_seg", segment_config.get("start", 0), segment_config.get("end", 0), 0, 0, 0, "", "", 0, "failed", str(seg_error)))
                    segments_failed += 1
                segments_processed += 1

        # MERGE SEGMENTS
        if merge_segments and segments:
            try:
                min_start = min(seg["start"] for seg in segments)
                max_end = max(seg["end"] for seg in segments)
                actual_start, actual_end, duration = video_service.calculate_segment_times(min_start, max_end, job.handle_seconds, total_duration)
                output_filename = video_service.generate_output_filename(video_path, actual_start, actual_end, "merged", 0)
                output_path = output_folder / split_job_id / "Merge_Seg" / output_filename
                result_path = video_service.split_video_segment(job.video_file_path, actual_start, actual_end, str(output_path), encoding=job.encoding)
                artifacts.append(_create_artifact_record(result_path, "video_merged", "mp4", split_job_id))
                results.append(generate_result_for_video_split(0, "merged_segments", round(min_start, 2), round(max_end, 2), round(actual_start, 2), round(actual_end, 2), round(duration, 2), output_filename, result_path, result_path.stat().st_size, "success", None))
                segments_successful += 1
                logger.info(f"Merged segment created : split_job_id={split_job_id}")
            except Exception as e:
                segments_failed += 1
                logger.error(f"Merge segments failed : error={str(e)}")

        # MANIFEST
        manifest = ExportVideoSplitManifest(
            split_job_id=UUID(split_job_id),
            repo_guid=repo_guid,
            status="completed",
            created_at=job.created_at,
            artifacts=[ArtifactVideoSplitResponse(**art) for art in artifacts if art],
        )

        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(VideoSplitJob).where(VideoSplitJob.split_job_id == split_job_id)
            )
            job = result.scalar_one_or_none()
            job.results = json.dumps(results)
            job.status = JobStatus.COMPLETED.value if segments_failed == 0 else JobStatus.FAILED.value
            job.completed_at = datetime.utcnow()
            job.manifest = json.dumps(manifest.model_dump(mode="json"))
            job.segments_processed = segments_processed
            job.segments_successful = segments_successful
            job.segments_failed = segments_failed
            await session.commit()

        logger.info(f"Video split processing completed : split_job_id={split_job_id} , successful={segments_successful} , failed={segments_failed}")
        logger.info("***************************************************************************************************************************")

    except Exception as exc:
        logger.error(f"Video split processing failed : split_job_id={split_job_id} , error={str(exc)} , traceback={traceback.format_exc()}")
        if job:
            try:
                async with AsyncSessionLocal() as session:
                    result = await session.execute(
                        select(VideoSplitJob).where(VideoSplitJob.split_job_id == split_job_id)
                    )
                    job = result.scalar_one_or_none()
                    if job:
                        job.status = JobStatus.FAILED.value
                        job.error_message = str(exc)
                        job.error_details = json.dumps({"traceback": traceback.format_exc()})
                        job.completed_at = datetime.utcnow()
                        await session.commit()
            except Exception as update_exc:
                logger.error(f"Failed to update split job status : split_job_id={split_job_id} , error={str(update_exc)}")

    finally:
        if video_service:
            await video_service.close()


# ---------------------------------------------------------------------------
# Helpers (unchanged logic, same signatures)
# ---------------------------------------------------------------------------

def generate_result_for_video_split(idx, label, start_time, end_time, actual_start, actual_end, duration, output_filename, result_path, file_size, status, error):
    return {
        "segment_index": idx,
        "label": label,
        "original_start": start_time,
        "original_end": end_time,
        "actual_start": actual_start,
        "actual_end": actual_end,
        "duration": duration,
        "output_filename": output_filename,
        "output_path": str(result_path),
        "file_size_bytes": file_size,
        "status": status,
        "error_message": error,
    }


def _generate_transcript_artifact(generator, data, fmt, is_single_segment):
    try:
        if fmt == "json":
            return generator.generate_transcript_json(data, is_single_segment), "Success"
        elif fmt == "srt":
            return generator.generate_transcript_srt(data), "Success"
        elif fmt == "vtt":
            return generator.generate_transcript_vtt(data), "Success"
        elif fmt == "fcpxml":
            return generator.generate_transcript_fcpxml(data), "Success"
        else:
            logger.error(f"Unsupported transcript format : format={fmt}")
            return None, f"Unsupported transcript format : format={fmt}"
    except Exception as exc:
        logger.error(f"Transcript artifact generation failed : format={fmt} , error={str(exc)}")
        return None, f"Transcript artifact generation failed : format={fmt} , error={str(exc)}"


def _generate_events_artifact(generator, data, fmt):
    try:
        if fmt == "json":
            return generator.generate_events_json(data), "Success"
        elif fmt == "csv":
            return generator.generate_events_csv(data), "Success"
        elif fmt == "fcpxml":
            return generator.generate_events_fcpxml(data), "Success"
        elif fmt == "edl":
            return generator.generate_events_edl(data), "Success"
        else:
            logger.error(f"Unsupported events format : format={fmt}")
            return None, f"Unsupported events format : format={fmt}"
    except Exception as exc:
        logger.error(f"Events artifact generation failed : format={fmt} , error={str(exc)}")
        return None, f"Events artifact generation failed : format={fmt} , error={str(exc)}"


def _generate_comments_artifact(generator, data, fmt):
    try:
        if fmt == "json":
            return generator.generate_comments_json(data), "Success"
        elif fmt == "csv":
            return generator.generate_comments_csv(data), "Success"
        elif fmt == "fcpxml":
            return generator.generate_comments_fcpxml(data), "Success"
        elif fmt == "edl":
            return generator.generate_comments_edl(data), "Success"
        else:
            logger.error(f"Unsupported comments format : format={fmt}")
            return None, f"Unsupported comments format : format={fmt}"
    except Exception as exc:
        logger.error(f"Comments artifact generation failed : format={fmt} , error={str(exc)}")
        return None, f"Comments artifact generation failed : format={fmt} , error={str(exc)}"


def _create_selects_from_comments_markers(comments):
    selects = []
    try:
        for comment in comments or []:
            start_time = comment.get("start_time")
            end_time = comment.get("end_time")
            if start_time is None or end_time is None:
                logger.warning(f"Skipping comment with missing time range : comment_id={comment.get('id')}")
                continue
            selects.append({"id": comment.get("id"), "label": comment.get("label"), "start_time": start_time, "end_time": end_time})
        selects.sort(key=lambda x: x["start_time"])
        return selects
    except Exception as exc:
        logger.error(f"Failed to create selects from comments : error={str(exc)}")
        return []


def _create_artifact_record(filepath, artifact_type, fmt, spark_id):
    try:
        if not filepath:
            logger.error(f"Artifact filepath is missing : artifact_type={artifact_type} , format={fmt} , spark_id={spark_id}")
            return None

        filepath = Path(filepath)
        filename = filepath.name
        url = f"{settings.FABRIC_API_URL}/{spark_id}/{filename}"

        try:
            file_size = filepath.stat().st_size if filepath.exists() else 0
        except Exception as fs_exc:
            logger.warning(f"Failed to read artifact file size : path={str(filepath)} , error={str(fs_exc)}")
            file_size = 0

        return {
            "artifact_type": artifact_type,
            "format": fmt,
            "filename": filename,
            "url": url,
            "file_size": file_size,
        }

    except Exception as exc:
        logger.error(f"Failed to create artifact record : artifact_type={artifact_type} , format={fmt} , spark_id={spark_id} , error={str(exc)}")
        return None