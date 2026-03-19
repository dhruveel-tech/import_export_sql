"""
Background Tasks for Export and Import Processing (SQLite)

Uses FastAPI BackgroundTasks for async job processing.
"""
import json
import asyncio
import math
import shutil
import traceback
from pathlib import Path
from typing import Dict, List, Optional
from uuid import UUID
from datetime import datetime
import zipfile
from fastapi.responses import JSONResponse
from app.services.export_service import ExportService
from sqlalchemy import select
from app.core.progress_tracker import progress_tracker   
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

async def process_export_background(export_id: str):
    """Process export job in background without crashing server."""
    logger.info(f"Starting export processing : export_id={export_id}")

    fabric_client = None
    job = None

    try:
        service = ExportService()
        
        await _ensure_db()

        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(ExportJob).where(ExportJob.export_id == export_id)
            )
            job = result.scalar_one_or_none()

        if not job:
            logger.error(f"Export job not found : export_id={export_id}")
            return

        # Update status to processing
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(ExportJob).where(ExportJob.export_id == export_id)
            )
            job = result.scalar_one_or_none()
            job.status = JobStatus.PROCESSING.value
            job.started_at = datetime.utcnow()
            await session.commit()

        work_order = json.loads(job.work_order)
        repo_guid = work_order["repo_guid"]

        fabric_client = FabricClient()
        generator = ArtifactGenerator(UUID(export_id), work_order)

        artifacts = []
        outputs = work_order.get("outputs", {})
        inputs = work_order.get("inputs", {})           
        user_prompt = work_order.get("user_inputs", {}).get("prompt")
        llm_instructions = work_order.get("user_inputs", {}).get("llm_instructions", True)
        error_msg = []

        # TRANSCRIPT
        if outputs.get("transcript"):
            is_single_segment = outputs.get("transcript", {}).get("isSingleSegment", False)
            transcript_data = await fabric_client.get_transcript(repo_guid, inputs)
            if transcript_data and transcript_data.get('segments'):
                for fmt in outputs["transcript"]["formats"]:
                    filepath, status_msg = _generate_transcript_artifact(generator, transcript_data, fmt, is_single_segment)
                    if status_msg != "Success":
                        error_msg.append(status_msg)
                    artifacts.append(_create_artifact_record(filepath, "transcript", fmt, export_id))

        # EVENTS
        if outputs.get("events"):
            events_data = await fabric_client.get_events(repo_guid, inputs)
            if events_data and events_data.get('segments'):
                for fmt in outputs["events"]["formats"]:
                    filepath, status_msg = _generate_events_artifact(generator, events_data, fmt)
                    if status_msg != "Success":
                        error_msg.append(status_msg)
                    if filepath:
                        artifacts.append(_create_artifact_record(filepath, "events", fmt, export_id))

        # COMMENTS
        insights_data = None
        if outputs.get("insights"):
            insights_data = await fabric_client.get_insights(repo_guid, inputs)
            if insights_data and insights_data.get('segments'):
                for fmt in outputs["insights"]["formats"]:
                    filepath, status_msg = _generate_insights_artifact(generator, insights_data, fmt)
                    if status_msg != "Success":
                        error_msg.append(status_msg)
                    if filepath:
                        artifacts.append(_create_artifact_record(filepath, "insights", fmt, export_id))

        # SELECTS
        # if outputs.get("selects", {}).get("enabled"):
        #     selects_data = _create_selects_from_comments_markers(insights_data or [])
        #     for fmt in outputs["selects"]["formats"]:
        #         if fmt == "edl":
        #             filepath = generator.generate_selects_edl(selects_data)
        #             artifacts.append(_create_artifact_record(filepath, "selects", fmt, export_id))

        # GROUNDING
        if outputs.get("grounding", {}).get("enabled"):
            filepath, status_msg = generator.generate_grounding_prompt(user_prompt)
            if status_msg != "Success":
                error_msg.append(status_msg)
            artifacts.append(_create_artifact_record(filepath, "grounding", "txt", export_id))

        # LLM INSTRUCTIONS
        if llm_instructions:
            llm_prompt_path, status_msg = generator.generate_llm_instructions()
            if status_msg != "Success":
                error_msg.append(status_msg)
            artifacts.append(_create_artifact_record(llm_prompt_path, "llm_instruct", "md", export_id))

        zip_path = _create_zip_from_folder(export_id, settings.EXPORT_BASE_PATH)
        logger.info(f"Zip File Path Generated : {zip_path}")
        # MANIFEST
        manifest = ExportManifest(
            export_id=UUID(export_id),
            repo_guid=repo_guid,
            status="completed",
            created_at=job.created_at,
            artifacts=[ArtifactResponse(**art) for art in artifacts if art],
        )
                
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(ExportJob).where(ExportJob.export_id == export_id)
            )
            job = result.scalar_one_or_none()
            job.manifest = json.dumps(manifest.model_dump(mode="json"))
            job.status = JobStatus.COMPLETED.value
            job.zip_file_path = str(zip_path)
            job.completed_at = datetime.utcnow()
            job.export_path = str(generator.export_dir)
            await session.commit()
            

        logger.info(f"Export processing completed : export_id={export_id} , artifacts_count={len(artifacts)}")
        logger.info("***************************************************************************************************************************")

    except Exception as exc:
        logger.error(f"Export processing failed : export_id={export_id} , error={str(exc)} , traceback={traceback.format_exc()}")
        if job:
            try:
                async with AsyncSessionLocal() as session:
                    result = await session.execute(
                        select(ExportJob).where(ExportJob.export_id == export_id)
                    )
                    job = result.scalar_one_or_none()
                    if job:
                        job.status = JobStatus.FAILED.value
                        job.error_message = str(exc)
                        job.error_details = json.dumps({"traceback": traceback.format_exc()})
                        job.completed_at = datetime.utcnow()
                        await session.commit()
            except Exception as update_exc:
                logger.error(f"Failed to update job status : export_id={export_id}, error={str(update_exc)}")

    finally:
        if fabric_client:
            await fabric_client.close()


# ---------------------------------------------------------------------------
# IMPORT
# ---------------------------------------------------------------------------

async def process_import_background_for_llm(import_id: str, highlights_list: List[Dict]):
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
        for offset in range(0, len(highlights_list), BATCH_SIZE):
            batch = highlights_list[offset:offset + BATCH_SIZE]

            result = await fabric_client.ingest_llm_highlights(
                repo_guid=asset["repo_guid"],
                full_path=asset["fullPath"],
                highlights=batch,
                tag=job.tag
            )

            items_created += result.get("created", 0)
            items_updated += result.get("updated", 0)
            items_skipped += result.get("skipped", 0)
            error_msg = result.get("error_msg", "") if result.get("error_msg", "") != "success" else None

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
    """Process video split job asynchronously with live progress tracking."""
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
        video_service = VideoSplitClient()
 
        if not await video_service.check_ffmpeg_available():
            return JSONResponse(
                status_code=503,
                content={"status": "error", "message": "FFmpeg or FFprobe not available on the system"},
            )
 
        total_duration = await video_service.get_video_duration(job.video_file_path)
        video_path = Path(job.video_file_path)
        output_folder = Path(settings.EXPORT_VIDEO_SPIT_PATH) / split_job_id
        output_folder.mkdir(parents=True, exist_ok=True)
 
        # ── Read output flags ────────────────────────────────────────────
        full_video         = work_order.get("outputs", {}).get("full_video",         {}).get("is_enabled", False)
        individual_segments= work_order.get("outputs", {}).get("individual_segments", {}).get("is_enabled", False)
        merge_segments     = work_order.get("outputs", {}).get("merge_segments",     {}).get("is_enabled", False)
        custom_segments    = work_order.get("outputs", {}).get("custom_segments",    {}).get("is_enabled", False)

        event_ids    = work_order.get("inputs", {}).get("event_ids", [])
        segment_data = await video_service.get_segment_data(event_ids, job.repo_guid) if event_ids else {}
                
        # inputs    = work_order.get("inputs", {})
        # segment_data = await video_service.get_segment_data(inputs, job.repo_guid) if inputs else {}
        segments     = segment_data.get("segments", []) if segment_data else []
 
        clip_duration = (
            work_order.get("outputs", {}).get("custom_segments", {}).get("duration_threshold", None)
            if custom_segments else None
        )
 
        # ── Count per-section FFmpeg operations ─────────────────────────
        section_ops = {}
        if full_video:
            section_ops["full_video"] = 1
        if individual_segments:
            section_ops["individual_segments"] = max(1, len(segments))
        if merge_segments and segments:
            section_ops["merge_segments"] = 1
        if custom_segments and clip_duration and clip_duration > 0:
            section_ops["custom_segments"] = math.ceil(total_duration / clip_duration)
 
        progress_tracker.init_job(split_job_id, section_ops)
        logger.info(f"Progress tracker initialised : split_job_id={split_job_id}, section_ops={section_ops}")
 
        # ── Helpers ──────────────────────────────────────────────────────
 
        def make_callback(section: str, label: str, file_hint: str = ""):
            """Return a progress_callback for a specific section."""
            def _cb(pct: int):
                progress_tracker.update_section(
                    split_job_id,
                    section,
                    label=label,
                    current_file=file_hint,
                    op_progress=pct,
                )
            return _cb
 
        artifacts = []
        results   = []
        segments_successful = 0
        segments_failed     = 0
        segments_processed  = 0
 
        # ════════════════════════════════════════════════════════════════
        # RUN ALL ENABLED SECTIONS CONCURRENTLY
        # Each section writes to its own subfolder — zero file conflicts.
        # ════════════════════════════════════════════════════════════════
 
        async def run_full_video():
            nonlocal segments_processed, segments_successful, segments_failed
            if not full_video:
                return
            try:
                segments_processed += 1
                resize_full_video = work_order.get("outputs", {}).get("full_video", {}).get("is_resize_enabled", {}).get("is_enabled", False)
                position = work_order.get("outputs", {}).get("full_video", {}).get("is_resize_enabled", {}).get("position", "center")
                resize_path = result_path = height = width = None
 
                if resize_full_video:
                    height = work_order.get("outputs", {}).get("full_video", {}).get("is_resize_enabled", {}).get("height", 16)
                    width  = work_order.get("outputs", {}).get("full_video", {}).get("is_resize_enabled", {}).get("width", 9)
                    progress_tracker.update_section(split_job_id, "full_video", label=f"Resizing full video ({height}x{width})")
                    logger.info(f"Resizing full video : split_job_id={split_job_id}, height={height}, width={width}")
                    resize_path, output_filename = await resize_video_task(
                        video_service, video_path, output_folder, height, width,
                        total_duration, resized_op_folder="Resized_Full_Video", position=position,
                        progress_callback=make_callback("full_video", f"Resizing full video ({height}x{width})"),
                    )
                else:
                    progress_tracker.update_section(split_job_id, "full_video", label="Exporting full video")
                    output_filename = video_service.generate_output_filename(video_path, 0, total_duration, "full_video", 0)
                    output_path = output_folder / "Full_Video" / output_filename
                    result_path = await video_service.split_video_segment(
                        job.video_file_path, 0, total_duration, str(output_path),
                        encoding=job.encoding,
                        progress_callback=make_callback("full_video", "Exporting full video", output_filename),
                    )
 
                progress_tracker.complete_section_op(split_job_id, "full_video")
                final_path = result_path if result_path else resize_path
                artifacts.append(_create_artifact_record(final_path, "resized_full_video" if resize_full_video else "full_video", "mp4", split_job_id))
                results.append(generate_result_for_video_split(
                    0, f"resized_{height}x{width}" if resize_full_video else "full_video",
                    0, round(total_duration, 2), 0, round(total_duration, 2), round(total_duration, 2),
                    output_filename, final_path, final_path.stat().st_size, "success", None,
                ))
                segments_successful += 1
                logger.info(f"Full video exported : split_job_id={split_job_id}")
            except Exception as e:
                segments_failed += 1
                progress_tracker.fail_section(split_job_id, "full_video")
                logger.error(f"Full video export failed : error={str(e)}")
 
        async def run_individual_segments():
            nonlocal segments_processed, segments_successful, segments_failed
            if not individual_segments:
                return
            try:
                resize_seg_video = work_order.get("outputs", {}).get("individual_segments", {}).get("is_resize_enabled", {}).get("is_enabled", False)
                position = work_order.get("outputs", {}).get("individual_segments", {}).get("is_resize_enabled", {}).get("position", "center")
                resize_path = result_path = height = width = None
                resize_log_printed = False
                indi_seg_success = indi_seg_failed = 0
 
                for idx, segment_config in enumerate(segments):
                    try:
                        segments_processed += 1
                        start_time = segment_config["start"]
                        end_time   = segment_config["end"]
                        actual_start, actual_end, duration = video_service.calculate_segment_times(
                            start_time, end_time, job.handle_seconds, total_duration
                        )
                        op_label = f"Segment {idx + 1}/{len(segments)}"
 
                        if resize_seg_video:
                            if not resize_log_printed:
                                logger.info("Resizing individual segments...")
                                resize_log_printed = True
                            height = work_order.get("outputs", {}).get("individual_segments", {}).get("is_resize_enabled", {}).get("height", 16)
                            width  = work_order.get("outputs", {}).get("individual_segments", {}).get("is_resize_enabled", {}).get("width", 9)
                            progress_tracker.update_section(split_job_id, "individual_segments", label=f"Resizing {op_label}")
                            resize_path, output_filename = await resize_video_task(
                                video_service, video_path, output_folder, height, width,
                                total_duration, resized_op_folder="Resized_Seg_Video",
                                start_time=actual_start, end_time=actual_end, position=position,
                                progress_callback=make_callback("individual_segments", f"Resizing {op_label}"),
                            )
                            indi_seg_success += 1
                        else:
                            if not resize_log_printed:
                                logger.info("Exporting individual segments...")
                                resize_log_printed = True
                            output_filename = video_service.generate_output_filename(video_path, actual_start, actual_end, "seg_video", idx)
                            output_path = output_folder / "Seg_Video" / output_filename
                            progress_tracker.update_section(split_job_id, "individual_segments", label=f"Exporting {op_label}", current_file=output_filename)
                            result_path = await video_service.split_video_segment(
                                job.video_file_path, actual_start, actual_end, str(output_path),
                                encoding=job.encoding,
                                progress_callback=make_callback("individual_segments", f"Exporting {op_label}", output_filename),
                            )
                            indi_seg_success += 1
 
                        progress_tracker.complete_section_op(split_job_id, "individual_segments")
                        final_path = result_path if result_path else resize_path
                        artifacts.append(_create_artifact_record(final_path, "resized_seg_video" if resize_seg_video else "seg_video", "mp4", split_job_id))
                        results.append(generate_result_for_video_split(
                            idx, f"resized_{height}x{width}" if resize_seg_video else "seg_video",
                            round(start_time, 2), round(end_time, 2),
                            round(actual_start, 2), round(actual_end, 2), round(duration, 2),
                            output_filename, final_path, final_path.stat().st_size, "success", None,
                        ))
                        segments_successful += 1
                    except Exception as seg_error:
                        logger.error(f"Failed to process segment: split_job_id={split_job_id}, idx={idx}, error={seg_error}")
                        progress_tracker.fail_section(split_job_id, "individual_segments")
                        results.append(generate_result_for_video_split(
                            idx, "resized_seg_video" if resize_seg_video else "seg_video",
                            segment_config.get("start", 0), segment_config.get("end", 0),
                            0, 0, 0, "", "", 0, "failed", str(seg_error),
                        ))
                        segments_failed += 1
                        indi_seg_failed += 1
 
                logger.info(f"Individual segments: {indi_seg_success} ok, {indi_seg_failed} failed")
            except Exception as e:
                logger.error(f"Individual segments export failed : error={str(e)}")
 
        async def run_merge_segments():
            nonlocal segments_processed, segments_successful, segments_failed
            if not (merge_segments and segments):
                return
            try:
                segments_processed += 1
                resize_merge_video = work_order.get("outputs", {}).get("merge_segments", {}).get("is_resize_enabled", {}).get("is_enabled", False)
                position = work_order.get("outputs", {}).get("merge_segments", {}).get("is_resize_enabled", {}).get("position", "center")
                resize_path = result_path = height = width = None
 
                min_start = min(seg["start"] for seg in segments)
                max_end   = max(seg["end"]   for seg in segments)
                actual_start, actual_end, duration = video_service.calculate_segment_times(
                    min_start, max_end, job.handle_seconds, total_duration
                )
 
                if resize_merge_video:
                    height = work_order.get("outputs", {}).get("merge_segments", {}).get("is_resize_enabled", {}).get("height", 16)
                    width  = work_order.get("outputs", {}).get("merge_segments", {}).get("is_resize_enabled", {}).get("width", 9)
                    progress_tracker.update_section(split_job_id, "merge_segments", label=f"Resizing merged video ({height}x{width})")
                    resize_path, output_filename = await resize_video_task(
                        video_service, video_path, output_folder, height, width,
                        total_duration, resized_op_folder="Resized_Merge_Video",
                        start_time=actual_start, end_time=actual_end, position=position,
                        progress_callback=make_callback("merge_segments", f"Resizing merged video ({height}x{width})"),
                    )
                else:
                    logger.info("Exporting merged video...")
                    output_filename = video_service.generate_output_filename(video_path, actual_start, actual_end, "merged", 0)
                    output_path = output_folder / "Merge_Video" / output_filename
                    progress_tracker.update_section(split_job_id, "merge_segments", label="Exporting merged video", current_file=output_filename)
                    result_path = await video_service.split_video_segment(
                        job.video_file_path, actual_start, actual_end, str(output_path),
                        encoding=job.encoding,
                        progress_callback=make_callback("merge_segments", "Exporting merged video", output_filename),
                    )
 
                progress_tracker.complete_section_op(split_job_id, "merge_segments")
                final_path = result_path if result_path else resize_path
                artifacts.append(_create_artifact_record(final_path, "resized_merge_video" if resize_merge_video else "merge_video", "mp4", split_job_id))
                results.append(generate_result_for_video_split(
                    0, f"resized_{height}x{width}" if resize_merge_video else "merge_video",
                    round(min_start, 2), round(max_end, 2),
                    round(actual_start, 2), round(actual_end, 2), round(duration, 2),
                    output_filename, final_path, final_path.stat().st_size, "success", None,
                ))
                segments_successful += 1
                logger.info(f"Merged segment created : split_job_id={split_job_id}")
            except Exception as e:
                segments_failed += 1
                progress_tracker.fail_section(split_job_id, "merge_segments")
                logger.error(f"Merge segments failed : error={str(e)}")
 
        async def run_custom_segments():
            nonlocal segments_processed, segments_successful, segments_failed
            if not custom_segments or not clip_duration:
                return
            try:
                resize_seg_video = work_order.get("outputs", {}).get("custom_segments", {}).get("is_resize_enabled", {}).get("is_enabled", False)
                position = work_order.get("outputs", {}).get("custom_segments", {}).get("is_resize_enabled", {}).get("position", "center")
                resize_path = result_path = height = width = None
                resize_log_printed = False
                indi_seg_success = indi_seg_failed = 0
 
                total_clips = math.ceil(total_duration / clip_duration)
                logger.info(f"Processing video in {clip_duration}s clips — total: {total_clips}")
                current_start = 0.0
                idx = 0
 
                while current_start < total_duration:
                    try:
                        segments_processed += 1
                        current_end = min(current_start + clip_duration, total_duration)
                        actual_start, actual_end, duration = video_service.calculate_segment_times(
                            current_start, current_end, job.handle_seconds, total_duration
                        )
                        op_label = f"Custom clip {idx + 1}/{total_clips}"
 
                        if resize_seg_video:
                            if not resize_log_printed:
                                logger.info("Resizing custom segments...")
                                resize_log_printed = True
                            height = work_order.get("outputs", {}).get("custom_segments", {}).get("is_resize_enabled", {}).get("height", 16)
                            width  = work_order.get("outputs", {}).get("custom_segments", {}).get("is_resize_enabled", {}).get("width", 9)
                            progress_tracker.update_section(split_job_id, "custom_segments", label=f"Resizing {op_label}")
                            resize_path, output_filename = await resize_video_task(
                                video_service, video_path, output_folder, height, width,
                                total_duration, resized_op_folder="Resized_Custom_Seg_Video",
                                start_time=actual_start, end_time=actual_end, position=position,
                                progress_callback=make_callback("custom_segments", f"Resizing {op_label}"),
                            )
                            result_path = resize_path
                            indi_seg_success += 1
                        else:
                            if not resize_log_printed:
                                logger.info("Exporting custom segments...")
                                resize_log_printed = True
                            output_filename = video_service.generate_output_filename(video_path, actual_start, actual_end, "seg_video", idx)
                            output_path = output_folder / "Custom_Seg_Video" / output_filename
                            progress_tracker.update_section(split_job_id, "custom_segments", label=f"Exporting {op_label}", current_file=output_filename)
                            result_path = await video_service.split_video_segment(
                                job.video_file_path, actual_start, actual_end, str(output_path),
                                encoding=job.encoding,
                                progress_callback=make_callback("custom_segments", f"Exporting {op_label}", output_filename),
                            )
                            indi_seg_success += 1
 
                        progress_tracker.complete_section_op(split_job_id, "custom_segments")
                        artifacts.append(_create_artifact_record(result_path, "resized_custom_seg_video" if resize_seg_video else "custom_seg_video", "mp4", split_job_id))
                        results.append(generate_result_for_video_split(
                            idx, f"resized_{height}x{width}" if resize_seg_video else "custom_seg_video",
                            round(current_start, 2), round(current_end, 2),
                            round(actual_start, 2), round(actual_end, 2), round(duration, 2),
                            output_filename, result_path, result_path.stat().st_size, "success", None,
                        ))
                        segments_successful += 1
                    except Exception as seg_error:
                        logger.error(f"Failed to process custom segment: idx={idx}, error={seg_error}")
                        progress_tracker.fail_section(split_job_id, "custom_segments")
                        results.append(generate_result_for_video_split(
                            idx, "resized_custom_seg_video" if resize_seg_video else "custom_seg_video",
                            current_start, current_end, 0, 0, 0, "", "", 0, "failed", str(seg_error),
                        ))
                        segments_failed += 1
                        indi_seg_failed += 1
 
                    current_start += clip_duration
                    idx += 1
 
                logger.info(f"Custom segments: {indi_seg_success} ok, {indi_seg_failed} failed")
            except Exception as e:
                logger.error(f"Custom segments export failed : error={str(e)}")
 
        # ── Fire all enabled sections at the same time ───────────────────
        logger.info(f"Starting concurrent processing : split_job_id={split_job_id}")
        await asyncio.gather(
            run_full_video(),
            run_individual_segments(),
            run_merge_segments(),
            run_custom_segments(),
        )
        logger.info(f"All sections complete : split_job_id={split_job_id}")
 
                # ── Zip & manifest ───────────────────────────────────────────────
        # Non-blocking: zip runs in a thread so the event loop stays free
        loop = asyncio.get_event_loop()
        zip_path = await loop.run_in_executor(
            None, _create_zip_from_folder, split_job_id, settings.EXPORT_VIDEO_SPIT_PATH
        )
        logger.info(f"Zip created : {zip_path}")
 
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
            job.results           = json.dumps(results)
            job.status            = JobStatus.COMPLETED.value if segments_failed == 0 else JobStatus.FAILED.value
            job.zip_file_path     = str(zip_path)
            job.completed_at      = datetime.utcnow()
            job.manifest          = json.dumps(manifest.model_dump(mode="json"))
            job.segments_processed= segments_processed
            job.segments_successful= segments_successful
            job.segments_failed   = segments_failed
            await session.commit()
 
        progress_tracker.finish_job(split_job_id)  # ← NEW — set 100%
        logger.info(f"Video split completed : split_job_id={split_job_id} , ok={segments_successful} , fail={segments_failed}")
        logger.info("*" * 120)
 
    except Exception as exc:
        logger.error(f"Video split failed : split_job_id={split_job_id} , error={exc} , trace={traceback.format_exc()}")
        if job:
            try:
                async with AsyncSessionLocal() as session:
                    result = await session.execute(
                        select(VideoSplitJob).where(VideoSplitJob.split_job_id == split_job_id)
                    )
                    job = result.scalar_one_or_none()
                    if job:
                        job.status        = JobStatus.FAILED.value
                        job.error_message = str(exc)
                        job.error_details = json.dumps({"traceback": traceback.format_exc()})
                        job.completed_at  = datetime.utcnow()
                        await session.commit()
            except Exception as update_exc:
                logger.error(f"Failed to update split job failure state: {update_exc}")
 
    finally:
        if video_service:
            await video_service.close()
 
async def resize_video_task(
    video_service,
    video_path,
    output_folder,
    height,
    width,
    total_duration,
    resized_op_folder,
    start_time=None,
    end_time=None,
    position="center",
    progress_callback=None,
):
    output_filename = video_service.generate_output_filename(
        video_path,
        start_time if start_time is not None else 0,
        total_duration if end_time is None else end_time,
        f"resized_{height}x{width}",
        0,
    )
    output_path = output_folder / resized_op_folder / output_filename
 
    resize_path = await video_service.resize_video(
        video_filepath=video_path,
        output_path=str(output_path),
        width=width,
        height=height,
        position=position,
        start_time=start_time,
        end_time=end_time,
        video_codec="libx264",
        audio_codec="aac",
        crf=23,
        preset="medium",
        progress_callback=progress_callback,
        total_duration=total_duration,   # ← pass full duration for progress fallback
    )
 
    return resize_path, output_filename
 

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


def _generate_insights_artifact(generator, data, fmt):
    try:
        if fmt == "json":
            return generator.generate_insights_json(data), "Success"
        elif fmt == "csv":
            return generator.generate_insights_csv(data), "Success"
        elif fmt == "fcpxml":
            return generator.generate_insights_fcpxml(data), "Success"
        elif fmt == "edl":
            return generator.generate_insights_edl(data), "Success"
        else:
            logger.error(f"Unsupported insights format : format={fmt}")
            return None, f"Unsupported insights format : format={fmt}"
    except Exception as exc:
        logger.error(f"Insights artifact generation failed : format={fmt} , error={str(exc)}")
        return None, f"Insights artifact generation failed : format={fmt} , error={str(exc)}"

def _create_zip_from_folder(export_id: UUID, zip_base_folder_path: str) -> str:
    """
    Zips the entire export folder into:
    folder_path/export_id/export_id.zip
    """

    base_folder = Path(zip_base_folder_path)
    export_folder = base_folder / str(export_id)

    if not export_folder.exists() or not export_folder.is_dir():
        return JSONResponse(
            status_code=404,
            content={
                "status": "error",
                "message": f"Export folder not found: {export_folder}"
            }
        )

    # Create zip path: folder_path/export_id/export_id.zip
    zip_path = export_folder / f"{export_id}.zip"

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        for file_path in export_folder.rglob("*"):
            if file_path.is_file() and file_path != zip_path:
                # Preserve internal structure
                zipf.write(file_path, file_path.relative_to(export_folder))
    
    # ---- Remove all original files and subfolders, keep only zip ----
    for item in export_folder.iterdir():
        if item == zip_path:
            continue  
        if item.is_dir():
            shutil.rmtree(item)   # remove subfolder and all its contents
        elif item.is_file():
            item.unlink()         # remove individual file
    # -----------------------------------------------------------------
    
    # zip_file_path = str(zip_path)
    # main_zip_path = zip_path.relative_to("/mnt/AI-Shared-Drive-Demo")

    return str(zip_path)
    

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


def _create_artifact_record(filepath, artifact_type, fmt, export_id):
    try:
        if not filepath:
            logger.error(f"Artifact filepath is missing : artifact_type={artifact_type} , format={fmt} , export_id={export_id}")
            return None

        filepath = Path(filepath)
        filename = filepath.name
        # filepath = filepath.relative_to("/mnt/AI-Shared-Drive-Demo")
        try:
            file_size = filepath.stat().st_size if filepath.exists() else 0
        except Exception as fs_exc:
            logger.warning(f"Failed to read artifact file size : path={str(filepath)} , error={str(fs_exc)}")
            file_size = 0

        return {
            "artifact_type": artifact_type,
            "format": fmt,
            "file_name": filename,
            "file_path": str(filepath),
            "file_size": file_size,
        }

    except Exception as exc:
        logger.error(f"Failed to create artifact record : artifact_type={artifact_type} , format={fmt} , export_id={export_id} , error={str(exc)}")
        return None