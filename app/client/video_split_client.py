"""
Video Split Service - Video Segmentation Logic
Changes vs original:
  - _run_ffmpeg_with_progress imported from app.core.ffmpeg_progress (Windows-safe)
  - split_video_segment / _split_video_segment_sync accept optional progress_callback
  - resize_video / _resize_video_sync               accept optional progress_callback
"""
import asyncio
import subprocess
from pathlib import Path
from typing import Callable, Optional, Tuple, Any

from pymongo import MongoClient
from bson import ObjectId

from app.core.config import settings
from app.core.logging_config import logger
from app.core.ffmpeg_progress import _run_ffmpeg_with_progress  # Windows-safe version

# ---------------------------------------------------------------------------
# Main client
# ---------------------------------------------------------------------------

class VideoSplitClient:
    """Service for splitting / resizing videos using FFmpeg."""

    def __init__(self):
        self.client = MongoClient(settings.MONGODB_URL)
        self.db = self.client[settings.MONGODB_DB_NAME]
        self.output_base_path = settings.EXPORT_VIDEO_SPIT_PATH

    async def close(self):
        if self.client:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self.client.close)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    def _get_collection(self, collection_name: str):
        return self.db[collection_name]

    def _to_object_id(self, id_value: Any) -> Any:
        if isinstance(id_value, str):
            try:
                return ObjectId(id_value)
            except Exception:
                return id_value
        return id_value

    # -----------------------------------------------------------------------
    # get_video_duration
    # -----------------------------------------------------------------------
    async def get_video_duration(self, video_filepath: str) -> float:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._get_video_duration_sync, video_filepath)

    def _get_video_duration_sync(self, video_filepath: str) -> float:
        try:
            video_path = Path(video_filepath)
            if not video_path.exists():
                logger.error(f"Video file not found: filepath={video_filepath}")
                return 0.0
            ffprobe_cmd = [
                "ffprobe", "-v", "error",
                "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1",
                str(video_path),
            ]
            result = subprocess.run(ffprobe_cmd, capture_output=True, text=True, check=True)
            duration = float(result.stdout.strip())
            logger.info(f"Retrieved video duration: filepath={video_filepath}, duration={duration}")
            return duration
        except Exception as exc:
            logger.error(f"Failed to get video duration: filepath={video_filepath}, error={exc}", exc_info=True)
            return 0.0

    # -----------------------------------------------------------------------
    # calculate_segment_times  (pure math)
    # -----------------------------------------------------------------------
    def calculate_segment_times(
        self,
        start_time: float,
        end_time: float,
        handle_seconds: float,
        total_duration: float,
    ) -> Tuple[float, float, float]:
        actual_start = max(0, start_time - handle_seconds)
        actual_end = min(total_duration, end_time + handle_seconds)
        duration = actual_end - actual_start
        return actual_start, actual_end, duration

    # -----------------------------------------------------------------------
    # split_video_segment  — now accepts progress_callback
    # -----------------------------------------------------------------------
    async def split_video_segment(
        self,
        video_filepath: str,
        start_time: float,
        end_time: float,
        output_path: str,
        encoding: str = "copy",
        progress_callback: Optional[Callable[[int], None]] = None,
    ) -> Path:
        """Split a single video segment using FFmpeg (non-blocking)."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            self._split_video_segment_sync,
            video_filepath, start_time, end_time, output_path, encoding, progress_callback,
        )

    def _split_video_segment_sync(
        self,
        video_filepath: str,
        start_time: float,
        end_time: float,
        output_path: str,
        encoding: str = "copy",
        progress_callback: Optional[Callable[[int], None]] = None,
    ) -> Path:
        video_path = Path(video_filepath)
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        duration = end_time - start_time

        if encoding == "copy":
            ffmpeg_cmd = [
                "ffmpeg", "-y",
                "-ss", str(start_time), "-i", str(video_path),
                "-t", str(duration), "-c", "copy",
                str(output_file),
            ]
        else:
            ffmpeg_cmd = [
                "ffmpeg", "-y",
                "-ss", str(start_time), "-i", str(video_path),
                "-t", str(duration), "-c:v", encoding, "-c:a", "aac",
                str(output_file),
            ]

        try:
            _run_ffmpeg_with_progress(ffmpeg_cmd, duration, progress_callback)
            return output_file
        except subprocess.CalledProcessError as e:
            error_msg = e.stderr.decode() if e.stderr else str(e)
            logger.error(
                f"FFmpeg failed to split video: input={video_filepath}, "
                f"output={output_file}, error={error_msg}"
            )
            return None

    # -----------------------------------------------------------------------
    # resize_video  — now accepts progress_callback
    # -----------------------------------------------------------------------
    async def resize_video(
        self,
        video_filepath: str,
        output_path: str,
        width: int,
        height: int,
        start_time: float = None,
        end_time: float = None,
        position: str = "center",
        video_codec: str = "libx264",
        audio_codec: str = "aac",
        crf: int = 23,
        preset: str = "medium",
        progress_callback: Optional[Callable[[int], None]] = None,
        total_duration: float = 0.0,   # full file duration — used when no start/end given
    ) -> Path:
        """Resize a video segment using FFmpeg (non-blocking)."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            self._resize_video_sync,
            video_filepath, output_path, width, height,
            start_time, end_time, position,
            video_codec, audio_codec, crf, preset,
            progress_callback, total_duration,
        )

    def _resize_video_sync(
        self,
        video_filepath: str,
        output_path: str,
        width: int,
        height: int,
        start_time: float = None,
        end_time: float = None,
        position: str = "center",
        video_codec: str = "libx264",
        audio_codec: str = "aac",
        crf: int = 23,
        preset: str = "veryfast",
        progress_callback: Optional[Callable[[int], None]] = None,
        total_duration: float = 0.0,   # full file duration — fallback when no start/end
    ) -> Path:
        video_path = Path(video_filepath)
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
 
        def _get_video_dimensions(filepath: str):
            """
            Return (width, height) of the first video stream using ffprobe.
            Uses key=value output format for reliable parsing on Windows and Linux.
            """
            try:
                probe = subprocess.run(
                    [
                        "ffprobe", "-v", "error",
                        "-select_streams", "v:0",
                        "-show_entries", "stream=width,height",
                        "-of", "default=noprint_wrappers=1:nokey=0",
                        filepath,
                    ],
                    capture_output=True, text=True,
                )
                src_w = src_h = None
                for line in probe.stdout.splitlines():
                    line = line.strip()
                    if line.startswith("width="):
                        src_w = int(line.split("=", 1)[1])
                    elif line.startswith("height="):
                        src_h = int(line.split("=", 1)[1])
                if src_w and src_h:
                    return src_w, src_h
            except Exception as e:
                logger.warning(f"ffprobe dimension check failed: {e}")
            return None, None
 
        src_w, src_h = _get_video_dimensions(str(video_path))
        logger.info(f"Source video dimensions: {src_w}x{src_h}")
 
        if src_w is not None and src_h is not None:
            desired_crop_w = src_h * width // height  
            desired_crop_h = src_h                     
 
            width_fits  = desired_crop_w <= src_w   
            height_fits = desired_crop_h <= src_h    
 
            final_crop_w = desired_crop_w if width_fits  else src_w
            final_crop_h = desired_crop_h if height_fits else src_h
 
            if width_fits:
                if position == "left":
                    x_offset = 0
                elif position == "right":
                    x_offset = src_w - final_crop_w
                else:
                    x_offset = (src_w - final_crop_w) // 2
            else:
                x_offset = 0   
 
            y_offset = (src_h - final_crop_h) // 2  
 
            vf_filter = f"crop={final_crop_w}:{final_crop_h}:{x_offset}:{y_offset}"
 
            logger.info(
                f"Crop decision — "
                f"desired=({desired_crop_w}x{desired_crop_h}), "
                f"source=({src_w}x{src_h}), "
                f"width_fits={width_fits}, height_fits={height_fits}, "
                f"final_crop=({final_crop_w}x{final_crop_h}), "
                f"offset=({x_offset},{y_offset})"
            )
        else:
            logger.warning("Could not probe video dimensions — using expression-based crop filter.")
            crop_width = f"ih*{width}/{height}"
            if position == "left":
                x_offset = "0"
            elif position == "right":
                x_offset = "(iw-ow)"
            else:
                x_offset = "(iw-ow)/2"
            vf_filter = f"crop={crop_width}:ih:{x_offset}:0"
 
        # ── Detect best available hardware encoder ───────────────────────────────
        def _pick_encoder():
            candidates = [
                ("h264_nvenc",        ["-preset", "p2", "-rc", "vbr", "-cq", str(crf)]),
                ("h264_videotoolbox", ["-q:v", "50"]),
                ("h264_qsv",          ["-global_quality", str(crf), "-preset", "veryfast"]),
                ("h264_amf",          ["-quality", "speed", "-rc", "cqp", "-qp_i", str(crf)]),
                ("h264_vaapi",        ["-qp", str(crf)]),
            ]
            for enc, flags in candidates:
                probe = subprocess.run(
                    ["ffmpeg", "-hide_banner", "-encoders"],
                    capture_output=True, text=True
                )
                if enc in probe.stdout:
                    test = subprocess.run(
                        ["ffmpeg", "-y", "-f", "lavfi", "-i", "nullsrc=s=128x128:d=0.1",
                         "-c:v", enc, "-f", "null", "-"],
                        capture_output=True
                    )
                    if test.returncode == 0:
                        return enc, flags
            return "libx264", ["-preset", "ultrafast", "-tune", "zerolatency", "-crf", str(crf)]
 
        encoder, enc_flags = _pick_encoder()
 
        # ── Build FFmpeg command ─────────────────────────────────────────────────
        ffmpeg_cmd = ["ffmpeg", "-y", "-threads", "0"]
 
        op_duration = None
        if start_time is not None and end_time is not None:
            op_duration = end_time - start_time
            ffmpeg_cmd.extend(["-ss", str(start_time), "-t", str(op_duration)])
 
        ffmpeg_cmd.extend(["-i", str(video_path)])
 
        vf_filter_full = (
            f"{vf_filter},format=nv12,hwupload" if encoder == "h264_vaapi" else vf_filter
        )
 
        ffmpeg_cmd.extend([
            "-vf", vf_filter_full,
            "-c:v", encoder,
            *enc_flags,
            "-c:a", "copy" if audio_codec == "copy" else audio_codec,
            "-movflags", "+faststart",
            str(output_file),
        ])
 
        # Duration for progress calculation
        progress_duration = op_duration if op_duration else total_duration
 
        try:
            logger.info(f"ffmpeg command :- {' '.join(ffmpeg_cmd)}")
            _run_ffmpeg_with_progress(ffmpeg_cmd, progress_duration, progress_callback)
            return output_file
        except subprocess.CalledProcessError as e:
            error_msg = e.stderr.decode() if e.stderr else str(e)
            logger.error(
                f"FFmpeg failed (encoder={encoder}): input={video_filepath}, "
                f"output={output_file}, error={error_msg}"
            )
            if encoder != "libx264":
                logger.warning("Hardware encode failed — retrying with libx264 ultrafast")
                return self._resize_video_sync(
                    video_filepath, output_path, width, height,
                    start_time, end_time, position,
                    video_codec="libx264", audio_codec=audio_codec,
                    crf=crf, preset="ultrafast",
                    progress_callback=progress_callback,
                    total_duration=total_duration,
                )
            return None

    # -----------------------------------------------------------------------
    # generate_output_filename  (pure logic)
    # -----------------------------------------------------------------------
    def generate_output_filename(
        self,
        video_path: Path,
        start_time: float,
        end_time: float,
        label: Optional[str] = None,
        segment_index: int = 0,
    ) -> str:
        stem = video_path.stem
        ext = video_path.suffix
        if label:
            safe_label = "".join(
                c for c in label if c.isalnum() or c in (" ", "-", "_")
            ).strip().replace(" ", "_")
            filename = f"{stem}_{segment_index:03d}_{safe_label}_{start_time:.2f}_to_{end_time:.2f}{ext}"
        else:
            filename = f"{stem}_{segment_index:03d}_{start_time:.2f}_to_{end_time:.2f}{ext}"
        return filename

    # -----------------------------------------------------------------------
    # check_ffmpeg_available
    # -----------------------------------------------------------------------
    async def check_ffmpeg_available(self) -> bool:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._check_ffmpeg_available_sync)

    def _check_ffmpeg_available_sync(self) -> bool:
        try:
            subprocess.run(["ffmpeg", "-version"], capture_output=True, check=True)
            subprocess.run(["ffprobe", "-version"], capture_output=True, check=True)
            return True
        except (subprocess.CalledProcessError, FileNotFoundError):
            logger.error("FFmpeg or FFprobe not found")
            return False

    # -----------------------------------------------------------------------
    # get_segment_data
    # -----------------------------------------------------------------------
    async def get_segment_data(self, segment_ids: list, repo_guid: str) -> Optional[dict]:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._get_segment_data_sync, segment_ids, repo_guid)

    def _get_segment_data_sync(self, segment_ids: list, repo_guid: str) -> Optional[dict]:
        try:
            collection = self._get_collection(settings.MONGODB_COLLECTION_NAME_FOR_GET_DATA)
            query = {}
            if segment_ids:
                object_ids = [self._to_object_id(id_val) for id_val in segment_ids]
                query["_id"] = {"$in": object_ids}

            cursor = collection.find(query)
            segments = [
                {"id": str(item.get("_id")), "start": item.get("start", 0), "end": item.get("end", 0)}
                for item in cursor
            ]

            if not segments:
                logger.warning(f"No segments found in MongoDB for: repo_guid={repo_guid}")
                return None

            segments.sort(key=lambda x: x["start"])
            segment_data = {"repo_guid": repo_guid, "segments": segments}
            logger.info(f"Fetched segments from MongoDB: repo_guid={repo_guid}, count={len(segments)}")
            return segment_data

        except Exception as e:
            logger.error(f"Failed to fetch segments from MongoDB: repo_guid={repo_guid}, error={e}")
            return {"repo_guid": repo_guid, "segments": []}
    
    
    
    # def _get_segment_data_sync(
    #     self,
    #     inputs: dict,
    #     repo_guid: str,
    # ) -> Optional[dict]:
    #     try:
    #         collection = self._get_collection(settings.MONGODB_COLLECTION_NAME_FOR_GET_DATA)

    #         segment_ids = inputs.get("event_ids", [])
    #         is_all = inputs.get("is_all", {}) or {}
    #         source_path = inputs.get("source_path", "")

    #         all_segments_map = {} 

    #         # ----------------------------------
    #         # 1. Common filters
    #         # ----------------------------------
    #         base_query = {
    #             "repoGuid": repo_guid,
    #             "fullPath": source_path
    #         }

    #         # Exclude list
    #         exclude_ids = []
    #         if is_all.get("exclude_list"):
    #             exclude_ids = [self._to_object_id(eid) for eid in is_all["exclude_list"]]

    #         # ----------------------------------
    #         # 2. Fetch by event_ids
    #         # ----------------------------------
    #         if segment_ids:
    #             query = base_query.copy()
    #             object_ids = [self._to_object_id(id_val) for id_val in segment_ids]

    #             query["_id"] = {"$in": object_ids}

    #             if exclude_ids:
    #                 query["_id"]["$nin"] = exclude_ids

    #             logger.info(f"[event_ids] Query: {query}")

    #             for item in collection.find(query):
    #                 all_segments_map[str(item["_id"])] = item

    #         # ----------------------------------
    #         # 3. Fetch ALL EVENTS
    #         # ----------------------------------
    #         if is_all.get("is_all_events"):
    #             query = base_query.copy()
    #             query["sdnaEventType"] = {"$nin": ["transcript", "insight"]}

    #             if exclude_ids:
    #                 query["_id"] = {"$nin": exclude_ids}

    #             logger.info(f"[is_all_events] Query: {query}")

    #             for item in collection.find(query):
    #                 all_segments_map[str(item["_id"])] = item

    #         # ----------------------------------
    #         # 4. Fetch ALL TRANSCRIPTS
    #         # ----------------------------------
    #         if is_all.get("is_all_transcript"):
    #             query = base_query.copy()
    #             query["sdnaEventType"] = "transcript"

    #             if exclude_ids:
    #                 query["_id"] = {"$nin": exclude_ids}

    #             logger.info(f"[is_all_transcript] Query: {query}")

    #             for item in collection.find(query):
    #                 all_segments_map[str(item["_id"])] = item

    #         # ----------------------------------
    #         # 5. Fetch ALL INSIGHTS
    #         # ----------------------------------
    #         if is_all.get("is_all_insights"):
    #             query = base_query.copy()
    #             query["sdnaEventType"] = "insight"

    #             if exclude_ids:
    #                 query["_id"] = {"$nin": exclude_ids}

    #             logger.info(f"[is_all_insights] Query: {query}")

    #             for item in collection.find(query):
    #                 all_segments_map[str(item["_id"])] = item

    #         # ----------------------------------
    #         # 6. Convert to list
    #         # ----------------------------------
    #         segments = [
    #             {
    #                 "id": str(item.get("_id")),
    #                 "start": item.get("start", 0),
    #                 "end": item.get("end", 0),
    #                 "type": item.get("sdnaEventType"),
    #             }
    #             for item in all_segments_map.values()
    #         ]

    #         if not segments:
    #             logger.warning(f"No segments found in MongoDB for: repo_guid={repo_guid}")
    #             return None

    #         # ----------------------------------
    #         # 7. Sort
    #         # ----------------------------------
    #         segments.sort(key=lambda x: x["start"])

    #         logger.info(f"Fetched {len(segments)} merged segments")

    #         return {
    #             "repo_guid": repo_guid,
    #             "segments": segments
    #         }

    #     except Exception as e:
    #         logger.error(
    #             f"Failed to fetch segments from MongoDB: repo_guid={repo_guid}, error={e}"
    #         )
    #         return {"repo_guid": repo_guid, "segments": []}