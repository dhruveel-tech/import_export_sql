"""
Video Split Service - Video Segmentation Logic
"""
import asyncio
import subprocess
from pathlib import Path
from typing import Optional, Tuple, Any
from pymongo import MongoClient
from bson import ObjectId
from app.core.config import settings
from app.core.logging_config import logger


class VideoSplitClient:
    """Service for splitting videos into segments using FFmpeg."""

    def __init__(self, output_base_path: Optional[str] = None):
        """Initialize MongoDB client directly."""
        self.client = MongoClient(settings.MONGODB_URL)
        self.db = self.client[settings.MONGODB_DB_NAME]
        self.output_base_path = output_base_path or settings.EXPORT_BASE_PATH

    async def close(self):
        """Close MongoDB connection."""
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
    # get_video_duration  (was sync, now async)
    # -----------------------------------------------------------------------
    async def get_video_duration(self, video_filepath: str) -> float:
        """Safe video duration fetch (non-blocking)."""
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
    # calculate_segment_times  (pure math, no I/O – stays sync)
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
    # split_video_segment  (was sync, now async)
    # -----------------------------------------------------------------------
    async def split_video_segment(
        self,
        video_filepath: str,
        start_time: float,
        end_time: float,
        output_path: str,
        encoding: str = "copy",
    ) -> Path:
        """Split a single video segment using FFmpeg (non-blocking)."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            self._split_video_segment_sync,
            video_filepath, start_time, end_time, output_path, encoding,
        )

    def _split_video_segment_sync(
        self,
        video_filepath: str,
        start_time: float,
        end_time: float,
        output_path: str,
        encoding: str = "copy",
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
            subprocess.run(ffmpeg_cmd, check=True, capture_output=True)
            return output_file
        except subprocess.CalledProcessError as e:
            error_msg = e.stderr.decode() if e.stderr else str(e)
            logger.error(f"FFmpeg failed to split video: input={video_filepath}, output={output_file}, error={error_msg}")
            return None

    # -----------------------------------------------------------------------
    # resize_video  (was sync, now async)
    # -----------------------------------------------------------------------
    async def resize_video(
        self,
        video_filepath: str,
        output_path: str,
        width: int,
        height: int,
        keep_aspect: str = "decrease",
        pad: bool = True,
        video_codec: str = "libx264",
        audio_codec: str = "aac",
        crf: int = 23,
        preset: str = "medium",
    ) -> Path:
        """Resize a video using FFmpeg (non-blocking)."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            self._resize_video_sync,
            video_filepath, output_path, width, height,
            keep_aspect, pad, video_codec, audio_codec, crf, preset,
        )

    def _resize_video_sync(
        self,
        video_filepath: str,
        output_path: str,
        width: int,
        height: int,
        keep_aspect: str = "decrease",
        pad: bool = True,
        video_codec: str = "libx264",
        audio_codec: str = "aac",
        crf: int = 23,
        preset: str = "medium",
    ) -> Path:
        video_path = Path(video_filepath)
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        scale_filter = f"scale={width}:{height}:force_original_aspect_ratio={keep_aspect}"
        vf_filter = scale_filter + (f",pad={width}:{height}:(ow-iw)/2:(oh-ih)/2" if pad else "")

        if video_codec == "copy":
            raise ValueError("Cannot use video filters when video_codec='copy'")

        ffmpeg_cmd = [
            "ffmpeg", "-y", "-i", str(video_path),
            "-vf", vf_filter,
            "-c:v", video_codec, "-preset", preset, "-crf", str(crf),
            "-c:a", audio_codec,
            str(output_file),
        ]

        try:
            subprocess.run(ffmpeg_cmd, check=True, capture_output=True)
            return output_file
        except subprocess.CalledProcessError as e:
            error_msg = e.stderr.decode() if e.stderr else str(e)
            logger.error(f"FFmpeg failed to resize video: input={video_filepath}, output={output_file}, error={error_msg}")
            return None

    # -----------------------------------------------------------------------
    # generate_output_filename  (pure logic, stays sync)
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
            safe_label = "".join(c for c in label if c.isalnum() or c in (" ", "-", "_")).strip()
            safe_label = safe_label.replace(" ", "_")
            filename = f"{stem}_{segment_index:03d}_{safe_label}_{start_time:.2f}_to_{end_time:.2f}{ext}"
        else:
            filename = f"{stem}_{segment_index:03d}_{start_time:.2f}_to_{end_time:.2f}{ext}"
        return filename

    # -----------------------------------------------------------------------
    # check_ffmpeg_available  (was sync, now async)
    # -----------------------------------------------------------------------
    async def check_ffmpeg_available(self) -> bool:
        """Check if FFmpeg and FFprobe are available (non-blocking)."""
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
    # get_segment_data  (was sync, now async)
    # -----------------------------------------------------------------------
    async def get_segment_data(self, segment_ids: list, repo_guid: str) -> Optional[dict]:
        """Fetch segment data from MongoDB (non-blocking)."""
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
                logger.warning(f"No segments found in MongoDB for : repo_guid={repo_guid}")
                return None

            segments.sort(key=lambda x: x["start"])
            segment_data = {"repo_guid": repo_guid, "segments": segments}
            logger.info(f"Fetched segments from MongoDB: repo_guid={repo_guid}, segments_count={len(segments)}")
            return segment_data

        except Exception as e:
            logger.error(f"Failed to fetch segments from MongoDB: repo_guid={repo_guid}, error={e}")
            return {"repo_guid": repo_guid, "segments": []}
        
        
# ffmpeg -i "D:\SDNA\AI_Spark\test\India vs Pakistan t20 subclip.mp4" -vf "crop=ih*9/16:ih:(iw-ow)/2:0,scale=1080:1920" -c:v libx264 -preset medium -crf 23 -c:a aac output_reel.mp4