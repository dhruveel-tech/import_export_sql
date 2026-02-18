"""
Video Split Service - Video Segmentation Logic
"""
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
            self.client.close()
    
    async def __aenter__(self):
        """Async context manager entry."""
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()
        
    def _get_collection(self, collection_name: str):
        """Get MongoDB collection."""
        return self.db[collection_name]
    
    def _to_object_id(self, id_value: Any) -> Any:
        """Convert string ID to ObjectId if possible."""
        if isinstance(id_value, str):
            try:
                return ObjectId(id_value)
            except Exception:
                return id_value
        return id_value
    
    def get_video_duration(self, video_filepath: str) -> float:
        """Safe video duration fetch."""
        try:
            video_path = Path(video_filepath)

            if not video_path.exists():
                logger.error(f"Video file not found: filepath={video_filepath}")
                return 0.0

            ffprobe_cmd = [
                "ffprobe",
                "-v", "error",
                "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1",
                str(video_path),
            ]

            result = subprocess.run(
                ffprobe_cmd,
                capture_output=True,
                text=True,
                check=True,
            )

            duration = float(result.stdout.strip())

            logger.info(f"Retrieved video duration: filepath={video_filepath}, duration={duration}")
            return duration

        except Exception as exc:
            logger.error(f"Failed to get video duration: filepath={video_filepath}, error={exc}", exc_info=True)
            return 0.0
    
    def calculate_segment_times(
        self,
        start_time: float,
        end_time: float,
        handle_seconds: float,
        total_duration: float
    ) -> Tuple[float, float, float]:
        """
        Calculate actual start/end times with handles applied.
        
        Args:
            start_time: Original start time
            end_time: Original end time
            handle_seconds: Seconds to add before/after
            total_duration: Total video duration
            
        Returns:
            Tuple of (actual_start, actual_end, duration)
        """
        # Apply handles
        actual_start = max(0, start_time - handle_seconds)
        actual_end = min(total_duration, end_time + handle_seconds)
        
        # Calculate duration
        duration = actual_end - actual_start
        
        return actual_start, actual_end, duration
    
    def split_video_segment(
        self,
        video_filepath: str,
        start_time: float,
        end_time: float,
        output_path: str,
        encoding: str = "copy"
    ) -> Path:
        """
        Split a single video segment using FFmpeg.
        
        Args:
            video_filepath: Source video file path
            start_time: Start time in seconds
            end_time: End time in seconds
            output_path: Output file path
            encoding: Encoding method ('copy' for stream copy, or codec name)
            
        Returns:
            Path to the output file
            
        Raises:
            subprocess.CalledProcessError: If FFmpeg fails
        """
        video_path = Path(video_filepath)
        output_file = Path(output_path)
        
        # Create output directory
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Calculate duration
        duration = end_time - start_time
        
        # Build FFmpeg command
        if encoding == "copy":
            # Fast stream copy (no re-encoding)
            ffmpeg_cmd = [
                "ffmpeg",
                "-y",  # Overwrite output
                "-ss", str(start_time),  # Seek to start
                "-i", str(video_path),   # Input file
                "-t", str(duration),     # Duration
                "-c", "copy",            # Copy streams
                str(output_file)
            ]
        else:
            # Re-encode with specified codec
            ffmpeg_cmd = [
                "ffmpeg",
                "-y",
                "-ss", str(start_time),
                "-i", str(video_path),
                "-t", str(duration),
                "-c:v", encoding,        # Video codec
                "-c:a", "aac",           # Audio codec (AAC is widely compatible)
                str(output_file)
            ]
        
        try:
            subprocess.run(ffmpeg_cmd, check=True, capture_output=True)
                        
            return output_file
            
        except subprocess.CalledProcessError as e:
            error_msg = e.stderr.decode() if e.stderr else str(e)
            logger.error(
                f"FFmpeg failed to split video: input={video_filepath}, output={output_file}, error={error_msg}"
            )
            return None
    
    def generate_output_filename(
        self,
        video_path: Path,
        start_time: float,
        end_time: float,
        label: Optional[str] = None,
        segment_index: int = 0
    ) -> str:
        """
        Generate output filename for a segment.
        
        Args:
            video_path: Original video path
            start_time: Segment start time
            end_time: Segment end time
            label: Optional label for the segment
            segment_index: Index of the segment
            
        Returns:
            Generated filename
        """
        stem = video_path.stem
        ext = video_path.suffix
        
        if label:
            # Sanitize label for filename
            safe_label = "".join(c for c in label if c.isalnum() or c in (' ', '-', '_')).strip()
            safe_label = safe_label.replace(' ', '_')
            filename = f"{stem}_{segment_index:03d}_{safe_label}_{start_time:.2f}_to_{end_time:.2f}{ext}"
        else:
            filename = f"{stem}_{segment_index:03d}_{start_time:.2f}_to_{end_time:.2f}{ext}"
        
        return filename
    
    def check_ffmpeg_available(self) -> bool:
        """
        Check if FFmpeg and FFprobe are available.
        
        Returns:
            True if both are available, False otherwise
        """
        try:
            subprocess.run(
                ["ffmpeg", "-version"],
                capture_output=True,
                check=True
            )
            subprocess.run(
                ["ffprobe", "-version"],
                capture_output=True,
                check=True
            )
            return True
        except (subprocess.CalledProcessError, FileNotFoundError):
            logger.error("FFmpeg or FFprobe not found")
            return False

    def get_segment_data(self, segment_ids: list, repo_guid: str) -> Optional[dict]:  # Changed parameter name
        try:
            collection = self._get_collection(settings.MONGODB_COLLECTION_NAME_FOR_GET_DATA)
            
            query = {}
            if segment_ids:  # Changed variable name
                object_ids = [self._to_object_id(id_val) for id_val in segment_ids]
                query["_id"] = {"$in": object_ids}
                
            # Fetch segments
            cursor = collection.find(query)
            
            # Transform to expected format
            segments = []
            for item in cursor:
                segment = {
                    "id": str(item.get("_id")),
                    "start": item.get("start", 0),
                    "end": item.get("end", 0),
                }
                segments.append(segment)
            
            if not segments:
                logger.warning(f"No segments found in MongoDB for : repo_guid={repo_guid}")
                return None
            
            # Sort by start time
            segments.sort(key=lambda x: x["start"])
            
            segment_data = {
                "repo_guid": repo_guid,
                "segments": segments
            }
            
            logger.info(f"Fetched segments from MongoDB: repo_guid={repo_guid}, segments_count={len(segments)}")
            
            return segment_data
            
        except Exception as e:
            logger.error(f"Failed to fetch segments from MongoDB: repo_guid={repo_guid}, error={e}")
            return {
                "repo_guid": repo_guid,
                "segments": []
            }
