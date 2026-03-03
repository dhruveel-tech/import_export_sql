"""
Artifact Generator - Generate export artifacts in various formats
"""
import json
import csv
import os
from pathlib import Path
from typing import Dict, List, Any
from uuid import UUID
import subprocess
from app.core.config import settings
import xml.etree.ElementTree as ET
from xml.dom import minidom
from datetime import timedelta
from app.core.logging_config import logger

import xml.etree.ElementTree as ET

FPS = 25  # change if needed

class ArtifactGenerator:
    """Generates export artifacts in various formats."""

    def __init__(self, export_id: UUID, work_order: Dict[str, Any]):
        self.export_id = export_id
        self.work_order = work_order
        self.export_dir = Path(settings.EXPORT_BASE_PATH) / str(export_id)
        self.export_dir.mkdir(parents=True, exist_ok=True)

    def generate_transcript_json(self, transcript_data: Dict, is_single_segment: bool) -> Path:
        """Generate canonical transcript JSON without fullPath.
        
        If is_single_segment=True:
            - Merge all transcript text into one string
            - start = min(start)
            - end = max(end)
            - Output only one segment
        """
        filename = "sdna_ai_spark_transcript.json"
        event_dir = self.export_dir / "transcript"
        event_dir.mkdir(parents=True, exist_ok=True)

        filepath = event_dir / filename

        segments = transcript_data.get("segments", []) if isinstance(transcript_data, dict) else []
        
        # ---------------------------------------------------
        # CASE 1 → Merge into single segment
        # ---------------------------------------------------
        try:
            if is_single_segment and segments:
                merged_text = " ".join(
                    str(seg.get("eventValue", "")).strip() for seg in segments if seg.get("eventValue")
                )

                min_start = min(seg.get("start", 0) for seg in segments)
                max_end = max(seg.get("end", 0) for seg in segments)

                cleaned_segments = [
                    {
                        "id": str(1),
                        "sdnaEventType": "transcript",
                        "eventValue": merged_text,
                        "start": min_start,
                        "end": max_end,
                    }
                ]
        
            # Fallback to normal multi-segment output if merging fails
            # ---------------------------------------------------
            # CASE 2 → Normal multi-segment cleaning
            # ---------------------------------------------------
            else:
                cleaned_segments = []

                for item in segments:
                    item_dict = dict(item)          # safe copy
                    item_dict.pop("fullPath", None) # remove unwanted field
                    cleaned_segments.append(item_dict)

        except Exception as exc:
            logger.error(f"Failed to merge transcript segments: error={exc}", exc_info=True)
        # ---------------------------------------------------
        # Final output
        # ---------------------------------------------------
        output_data = {
            "segments": cleaned_segments,
        }

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)
        logger.info("--------------------------------------------------")
        logger.info(f"Generated transcript JSON : filepath={filepath}")
        return filepath
            
    def generate_transcript_srt(self, transcript_data: Dict) -> Path:
        """Generate SRT subtitle file from transcript."""
        filename = "sdna_ai_spark_transcript.srt"
        event_dir = self.export_dir / "transcript"
        event_dir.mkdir(parents=True, exist_ok=True)

        filepath = event_dir / filename

        segments = transcript_data.get("segments", [])
        
        with open(filepath, "w",  encoding="utf-8") as f:
            for i, segment in enumerate(segments, 1):
                start = self._format_srt_time(segment["start"])
                end = self._format_srt_time(segment["end"])
                text = segment["eventValue"]
                
                f.write(f"{i}\n")
                f.write(f"{start} --> {end}\n")
                f.write(f"{text}\n\n")

        logger.info(f"Generated transcript SRT : filepath={filepath}")
        return filepath

    def generate_transcript_vtt(self, transcript_data: Dict) -> Path:
        """Generate WebVTT file from transcript."""
        filename = "sdna_ai_spark_transcript.vtt"
        event_dir = self.export_dir / "transcript"
        event_dir.mkdir(parents=True, exist_ok=True)

        filepath = event_dir / filename

        segments = transcript_data.get("segments", [])
        
        with open(filepath, "w", encoding="utf-8") as f:
            f.write("WEBVTT\n\n")
            
            for idx, segment in enumerate(segments, 1):
                # Skip segments with invalid or empty text
                text = segment.get("eventValue", "").strip()
                if not text:
                    logger.warning(f"Skipping invalid segment {idx}: '{text}'")
                    continue
                
                # Get timestamps
                start = self._format_vtt_time(segment.get("start", 0))
                end = self._format_vtt_time(segment.get("end", 0))
                
                # Validate timestamps
                if start >= end:
                    logger.warning(f"Invalid timestamps for segment {idx}: {start} >= {end}")
                    continue
                
                # Write cue with optional identifier
                f.write(f"{idx}\n")
                f.write(f"{start} --> {end}\n")
                f.write(f"{text}\n\n")

        logger.info(f"Generated transcript VTT : filepath={filepath} , segments_written={idx}")
        return filepath

    def generate_transcript_fcpxml(self, transcript_data: Dict) -> Path:
        """Generate XMEML (FCP7/Premiere) XML for transcript (segments optional)."""

        segments: List[Dict] = transcript_data.get("segments", [])

        # ---------------------------------------------
        # Get optional video path
        # ---------------------------------------------
        video_path = segments[0].get("fullPath") if segments else None
        video_path = r"D:\SDNA\AI_Spark\test\India vs Pakistan t20 subclip.mp4"

        if os.path.exists(video_path):
            meta = self._get_video_metadata(video_path)

            raw_fps = meta["fps"]
            if isinstance(raw_fps, str) and "/" in raw_fps:
                num, den = raw_fps.split("/")
                fps = round(int(num) / int(den))
            else:
                fps = round(float(raw_fps))

            duration_frames = int(meta["duration"] * fps)
            width = str(meta["width"])
            height = str(meta["height"])
        else:
            fps = 25
            duration_seconds = (
                max(float(seg["end"]) for seg in segments) if segments else 1
            )
            duration_frames = int(duration_seconds * fps)
            width = "1920"
            height = "1080"

        def seconds_to_frames(seconds: float) -> int:
            return int(round(float(seconds) * fps))

        # ---------------------------------------------
        # File path
        # ---------------------------------------------
        filename = "sdna_ai_spark_transcript.xml"
        event_dir = self.export_dir / "transcript"
        event_dir.mkdir(parents=True, exist_ok=True)
        filepath = event_dir / filename

        video_filename = Path(video_path).name if video_path else "video.mp4"
        pathurl = Path(video_path).as_uri() if video_path else ""

        # -----------------------------------------------------
        # Build XML
        # -----------------------------------------------------
        xmeml = ET.Element("xmeml", version="4")
        sequence = ET.SubElement(xmeml, "sequence")

        ET.SubElement(sequence, "n").text = "AI Events Project"
        ET.SubElement(sequence, "duration").text = str(duration_frames)

        def add_rate(parent):
            rate = ET.SubElement(parent, "rate")
            ET.SubElement(rate, "timebase").text = str(fps)
            ET.SubElement(rate, "ntsc").text = "FALSE"

        add_rate(sequence)

        # Timecode
        timecode = ET.SubElement(sequence, "timecode")
        add_rate(timecode)
        ET.SubElement(timecode, "string").text = "00:00:00:00"
        ET.SubElement(timecode, "frame").text = "0"
        ET.SubElement(timecode, "displayformat").text = "NDF"

        media = ET.SubElement(sequence, "media")

        # ---------------- VIDEO ----------------
        video = ET.SubElement(media, "video")
        fmt = ET.SubElement(video, "format")
        samplechar = ET.SubElement(fmt, "samplecharacteristics")
        add_rate(samplechar)
        ET.SubElement(samplechar, "width").text = width
        ET.SubElement(samplechar, "height").text = height
        ET.SubElement(samplechar, "pixelaspectratio").text = "square"
        ET.SubElement(samplechar, "fielddominance").text = "none"

        track_v = ET.SubElement(video, "track")
        clipitem_v = ET.SubElement(track_v, "clipitem", id="clipitem-1")

        ET.SubElement(clipitem_v, "masterclipid").text = "masterclip-1"
        ET.SubElement(clipitem_v, "ismasterclip").text = "FALSE"
        ET.SubElement(clipitem_v, "n").text = video_filename
        ET.SubElement(clipitem_v, "enabled").text = "TRUE"
        ET.SubElement(clipitem_v, "duration").text = str(duration_frames)

        add_rate(clipitem_v)

        ET.SubElement(clipitem_v, "start").text = "0"
        ET.SubElement(clipitem_v, "end").text = str(duration_frames)
        ET.SubElement(clipitem_v, "in").text = "0"
        ET.SubElement(clipitem_v, "out").text = str(duration_frames)
        ET.SubElement(clipitem_v, "alphatype").text = "none"
        ET.SubElement(clipitem_v, "pixelaspectratio").text = "square"

        # File reference
        file_elem = ET.SubElement(clipitem_v, "file", id="file-1")
        ET.SubElement(file_elem, "n").text = video_filename
        ET.SubElement(file_elem, "pathurl").text = pathurl
        add_rate(file_elem)
        ET.SubElement(file_elem, "duration").text = str(duration_frames)

        file_media = ET.SubElement(file_elem, "media")

        file_video = ET.SubElement(file_media, "video")
        file_samplechar = ET.SubElement(file_video, "samplecharacteristics")
        add_rate(file_samplechar)
        ET.SubElement(file_samplechar, "width").text = width
        ET.SubElement(file_samplechar, "height").text = height

        file_audio = ET.SubElement(file_media, "audio")
        file_audio_sc = ET.SubElement(file_audio, "samplecharacteristics")
        ET.SubElement(file_audio_sc, "depth").text = "16"
        ET.SubElement(file_audio_sc, "samplerate").text = "48000"
        ET.SubElement(file_audio, "channelcount").text = "2"

        # ---------------- MARKERS ----------------
        for seg in segments:
            start_frame = seconds_to_frames(float(seg.get("start", 0)))
            end_frame = seconds_to_frames(float(seg.get("end", seg.get("start", 0))))
            event_type = seg.get("sdnaEventType", "")
            text = seg.get("eventValue", "")

            marker_label = f"[{event_type}] {text}" if event_type else text

            marker = ET.SubElement(clipitem_v, "marker")
            ET.SubElement(marker, "comment").text = text
            ET.SubElement(marker, "n").text = marker_label
            ET.SubElement(marker, "in").text = str(start_frame)
            ET.SubElement(marker, "out").text = str(end_frame)

        # ---------------- AUDIO TRACKS ----------------
        audio = ET.SubElement(media, "audio")

        for track_index in [1, 2]:
            track_a = ET.SubElement(audio, "track")
            clipitem_a = ET.SubElement(
                track_a, "clipitem", id=f"clipitem-{track_index + 1}"
            )

            ET.SubElement(clipitem_a, "masterclipid").text = "masterclip-1"
            ET.SubElement(clipitem_a, "ismasterclip").text = "FALSE"
            ET.SubElement(clipitem_a, "n").text = video_filename
            ET.SubElement(clipitem_a, "enabled").text = "TRUE"
            ET.SubElement(clipitem_a, "duration").text = str(duration_frames)

            add_rate(clipitem_a)

            ET.SubElement(clipitem_a, "start").text = "0"
            ET.SubElement(clipitem_a, "end").text = str(duration_frames)
            ET.SubElement(clipitem_a, "in").text = "0"
            ET.SubElement(clipitem_a, "out").text = str(duration_frames)

            ET.SubElement(clipitem_a, "file", id="file-1")

            sourcetrack = ET.SubElement(clipitem_a, "sourcetrack")
            ET.SubElement(sourcetrack, "mediatype").text = "audio"
            ET.SubElement(sourcetrack, "trackindex").text = str(track_index)

        # -----------------------------------------------------
        # Write XML with proper header
        # -----------------------------------------------------
        pretty_xml = self._prettify_xml(xmeml)

        # Remove old declaration if exists
        if pretty_xml.startswith("<?xml"):
            pretty_xml = pretty_xml.split("?>", 1)[1].strip()

        xml_header = '<?xml version="1.0" encoding="UTF-8"?>'
        doctype = "<!DOCTYPE xmeml>"

        final_xml = f"{xml_header}\n{doctype}\n{pretty_xml}"

        with open(filepath, "w", encoding="utf-8") as f:
            f.write(final_xml)
        logger.info(f"Generated Trascript XMEML file : filepath={filepath}")
        logger.info("--------------------------------------------------")
        return filepath
    
    ##################### Events ######################
    
    def generate_events_json(self, events_data: Dict) -> Path:
        """Generate canonical events JSON without fullPath."""

        filename = "sdna_ai_spark_events.json"
        event_dir = self.export_dir / "event"
        event_dir.mkdir(parents=True, exist_ok=True)

        filepath = event_dir / filename

        segments = events_data.get("segments", [])

        cleaned_segments = []

        for item in segments:
            item_dict = dict(item)          # safe copy
            item_dict.pop("fullPath", None) # remove unwanted field
            cleaned_segments.append(item_dict)

        # keep repo_guid if needed
        output_data = {
            "segments": cleaned_segments,
        }

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(output_data, f, indent=2)

        logger.info(f"Generated events JSON : filepath={filepath}")
        return filepath
        
    def generate_events_csv(self, events_data: Dict) -> Path:
        """Generate CSV file from events without positions and confidenceScore."""
        filename = "sdna_ai_spark_events.csv"
        event_dir = self.export_dir / "event"
        event_dir.mkdir(parents=True, exist_ok=True)

        filepath = event_dir / filename

        headers = ["id", "sdnaEventType", "eventValue", "start", "end"]

        # Ensure events_data is usable
        segments = []
        if isinstance(events_data, dict):
            segments = events_data.get("segments") or []

        with open(filepath, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()

            for event in segments:
                row = {
                    "id": event.get("id"),
                    "sdnaEventType": event.get("sdnaEventType"),
                    "eventValue": event.get("eventValue"),
                    "start": event.get("start"),
                    "end": event.get("end"),
                }
                writer.writerow(row)

        logger.info(f"Generated events CSV : filepath={filepath}")
        return filepath

    def generate_events_fcpxml(self, event_data: Dict) -> Path:
        """Generate XMEML (FCP7/Premiere) XML for events (segments optional)."""

        segments: List[Dict] = event_data.get("segments", [])

        # ---------------------------------------------
        # Get optional video path
        # ---------------------------------------------
        video_path = segments[0].get("fullPath") if segments else None
        video_path = r"D:\SDNA\AI_Spark\test\India vs Pakistan t20 subclip.mp4"

        if os.path.exists(video_path):
            meta = self._get_video_metadata(video_path)

            raw_fps = meta["fps"]
            if isinstance(raw_fps, str) and "/" in raw_fps:
                num, den = raw_fps.split("/")
                fps = round(int(num) / int(den))
            else:
                fps = round(float(raw_fps))

            duration_frames = int(meta["duration"] * fps)
            width = str(meta["width"])
            height = str(meta["height"])
        else:
            fps = 25
            duration_seconds = (
                max(float(seg["end"]) for seg in segments) if segments else 1
            )
            duration_frames = int(duration_seconds * fps)
            width = "1920"
            height = "1080"

        def seconds_to_frames(seconds: float) -> int:
            return int(round(float(seconds) * fps))

        # ---------------------------------------------
        # File path
        # ---------------------------------------------
        filename = "sdna_ai_spark_events.xml"
        event_dir = self.export_dir / "event"
        event_dir.mkdir(parents=True, exist_ok=True)
        filepath = event_dir / filename

        video_filename = Path(video_path).name if video_path else "video.mp4"
        pathurl = Path(video_path).as_uri() if video_path else ""

        # -----------------------------------------------------
        # Build XML
        # -----------------------------------------------------
        xmeml = ET.Element("xmeml", version="4")
        sequence = ET.SubElement(xmeml, "sequence")

        ET.SubElement(sequence, "n").text = "AI Events Project"
        ET.SubElement(sequence, "duration").text = str(duration_frames)

        def add_rate(parent):
            rate = ET.SubElement(parent, "rate")
            ET.SubElement(rate, "timebase").text = str(fps)
            ET.SubElement(rate, "ntsc").text = "FALSE"

        add_rate(sequence)

        # Timecode
        timecode = ET.SubElement(sequence, "timecode")
        add_rate(timecode)
        ET.SubElement(timecode, "string").text = "00:00:00:00"
        ET.SubElement(timecode, "frame").text = "0"
        ET.SubElement(timecode, "displayformat").text = "NDF"

        media = ET.SubElement(sequence, "media")

        # ---------------- VIDEO ----------------
        video = ET.SubElement(media, "video")
        fmt = ET.SubElement(video, "format")
        samplechar = ET.SubElement(fmt, "samplecharacteristics")
        add_rate(samplechar)
        ET.SubElement(samplechar, "width").text = width
        ET.SubElement(samplechar, "height").text = height
        ET.SubElement(samplechar, "pixelaspectratio").text = "square"
        ET.SubElement(samplechar, "fielddominance").text = "none"

        track_v = ET.SubElement(video, "track")
        clipitem_v = ET.SubElement(track_v, "clipitem", id="clipitem-1")

        ET.SubElement(clipitem_v, "masterclipid").text = "masterclip-1"
        ET.SubElement(clipitem_v, "ismasterclip").text = "FALSE"
        ET.SubElement(clipitem_v, "n").text = video_filename
        ET.SubElement(clipitem_v, "enabled").text = "TRUE"
        ET.SubElement(clipitem_v, "duration").text = str(duration_frames)

        add_rate(clipitem_v)

        ET.SubElement(clipitem_v, "start").text = "0"
        ET.SubElement(clipitem_v, "end").text = str(duration_frames)
        ET.SubElement(clipitem_v, "in").text = "0"
        ET.SubElement(clipitem_v, "out").text = str(duration_frames)
        ET.SubElement(clipitem_v, "alphatype").text = "none"
        ET.SubElement(clipitem_v, "pixelaspectratio").text = "square"

        # File reference
        file_elem = ET.SubElement(clipitem_v, "file", id="file-1")
        ET.SubElement(file_elem, "n").text = video_filename
        ET.SubElement(file_elem, "pathurl").text = pathurl
        add_rate(file_elem)
        ET.SubElement(file_elem, "duration").text = str(duration_frames)

        file_media = ET.SubElement(file_elem, "media")

        file_video = ET.SubElement(file_media, "video")
        file_samplechar = ET.SubElement(file_video, "samplecharacteristics")
        add_rate(file_samplechar)
        ET.SubElement(file_samplechar, "width").text = width
        ET.SubElement(file_samplechar, "height").text = height

        file_audio = ET.SubElement(file_media, "audio")
        file_audio_sc = ET.SubElement(file_audio, "samplecharacteristics")
        ET.SubElement(file_audio_sc, "depth").text = "16"
        ET.SubElement(file_audio_sc, "samplerate").text = "48000"
        ET.SubElement(file_audio, "channelcount").text = "2"

        # ---------------- MARKERS ----------------
        for seg in segments:
            start_frame = seconds_to_frames(float(seg.get("start", 0)))
            end_frame = seconds_to_frames(float(seg.get("end", seg.get("start", 0))))
            event_type = seg.get("sdnaEventType", "")
            text = seg.get("eventValue", "")

            marker_label = f"[{event_type}] {text}" if event_type else text

            marker = ET.SubElement(clipitem_v, "marker")
            ET.SubElement(marker, "comment").text = text
            ET.SubElement(marker, "n").text = marker_label
            ET.SubElement(marker, "in").text = str(start_frame)
            ET.SubElement(marker, "out").text = str(end_frame)

        # ---------------- AUDIO TRACKS ----------------
        audio = ET.SubElement(media, "audio")

        for track_index in [1, 2]:
            track_a = ET.SubElement(audio, "track")
            clipitem_a = ET.SubElement(
                track_a, "clipitem", id=f"clipitem-{track_index + 1}"
            )

            ET.SubElement(clipitem_a, "masterclipid").text = "masterclip-1"
            ET.SubElement(clipitem_a, "ismasterclip").text = "FALSE"
            ET.SubElement(clipitem_a, "n").text = video_filename
            ET.SubElement(clipitem_a, "enabled").text = "TRUE"
            ET.SubElement(clipitem_a, "duration").text = str(duration_frames)

            add_rate(clipitem_a)

            ET.SubElement(clipitem_a, "start").text = "0"
            ET.SubElement(clipitem_a, "end").text = str(duration_frames)
            ET.SubElement(clipitem_a, "in").text = "0"
            ET.SubElement(clipitem_a, "out").text = str(duration_frames)

            ET.SubElement(clipitem_a, "file", id="file-1")

            sourcetrack = ET.SubElement(clipitem_a, "sourcetrack")
            ET.SubElement(sourcetrack, "mediatype").text = "audio"
            ET.SubElement(sourcetrack, "trackindex").text = str(track_index)

        # -----------------------------------------------------
        # Write XML with proper header
        # -----------------------------------------------------
        pretty_xml = self._prettify_xml(xmeml)

        # Remove old declaration if exists
        if pretty_xml.startswith("<?xml"):
            pretty_xml = pretty_xml.split("?>", 1)[1].strip()

        xml_header = '<?xml version="1.0" encoding="UTF-8"?>'
        doctype = "<!DOCTYPE xmeml>"

        final_xml = f"{xml_header}\n{doctype}\n{pretty_xml}"

        with open(filepath, "w", encoding="utf-8") as f:
            f.write(final_xml)
        logger.info(f"Generated Events XMEML file : filepath={filepath}")
        return filepath
    
    def generate_events_edl(self, events: Dict, reel="AX", track="V") -> str:
        """Convert event list → EDL text with proper CMX 3600 format"""
        
        filename = "sdna_ai_spark_events.edl"
        event_dir = self.export_dir / "event"
        event_dir.mkdir(parents=True, exist_ok=True)
        filepath = event_dir / filename
        
        # ---------------------------------------------
        # Get video path from first segment or use default
        # ---------------------------------------------
        segments = events.get("segments", [])
        video_path = ""

        if segments:
            video_path = segments[0].get("fullPath", r"D:\SDNA\AI_Spark\test\India vs Pakistan t20 subclip.mp4") or ""
        video_path = r"D:\SDNA\AI_Spark\test\India vs Pakistan t20 subclip.mp4" 
        if video_path:
            video_name = Path(video_path).name
        else:
            logger.info("Video path not found. Generating EDL without clip reference.")
            video_name = ""

        edl_lines = []
        record_in = 0.0  # Running record timecode tracker
        
        for i, event in enumerate(segments, start=1):
            # Source timecodes (from original video)
            source_in = self.seconds_to_timecode(event["start"])
            source_out = self.seconds_to_timecode(event["end"])
            
            # Record timecodes (timeline position)
            duration = event["end"] - event["start"]
            record_in_tc = self.seconds_to_timecode(record_in)
            record_out_tc = self.seconds_to_timecode(record_in + duration)
            
            # EDL edit line (proper spacing for CMX 3600 format)
            line = (
                f"{i:03d}  {reel:<8} {track:<5} C        "
                f"{source_in} {source_out} {record_in_tc} {record_out_tc}"
            )
            
            edl_lines.append(line)
            edl_lines.append(f"* FROM CLIP NAME: {video_name}")
            
            # Custom metadata (non-standard but useful)
            if event.get('id'):
                edl_lines.append(f"* EVENT ID: {event['id']}")
            if event.get('sdnaEventType'):
                edl_lines.append(f"* SDNA EVENT TYPE: {event['sdnaEventType']}")
            if event.get('eventValue'):
                edl_lines.append(f"* EVENT VALUE: {event['eventValue']}")
            
            edl_lines.append("")  # Blank line between events
            
            # Update record position for next event
            record_in += duration
        
        # Write EDL file
        edl_content = (
            f"TITLE: AI Spark Events\n"
            f"FCM: NON-DROP FRAME\n\n"
            f"{chr(10).join(edl_lines)}"
        )
        
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(edl_content)
        logger.info(f"Generated events edl : filepath={filepath}")
        logger.info("--------------------------------------------------")
        return filepath

    def seconds_to_timecode(self, seconds: float, fps: int = FPS) -> str:
        """Convert seconds → HH:MM:SS:FF"""
        td = timedelta(seconds=seconds)

        total_seconds = int(td.total_seconds())
        frames = int((seconds - total_seconds) * fps)

        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        secs = total_seconds % 60

        return f"{hours:02}:{minutes:02}:{secs:02}:{frames:02}"

    ##################### insights ######################

    def generate_insights_json(self, insights_data: Dict) -> Path:
        """Generate canonical insights JSON."""
        filename = "sdna_ai_spark_insights.json"
        event_dir = self.export_dir / "insights"
        event_dir.mkdir(parents=True, exist_ok=True)

        filepath = event_dir / filename

        segments = insights_data.get("segments", [])

        cleaned_segments = []

        for item in segments:
            item_dict = dict(item)          # safe copy
            item_dict.pop("fullPath", None) # remove unwanted field
            cleaned_segments.append(item_dict)

        # keep repo_guid if needed
        output_data = {
            "segments": cleaned_segments,
        }

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(output_data, f, indent=2)

        logger.info(f"Generated insights JSON : filepath={filepath}")
        return filepath

    def generate_insights_csv(self, insights_data: Dict) -> Path:
        """Generate CSV file from insights."""
        filename = "sdna_ai_spark_insights.csv"
        event_dir = self.export_dir / "insights"
        event_dir.mkdir(parents=True, exist_ok=True)

        filepath = event_dir / filename

        import csv

        headers = ["id", "sdnaEventType", "eventValue", "start", "end", "source"]
        
        with open(filepath, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
            writer.writeheader()
            
            for comment in insights_data.get("segments", []):
                row = {k: v for k, v in comment.items() if k != "metadata"}
                writer.writerow(row)

        logger.info(f"Generated insights CSV : filepath={filepath}")
        return filepath

    def generate_insights_fcpxml(self, insights_data: Dict) -> Path:
        """Generate XMEML (FCP7/Premiere) XML for events (segments optional)."""

        segments: List[Dict] = insights_data.get("segments", [])

        # ---------------------------------------------
        # Get optional video path
        # ---------------------------------------------
        video_path = segments[0].get("fullPath") if segments else None
        video_path = r"D:\SDNA\AI_Spark\test\India vs Pakistan t20 subclip.mp4"

        if os.path.exists(video_path):
            meta = self._get_video_metadata(video_path)

            raw_fps = meta["fps"]
            if isinstance(raw_fps, str) and "/" in raw_fps:
                num, den = raw_fps.split("/")
                fps = round(int(num) / int(den))
            else:
                fps = round(float(raw_fps))

            duration_frames = int(meta["duration"] * fps)
            width = str(meta["width"])
            height = str(meta["height"])
        else:
            fps = 25
            duration_seconds = (
                max(float(seg["end"]) for seg in segments) if segments else 1
            )
            duration_frames = int(duration_seconds * fps)
            width = "1920"
            height = "1080"

        def seconds_to_frames(seconds: float) -> int:
            return int(round(float(seconds) * fps))

        # ---------------------------------------------
        # File path
        # ---------------------------------------------
        filename = "sdna_ai_spark_insights.xml"
        event_dir = self.export_dir / "insights"
        event_dir.mkdir(parents=True, exist_ok=True)
        filepath = event_dir / filename

        video_filename = Path(video_path).name if video_path else "video.mp4"
        pathurl = Path(video_path).as_uri() if video_path else ""

        # -----------------------------------------------------
        # Build XML
        # -----------------------------------------------------
        xmeml = ET.Element("xmeml", version="4")
        sequence = ET.SubElement(xmeml, "sequence")

        ET.SubElement(sequence, "n").text = "AI Events Project"
        ET.SubElement(sequence, "duration").text = str(duration_frames)

        def add_rate(parent):
            rate = ET.SubElement(parent, "rate")
            ET.SubElement(rate, "timebase").text = str(fps)
            ET.SubElement(rate, "ntsc").text = "FALSE"

        add_rate(sequence)

        # Timecode
        timecode = ET.SubElement(sequence, "timecode")
        add_rate(timecode)
        ET.SubElement(timecode, "string").text = "00:00:00:00"
        ET.SubElement(timecode, "frame").text = "0"
        ET.SubElement(timecode, "displayformat").text = "NDF"

        media = ET.SubElement(sequence, "media")

        # ---------------- VIDEO ----------------
        video = ET.SubElement(media, "video")
        fmt = ET.SubElement(video, "format")
        samplechar = ET.SubElement(fmt, "samplecharacteristics")
        add_rate(samplechar)
        ET.SubElement(samplechar, "width").text = width
        ET.SubElement(samplechar, "height").text = height
        ET.SubElement(samplechar, "pixelaspectratio").text = "square"
        ET.SubElement(samplechar, "fielddominance").text = "none"

        track_v = ET.SubElement(video, "track")
        clipitem_v = ET.SubElement(track_v, "clipitem", id="clipitem-1")

        ET.SubElement(clipitem_v, "masterclipid").text = "masterclip-1"
        ET.SubElement(clipitem_v, "ismasterclip").text = "FALSE"
        ET.SubElement(clipitem_v, "n").text = video_filename
        ET.SubElement(clipitem_v, "enabled").text = "TRUE"
        ET.SubElement(clipitem_v, "duration").text = str(duration_frames)

        add_rate(clipitem_v)

        ET.SubElement(clipitem_v, "start").text = "0"
        ET.SubElement(clipitem_v, "end").text = str(duration_frames)
        ET.SubElement(clipitem_v, "in").text = "0"
        ET.SubElement(clipitem_v, "out").text = str(duration_frames)
        ET.SubElement(clipitem_v, "alphatype").text = "none"
        ET.SubElement(clipitem_v, "pixelaspectratio").text = "square"

        # File reference
        file_elem = ET.SubElement(clipitem_v, "file", id="file-1")
        ET.SubElement(file_elem, "n").text = video_filename
        ET.SubElement(file_elem, "pathurl").text = pathurl
        add_rate(file_elem)
        ET.SubElement(file_elem, "duration").text = str(duration_frames)

        file_media = ET.SubElement(file_elem, "media")

        file_video = ET.SubElement(file_media, "video")
        file_samplechar = ET.SubElement(file_video, "samplecharacteristics")
        add_rate(file_samplechar)
        ET.SubElement(file_samplechar, "width").text = width
        ET.SubElement(file_samplechar, "height").text = height

        file_audio = ET.SubElement(file_media, "audio")
        file_audio_sc = ET.SubElement(file_audio, "samplecharacteristics")
        ET.SubElement(file_audio_sc, "depth").text = "16"
        ET.SubElement(file_audio_sc, "samplerate").text = "48000"
        ET.SubElement(file_audio, "channelcount").text = "2"

        # ---------------- MARKERS ----------------
        for seg in segments:
            start_frame = seconds_to_frames(float(seg.get("start", 0)))
            end_frame = seconds_to_frames(float(seg.get("end", seg.get("start", 0))))
            event_type = seg.get("sdnaEventType", "")
            text = seg.get("eventValue", "")

            marker_label = f"[{event_type}] {text}" if event_type else text

            marker = ET.SubElement(clipitem_v, "marker")
            ET.SubElement(marker, "comment").text = text
            ET.SubElement(marker, "n").text = marker_label
            ET.SubElement(marker, "in").text = str(start_frame)
            ET.SubElement(marker, "out").text = str(end_frame)

        # ---------------- AUDIO TRACKS ----------------
        audio = ET.SubElement(media, "audio")

        for track_index in [1, 2]:
            track_a = ET.SubElement(audio, "track")
            clipitem_a = ET.SubElement(
                track_a, "clipitem", id=f"clipitem-{track_index + 1}"
            )

            ET.SubElement(clipitem_a, "masterclipid").text = "masterclip-1"
            ET.SubElement(clipitem_a, "ismasterclip").text = "FALSE"
            ET.SubElement(clipitem_a, "n").text = video_filename
            ET.SubElement(clipitem_a, "enabled").text = "TRUE"
            ET.SubElement(clipitem_a, "duration").text = str(duration_frames)

            add_rate(clipitem_a)

            ET.SubElement(clipitem_a, "start").text = "0"
            ET.SubElement(clipitem_a, "end").text = str(duration_frames)
            ET.SubElement(clipitem_a, "in").text = "0"
            ET.SubElement(clipitem_a, "out").text = str(duration_frames)

            ET.SubElement(clipitem_a, "file", id="file-1")

            sourcetrack = ET.SubElement(clipitem_a, "sourcetrack")
            ET.SubElement(sourcetrack, "mediatype").text = "audio"
            ET.SubElement(sourcetrack, "trackindex").text = str(track_index)

        # -----------------------------------------------------
        # Write XML with proper header
        # -----------------------------------------------------
        pretty_xml = self._prettify_xml(xmeml)

        # Remove old declaration if exists
        if pretty_xml.startswith("<?xml"):
            pretty_xml = pretty_xml.split("?>", 1)[1].strip()

        xml_header = '<?xml version="1.0" encoding="UTF-8"?>'
        doctype = "<!DOCTYPE xmeml>"

        final_xml = f"{xml_header}\n{doctype}\n{pretty_xml}"

        with open(filepath, "w", encoding="utf-8") as f:
            f.write(final_xml)
        logger.info(f"Generated Insight XMEML file : filepath={filepath}")
        return filepath
    
    def generate_insights_edl(self, events: Dict, reel="AX", track="V") -> str:
        """Convert event list → EDL text"""

        filename = "sdna_ai_spark_insights.edl"
        event_dir = self.export_dir / "insights"
        event_dir.mkdir(parents=True, exist_ok=True)
        filepath = event_dir / filename

        # ---------------------------------------------
        # Get video path safely (optional)
        # ---------------------------------------------
        segments = events.get("segments", [])
        video_path = ""

        if segments:
            video_path = segments[0].get("fullPath", r"D:\SDNA\AI_Spark\test\India vs Pakistan t20 subclip.mp4") or ""
        video_path = r"D:\SDNA\AI_Spark\test\India vs Pakistan t20 subclip.mp4" 
        if video_path:
            video_name = Path(video_path).name
        else:
            logger.info("Video path not found. Generating EDL without clip reference.")
            video_name = ""

        edl_lines = []

        record_in = 0.0  # Running record timecode tracker
        
        for i, event in enumerate(segments, start=1):
            # Source timecodes (from original video)
            source_in = self.seconds_to_timecode(event["start"])
            source_out = self.seconds_to_timecode(event["end"])
            
            # Record timecodes (timeline position)
            duration = event["end"] - event["start"]
            record_in_tc = self.seconds_to_timecode(record_in)
            record_out_tc = self.seconds_to_timecode(record_in + duration)
            
            # EDL edit line (proper spacing for CMX 3600 format)
            line = (
                f"{i:03d}  {reel:<8} {track:<5} C        "
                f"{source_in} {source_out} {record_in_tc} {record_out_tc}"
            )
            
            edl_lines.append(line)
            edl_lines.append(f"* FROM CLIP NAME: {video_name}")
            
            # Custom metadata (non-standard but useful)
            if event.get('id'):
                edl_lines.append(f"* COMMENT ID: {event['id']}")
            if event.get('sdnaEventType'):
                edl_lines.append(f"* SDNA EVENT TYPE: {event['sdnaEventType']}")
            if event.get('eventValue'):
                edl_lines.append(f"* EVENT VALUE: {event['eventValue']}")
            
            edl_lines.append("")  # Blank line between events
            
            # Update record position for next event
            record_in += duration
        
        # Write EDL file
        edl_content = (
            f"TITLE: AI Spark Events\n"
            f"FCM: NON-DROP FRAME\n\n"
            f"{chr(10).join(edl_lines)}"
        )
        
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(edl_content)
        logger.info(f"Generated insights edl : filepath={filepath}")
        logger.info("--------------------------------------------------")
        return edl_content

    def generate_selects_edl(
        self, selects_data: List[Dict]
    ) -> Path:
        """Generate EDL file from selects."""
        filename = "sdna_ai_spark_selects.edl"
        filepath = self.export_dir / filename

        with open(filepath, "w") as f:
            f.write("TITLE: AI Spark Selects\n")
            f.write(f"FCM: NON-DROP FRAME\n\n")
            
            for i, select in enumerate(selects_data, 1):
                start_tc = self._seconds_to_timecode(select["start_time"])
                end_tc = self._seconds_to_timecode(select["end_time"])
                
                # EDL format: event_num  reel  track  transition  duration  source_in  source_out  rec_in  rec_out
               
                f.write(f"{start_tc} {end_tc} {start_tc} {end_tc}\n")
                
                # Add comment line with label
                if "label" in select:
                    f.write(f"* FROM CLIP NAME: {select['label']}\n")
                
                f.write("\n")

        logger.info(f"Generated selects EDL : filepath={filepath}")
        return filepath

    def generate_grounding_prompt(self, user_prompt) -> Path:
        """Generate grounding prompt text for LLM."""
        try:
            filename = "sdna_ai_spark_grounding.txt"
            filepath = self.export_dir / filename

            with open(filepath, "w") as f:
                f.write(user_prompt)

            logger.info(f"Generated grounding prompt : filepath={filepath}")
            return filepath, "Success"
        except Exception as e:
            logger.error(f"Error occure when writting in grounding file : {e}")
            return None, f"Error occure when writting in grounding file : {e}"

    def generate_llm_instructions(self) -> Path:
        """Generate LLM instructions text file."""
        try:
            filename = "sdna_ai_spark_llm_instructions.md"
            filepath = self.export_dir / filename

            content = """
# SDNA AI Spark - Event Analysis Export Instructions

## Your Task

You have been provided with JSON files containing events and transcripts from a video analysis.

Your job is to:
1. Analyze the events and transcript based on the user's request
2. Create NEW events, modify existing events, or merge events as needed
3. **Export your results as a downloadable `.json` FILE in the EXACT format specified below - NO VARIATIONS ALLOWED**
4. Export a JSON file only when the user explicitly requests it. Generate a `.json` file only if the user asks to export in JSON format.

---

## ⚠️ CRITICAL: You MUST Follow the Exact Schema

**READ THIS CAREFULLY:**

The export format is **NOT NEGOTIABLE**. You cannot:
- ❌ Create your own "better" or "more readable" structure
- ❌ Add extra fields you think would be helpful
- ❌ Reorganize the data in a way that makes more sense to you
- ❌ Nest data differently than specified
- ❌ Use different field names
- ❌ Output raw JSON text in the chat — always produce a `.json` file

**The schema below is MANDATORY. Follow it EXACTLY.**

If you deviate from this schema, your export will fail to import and the user's work will be lost.

---

## MANDATORY Export Format Rules

When you export, you MUST follow ALL of these rules:

### File Requirements

1. ✅ **Create a `.json` file** — do NOT output raw JSON text into the chat
2. ✅ **Save the file** to `/mnt/user-data/outputs/` and use the `present_files` tool so the user can download it
3. ✅ **Suggested filename**: `sdna_export.json` (or a descriptive name if context warrants it)
4. ✅ **The file must contain ONLY valid JSON** — no markdown, no comments, no explanatory text inside the file
5. ✅ **File starts with `{` and ends with `}`** — nothing before or after inside the file

### Schema Requirements

6. ✅ **Use ONLY the fields specified** in the schema below
7. ✅ **Do NOT add extra fields** (no matter how useful they seem)
8. ✅ **Do NOT rename fields** (e.g., "eventType" instead of "sdnaEventType")
9. ✅ **Do NOT reorganize structure** (e.g., grouping events by type)
10. ✅ **Match data types exactly** (numbers as numbers, not strings)

---

## Required JSON Schema - FOLLOW EXACTLY

```json
{
  "segments": [
    {
      "insight": "string",
      "start": "float value",
      "end": "float value",
      "confidenceScore": "float value",
      "eventMeta": {
        "associatedEventIds": [
          "event/trascript id",
          "event/trascript id"
        ]
      }
    }
  ]
}
```

### Root Level Fields (REQUIRED)

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `segments` | array | YES | Array of event segment objects |

### segments Array Items (REQUIRED)

Each object in the segments array MUST have exactly these fields:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `insight` | string | YES | The detected/analyzed content |
| `start` | number | YES | Start time in seconds (NOT a string) |
| `end` | number | YES | End time in seconds (NOT a string) |
| `confidenceScore` | number or null | YES | Integer 0-100, or null |
| `eventMeta` | object | YES | Metadata object containing associated event references |
| `eventMeta.associatedEventIds` | array | YES | Array of event/transcript IDs. Can be an empty array [] if no associations exist. |

**Do NOT add any other fields to segment objects.**

---

## ❌ WRONG - Examples of What NOT to Do

### WRONG Example 1: Outputting raw JSON text in chat

```
Here is your export:

{
  "segments": [...]
}
```

**WHY THIS IS WRONG:**
- The JSON must be saved as a `.json` file and presented via `present_files`
- Raw text in chat cannot be imported by the system

### WRONG Example 2: Custom Schema

```json
{
  "analysis_date": "2026-02-16T07:32:20Z",
  "celebrities_analyzed": 3,
  "celebrities": [...]
}
```

**WHY THIS IS WRONG:**
- Missing `segments` array
- Created custom structure
- Added unauthorized fields

### WRONG Example 3: Added Extra Fields

```json
{
  "analysisMetadata": {
    "analyzedBy": "claude",
    "totalEvents": 5
  },
  "segments": [...]
}
```

**WHY THIS IS WRONG:**
- Added `analysisMetadata` wrapper (not in schema)
- Root object must have ONLY `segments`

### WRONG Example 4: String Numbers

```json
{
  "segments": [
    {
      "start": "1239.44",
      "end": "1334.92",
      "confidenceScore": "95"
    }
  ]
}
```

**WHY THIS IS WRONG:**
- `start`, `end`, and `confidenceScore` are strings instead of numbers

---

## ✅ CORRECT - Valid Export Example

The file `/mnt/user-data/outputs/sdna_export.json` should contain:

```json
{
  "segments": [
    {
      "insight": "Hook: celebrities behind bars",
      "start": 4.56,
      "end": 15.08,
      "confidenceScore": 0.91,
      "eventMeta": {
        "associatedEventIds": [
          "6970621cffa3a37fa17111c6",
          "6970621cffa3a37fa1711343"
        ]
      }
    },
    {
      "insight": "celebrities dancing",
      "start": 16.56,
      "end": 20.08,
      "confidenceScore": 0.81,
      "eventMeta": {
        "associatedEventIds": [
          "6970621cffa3a37fa17111c7",
          "6970621cffa3a37fa1711353"
        ]
      }
    }
  ]
}
```

**WHY THIS IS CORRECT:**
- Saved as a `.json` file with `present_files` tool
- Follows exact schema structure
- All required fields present
- No extra fields added
- Numbers are numbers (not strings)
- File contains pure JSON only

---

## Schema Validation Checklist

Before exporting, verify EVERY item on this checklist:

### Structure
- [ ] Root object has ONLY 1 field: `segments`
- [ ] Each segment has ONLY 5 fields: `insight`, `start`, `end`, `confidenceScore`, `eventMeta`
- [ ] `eventMeta` contains ONLY one field: `associatedEventIds`
- [ ] `associatedEventIds` exists ONLY inside `eventMeta` (NOT at segment root level)

### Data Types
- [ ] `insight` is a string
- [ ] `start` is a number (NOT a string)
- [ ] `end` is a number (NOT a string)
- [ ] `confidenceScore` is a number or null (NOT a string)
- [ ] `eventMeta` is an object
- [ ] `associatedEventIds` is an array
- [ ] Every value inside `associatedEventIds` is a string

### Values
- [ ] All `associatedEventIds` values are unique within each segment
- [ ] All `associatedEventIds` are valid 24-character hexadecimal strings
- [ ] All `start` values are >= 0
- [ ] All `end` values are > their corresponding start values
- [ ] All `confidenceScore` values are 0-100 or null

### File
- [ ] File is saved to `/mnt/user-data/outputs/` with a `.json` extension
- [ ] `present_files` tool is called so the user can download it
- [ ] File contains ONLY valid JSON (no markdown, no text)
- [ ] File starts with `{` and ends with `}`
- [ ] No trailing commas
- [ ] No extra fields anywhere in the object

---

## Two-Step Export Process

### Step 1: Analysis & Discussion (Natural Conversation)

First, analyze the content and discuss findings with the user naturally.

**During this phase:**
- Answer questions conversationally
- Explain what you discovered
- Ask clarifying questions if needed
- Iterate and refine your analysis
- Use readable formats (lists, tables, prose)
- Be helpful and thorough

**DO NOT export during this phase.**

### Step 2: Export (JSON File Only)

When the user explicitly requests export with phrases like:
- "Export the results"
- "Give me the JSON"
- "Ready to export"
- "Export now"

**At that exact moment:**
1. Write the valid JSON to `/mnt/user-data/outputs/sdna_export.json`
2. Call the `present_files` tool with that path so the user can download it
3. Say nothing else — no explanations, no summaries, no extra text

---

## Common LLM Mistakes to Avoid

1. **Outputting JSON as plain text** — Always write a file; never paste raw JSON into chat
2. **"Helpful" restructuring** — Don't reorganize data to make it "easier to read"
3. **Extra context fields** — Don't add helpful fields like `summary`, `category`, `notes`
4. **Nested grouping** — Don't group segments by type, time period, or any other criteria
5. **Explanatory wrapper** — Don't add `results`, `data`, or `output` wrapper objects
6. **String numbers** — Don't quote numeric values
7. **Mixed content** — Don't combine JSON with explanatory text inside the file
8. **Forgetting `present_files`** — Always call `present_files` after writing the file

---

## If You're Unsure

**When in doubt, ask yourself:**

1. "Have I written the output to a `.json` file (not pasted it as text)?"
2. "Did I call `present_files` so the user can download it?"
3. "Does my file content match the CORRECT example exactly in structure?"
4. "Have I added ANY fields not in the schema?"
5. "Are ALL my field names exactly as specified?"

**If the answer to ANY of these is "no" or "I'm not sure" — STOP and fix it.**

---

## Analysis Tips

When analyzing the provided files during Step 1:

1. **Cross-reference events with transcript**: Provides context for timing
2. **Look for patterns**: Similar events clustered together may indicate themes
3. **Consider confidence scores**: Higher scores (>80) are more reliable
4. **Temporal alignment**: Events at the same time may be related
5. **Create meaningful insight text**: Put all relevant info in the insight string since you cannot add custom fields

**Remember**: All your analysis insights must fit into the `insight` field as a descriptive string. You cannot add additional fields to store structured data.

---

## Final Reminder

**The export schema is not a suggestion — it is a requirement.**

Your output will be parsed by automated systems that expect this exact structure. Any deviation will cause import failures and data loss.

When you're ready to export:
1. Write the JSON to `/mnt/user-data/outputs/sdna_export.json`
2. Call `present_files` with that path
3. Say nothing else

Good luck with your analysis!
"""
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(content.strip())

            logger.info(f"Generated LLM instructions : filepath={filepath}")
            return filepath, "Success"
        except Exception as e:
            logger.error(f"Error Occure when writting in llm instruct file : {e}")
            return None, f"Error Occure when writting in llm instruct file : {e}"

    def _format_srt_time(self, seconds: float) -> str:
        """Format seconds as SRT timestamp (HH:MM:SS,mmm)."""
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)
        millis = int((seconds % 1) * 1000)
        return f"{hours:02d}:{minutes:02d}:{secs:02d},{millis:03d}"

    def _format_vtt_time(self, seconds: float) -> str:
        """Format seconds as WebVTT timestamp (HH:MM:SS.mmm)."""
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)
        millis = int((seconds % 1) * 1000)
        return f"{hours:02d}:{minutes:02d}:{secs:02d}.{millis:03d}"

    def _seconds_to_timecode(self, seconds: float, fps: int = 30) -> str:
        """Convert seconds to SMPTE timecode."""
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)
        frames = int((seconds % 1) * fps)
        return f"{hours:02d}:{minutes:02d}:{secs:02d}:{frames:02d}"

    def _seconds_to_fcpx_time(self, sec: float) -> str:
        return f"{sec:.3f}s"

    def _fps_to_frame_duration(self, fps: str) -> str:
        num, den = fps.split("/")
        return f"{den}/{num}s"

    def _prettify_xml(self, element: ET.Element) -> str:
        rough = ET.tostring(element, "utf-8")
        reparsed = minidom.parseString(rough)
        return reparsed.toprettyxml(indent="  ")
    
    def _get_video_metadata(self, video_path: str) -> dict:
        """Extract duration, fps, width, height using ffprobe."""
        cmd = [
            "ffprobe",
            "-v", "error",
            "-print_format", "json",
            "-show_streams",
            "-show_format",
            video_path
        ]

        result = subprocess.run(cmd, capture_output=True, text=True)
        data = json.loads(result.stdout)

        video_stream = next(s for s in data["streams"] if s["codec_type"] == "video")

        duration = float(data["format"]["duration"])
        width = int(video_stream["width"])
        height = int(video_stream["height"])

        # fps like "30000/1001"
        fps = video_stream["r_frame_rate"]

        return {
            "duration": duration,
            "width": width,
            "height": height,
            "fps": fps
        }
        
        
