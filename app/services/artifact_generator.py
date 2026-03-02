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

    # def generate_transcript_fcpxml(self, transcript_data: Dict) -> Path:
    #     """Generate FCPXML for transcript (segments optional)."""
    #     segments: List[Dict] = transcript_data.get("segments", [])
    #     # ---------------------------------------------
    #     # Get optional video path
    #     # ---------------------------------------------
    #     video_path = segments[0].get("fullPath") if segments else None
    #     video_path = r"D:\SDNA\AI_Spark\test\36_celebraties.mp4"  
    #     meta = None
    #     if os.path.exists(video_path):
    #         meta = self._get_video_metadata(video_path)

    #         frame_duration = self._fps_to_frame_duration(meta["fps"])
    #         video_duration = self._seconds_to_fcpx_time(meta["duration"])
    #         width = str(meta["width"])
    #         height = str(meta["height"])
    #     else:
    #         # ---------------------------------------------
    #         # Safe fallback when NO segments or NO video
    #         # ---------------------------------------------
    #         frame_duration = "1/25s"

    #         if segments:
    #             duration_seconds = max(float(seg["end"]) for seg in segments)
    #         else:
    #             duration_seconds = 1  # minimal valid duration

    #         video_duration = self._seconds_to_fcpx_time(duration_seconds)
    #         width = "1920"
    #         height = "1080"

    #     # ---------------------------------------------
    #     # File path
    #     # ---------------------------------------------
    #     filename = "sdna_ai_spark_transcript.fcpxml"
    #     event_dir = self.export_dir / "transcript"
    #     event_dir.mkdir(parents=True, exist_ok=True)
    #     filepath = event_dir / filename

    #     # -----------------------------------------------------
    #     # Root
    #     # -----------------------------------------------------
    #     fcpxml = ET.Element("fcpxml", version="1.10")

    #     # -----------------------------------------------------
    #     # Resources
    #     # -----------------------------------------------------
    #     resources = ET.SubElement(fcpxml, "resources")

    #     ET.SubElement(
    #         resources,
    #         "format",
    #         id="r1",
    #         name=f"FFVideoFormat{height}p",
    #         frameDuration=frame_duration,
    #         width=width,
    #         height=height,
    #     )

    #     if video_path:
    #         ET.SubElement(
    #             resources,
    #             "asset",
    #             id="r2",
    #             name=Path(video_path).name,
    #             src=video_path,
    #             start="0s",
    #             duration=video_duration,
    #             hasVideo="1",
    #             hasAudio="1",
    #             format="r1",
    #         )

    #     # -----------------------------------------------------
    #     # Library structure
    #     # -----------------------------------------------------
    #     library = ET.SubElement(fcpxml, "library")
    #     event = ET.SubElement(library, "event", name="AI transcript Event")
    #     project = ET.SubElement(event, "project", name="AI transcript Project")

    #     sequence = ET.SubElement(
    #         project,
    #         "sequence",
    #         duration=video_duration,
    #         format="r1",
    #     )

    #     spine = ET.SubElement(sequence, "spine")

    #     # asset-clip only if video exists
    #     asset_clip = (
    #         ET.SubElement(
    #             spine,
    #             "asset-clip",
    #             name="video",
    #             ref="r2",
    #             offset="0s",
    #             start="0s",
    #             duration=video_duration,
    #         )
    #         if video_path
    #         else spine
    #     )

    #     # -----------------------------------------------------
    #     # Add markers ONLY if segments exist
    #     # -----------------------------------------------------
    #     for seg in segments:
    #         seg_id = seg.get("id", "")
    #         start = float(seg.get("start", 0))
    #         end = float(seg.get("end", start))
    #         text = seg.get("eventValue", "")
    #         event_type = seg.get("sdnaEventType", "")

    #         ET.SubElement(
    #             asset_clip,
    #             "marker",
    #             id=seg_id,
    #             start=self._seconds_to_fcpx_time(start),
    #             end=self._seconds_to_fcpx_time(end),
    #             duration=self._seconds_to_fcpx_time(max(end - start, 0)),
    #             sdnaEventType=event_type,
    #             value=text,
    #         )

    #     # -----------------------------------------------------
    #     # Write XML
    #     # -----------------------------------------------------
    #     pretty_xml = self._prettify_xml(fcpxml)
    #     logger.info(f"Generated transcript fcpxml : filepath={filepath}")
    #     logger.info("--------------------------------------------------")
    #     with open(filepath, "w", encoding="utf-8") as f:
    #         f.write(pretty_xml)

    #     return filepath
    def generate_transcript_fcpxml(self, transcript_data: Dict) -> Path:
        """Generate XMEML (FCP7/Premiere) XML for transcript (segments optional)."""
        segments: List[Dict] = transcript_data.get("segments", [])

        # ---------------------------------------------
        # Get optional video path
        # ---------------------------------------------
        video_path = segments[0].get("fullPath") if segments else None
        video_path = r"D:\SDNA\AI_Spark\test\36_celebraties.mp4"
        meta = None
        if os.path.exists(video_path):
            meta = self._get_video_metadata(video_path)
            # fps may be an int, float, or fraction string like '25/1' or '30000/1001'
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
            if segments:
                duration_seconds = max(float(seg["end"]) for seg in segments)
            else:
                duration_seconds = 1
            duration_frames = int(duration_seconds * fps)
            width = "1920"
            height = "1080"

        # Helper: convert seconds → frames
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

        # Convert file path to file URL (cross-platform)
        if video_path:
            pathurl = Path(video_path).as_uri()  # e.g. file:///D:/SDNA/.../36_celebraties.mp4
        else:
            pathurl = ""

        # -----------------------------------------------------
        # Build XML tree
        # -----------------------------------------------------
        xmeml = ET.Element("xmeml", version="4")
        sequence = ET.SubElement(xmeml, "sequence")

        ET.SubElement(sequence, "n").text = "AI Events Project"
        ET.SubElement(sequence, "duration").text = str(duration_frames)

        def add_rate(parent, timebase=None, ntsc="FALSE"):
            rate = ET.SubElement(parent, "rate")
            ET.SubElement(rate, "timebase").text = str(timebase or fps)
            ET.SubElement(rate, "ntsc").text = ntsc

        add_rate(sequence)

        # Timecode
        timecode = ET.SubElement(sequence, "timecode")
        add_rate(timecode)
        ET.SubElement(timecode, "string").text = "00:00:00:00"
        ET.SubElement(timecode, "frame").text = "0"
        ET.SubElement(timecode, "displayformat").text = "NDF"

        # Media
        media = ET.SubElement(sequence, "media")

        # ---- Video track ----
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
        ET.SubElement(clipitem_v, "n").text = video_filename
        ET.SubElement(clipitem_v, "duration").text = str(duration_frames)
        add_rate(clipitem_v)
        ET.SubElement(clipitem_v, "start").text = "0"
        ET.SubElement(clipitem_v, "end").text = str(duration_frames)
        ET.SubElement(clipitem_v, "in").text = "0"
        ET.SubElement(clipitem_v, "out").text = str(duration_frames)

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

        # ---- Markers ----
        for seg in segments:
            start_frame = seconds_to_frames(float(seg.get("start", 0)))
            end_frame = seconds_to_frames(float(seg.get("end", seg.get("start", 0))))
            event_type = seg.get("sdnaEventType", "")
            text = seg.get("eventValue", "")

            marker_label = f"[{event_type}] {text}" if event_type else text

            marker = ET.SubElement(clipitem_v, "marker")
            ET.SubElement(marker, "n").text = marker_label
            ET.SubElement(marker, "in").text = str(start_frame)
            ET.SubElement(marker, "out").text = str(end_frame)

        # ---- Audio tracks ----
        audio = ET.SubElement(media, "audio")
        for track_index in [1, 2]:
            clip_id = f"clipitem-{track_index + 1}"
            track_a = ET.SubElement(audio, "track")
            clipitem_a = ET.SubElement(track_a, "clipitem", id=clip_id)
            ET.SubElement(clipitem_a, "n").text = video_filename
            ET.SubElement(clipitem_a, "duration").text = str(duration_frames)
            add_rate(clipitem_a)
            ET.SubElement(clipitem_a, "start").text = "0"
            ET.SubElement(clipitem_a, "end").text = str(duration_frames)
            ET.SubElement(clipitem_a, "in").text = "0"
            ET.SubElement(clipitem_a, "out").text = str(duration_frames)
            ET.SubElement(clipitem_a, "file", id="file-1")  # reference only
            sourcetrack = ET.SubElement(clipitem_a, "sourcetrack")
            ET.SubElement(sourcetrack, "mediatype").text = "audio"
            ET.SubElement(sourcetrack, "trackindex").text = str(track_index)

        # -----------------------------------------------------
        # Write XML with DOCTYPE declaration
        # -----------------------------------------------------
        pretty_xml = self._prettify_xml(xmeml)

        # Inject DOCTYPE after the XML declaration
        xml_declaration = '<?xml version="1.0" encoding="UTF-8"?>'
        doctype = "<!DOCTYPE xmeml>"
        if pretty_xml.startswith("<?xml"):
            pretty_xml = pretty_xml.replace(
                xml_declaration,
                f"{xml_declaration}\n{doctype}",
                1,
            )
        else:
            pretty_xml = f"{xml_declaration}\n{doctype}\n{pretty_xml}"

        logger.info(f"Generated transcript xmeml : filepath={filepath}")
        logger.info("--------------------------------------------------")
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(pretty_xml)

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
        """Generate FCPXML for Events (segments optional)."""

        segments: List[Dict] = event_data.get("segments", [])

        # ---------------------------------------------
        # Get optional video path
        # ---------------------------------------------
        video_path = segments[0].get("fullPath") if segments else None
        video_path = r"D:\SDNA\AI_Spark\test\36_celebraties.mp4" 
        meta = None
        if os.path.exists(video_path):
            meta = self._get_video_metadata(video_path)

            frame_duration = self._fps_to_frame_duration(meta["fps"])
            video_duration = self._seconds_to_fcpx_time(meta["duration"])
            width = str(meta["width"])
            height = str(meta["height"])
        else:
            # ---------------------------------------------
            # Safe fallback when NO segments or NO video
            # ---------------------------------------------
            frame_duration = "1/25s"

            if segments:
                duration_seconds = max(float(seg["end"]) for seg in segments)
            else:
                duration_seconds = 1  # minimal valid duration

            video_duration = self._seconds_to_fcpx_time(duration_seconds)
            width = "1920"
            height = "1080"

        # ---------------------------------------------
        # File path
        # ---------------------------------------------
        filename = "sdna_ai_spark_events.fcpxml"
        event_dir = self.export_dir / "event"
        event_dir.mkdir(parents=True, exist_ok=True)
        filepath = event_dir / filename

        # -----------------------------------------------------
        # Root
        # -----------------------------------------------------
        fcpxml = ET.Element("fcpxml", version="1.10")

        # -----------------------------------------------------
        # Resources
        # -----------------------------------------------------
        resources = ET.SubElement(fcpxml, "resources")

        ET.SubElement(
            resources,
            "format",
            id="r1",
            name=f"FFVideoFormat{height}p",
            frameDuration=frame_duration,
            width=width,
            height=height,
        )

        if video_path:
            ET.SubElement(
                resources,
                "asset",
                id="r2",
                name=Path(video_path).name,
                src=video_path,
                start="0s",
                duration=video_duration,
                hasVideo="1",
                hasAudio="1",
                format="r1",
            )

        # -----------------------------------------------------
        # Library structure
        # -----------------------------------------------------
        library = ET.SubElement(fcpxml, "library")
        event = ET.SubElement(library, "event", name="AI Events Event")
        project = ET.SubElement(event, "project", name="AI Events Project")

        sequence = ET.SubElement(
            project,
            "sequence",
            duration=video_duration,
            format="r1",
        )

        spine = ET.SubElement(sequence, "spine")

        # asset-clip only if video exists
        asset_clip = (
            ET.SubElement(
                spine,
                "asset-clip",
                name="video",
                ref="r2",
                offset="0s",
                start="0s",
                duration=video_duration,
            )
            if video_path
            else spine
        )

        # -----------------------------------------------------
        # Add markers ONLY if segments exist
        # -----------------------------------------------------
        for seg in segments:
            seg_id = seg.get("id", "")
            start = float(seg.get("start", 0))
            end = float(seg.get("end", start))
            text = seg.get("eventValue", "")
            event_type = seg.get("sdnaEventType", "")

            ET.SubElement(
                asset_clip,
                "marker",
                id=seg_id,
                start=self._seconds_to_fcpx_time(start),
                end=self._seconds_to_fcpx_time(end),
                duration=self._seconds_to_fcpx_time(max(end - start, 0)),
                sdnaEventType=event_type,
                value=text,
            )

        # -----------------------------------------------------
        # Write XML
        # -----------------------------------------------------
        pretty_xml = self._prettify_xml(fcpxml)
        logger.info(f"Generated events fcpxml : filepath={filepath}")
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(pretty_xml)

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
            video_path = segments[0].get("fullPath", r"D:\SDNA\AI_Spark\test\36_celebraties.mp4") or ""
        video_path = r"D:\SDNA\AI_Spark\test\36_celebraties.mp4" 
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
        """Generate FCPXML for insights (segments optional)."""

        segments: List[Dict] = insights_data.get("segments", [])

        # ---------------------------------------------
        # Get optional video path
        # ---------------------------------------------
        video_path = segments[0].get("fullPath") if segments else None
        video_path = r"D:\SDNA\AI_Spark\test\36_celebraties.mp4" 
        meta = None
        if os.path.exists(video_path):
            meta = self._get_video_metadata(video_path)

            frame_duration = self._fps_to_frame_duration(meta["fps"])
            video_duration = self._seconds_to_fcpx_time(meta["duration"])
            width = str(meta["width"])
            height = str(meta["height"])
        else:
            # ---------------------------------------------
            # Safe fallback when NO segments or NO video
            # ---------------------------------------------
            frame_duration = "1/25s"

            if segments:
                duration_seconds = max(float(seg["end"]) for seg in segments)
            else:
                duration_seconds = 1  # minimal valid duration

            video_duration = self._seconds_to_fcpx_time(duration_seconds)
            width = "1920"
            height = "1080"

        # ---------------------------------------------
        # File path
        # ---------------------------------------------
        filename = "sdna_ai_spark_insights.fcpxml"
        event_dir = self.export_dir / "insights"
        event_dir.mkdir(parents=True, exist_ok=True)
        filepath = event_dir / filename

        # -----------------------------------------------------
        # Root
        # -----------------------------------------------------
        fcpxml = ET.Element("fcpxml", version="1.10")

        # -----------------------------------------------------
        # Resources
        # -----------------------------------------------------
        resources = ET.SubElement(fcpxml, "resources")

        ET.SubElement(
            resources,
            "format",
            id="r1",
            name=f"FFVideoFormat{height}p",
            frameDuration=frame_duration,
            width=width,
            height=height,
        )

        if video_path:
            ET.SubElement(
                resources,
                "asset",
                id="r2",
                name=Path(video_path).name,
                src=video_path,
                start="0s",
                duration=video_duration,
                hasVideo="1",
                hasAudio="1",
                format="r1",
            )

        # -----------------------------------------------------
        # Library structure
        # -----------------------------------------------------
        library = ET.SubElement(fcpxml, "library")
        event = ET.SubElement(library, "event", name="AI insights Event")
        project = ET.SubElement(event, "project", name="AI insights Project")

        sequence = ET.SubElement(
            project,
            "sequence",
            duration=video_duration,
            format="r1",
        )

        spine = ET.SubElement(sequence, "spine")

        # asset-clip only if video exists
        asset_clip = (
            ET.SubElement(
                spine,
                "asset-clip",
                name="video",
                ref="r2",
                offset="0s",
                start="0s",
                duration=video_duration,
            )
            if video_path
            else spine
        )

        # -----------------------------------------------------
        # Add markers ONLY if segments exist
        # -----------------------------------------------------
        for seg in segments:
            seg_id = seg.get("id", "")
            start = float(seg.get("start", 0))
            end = float(seg.get("end", start))
            text = seg.get("eventValue", "")
            event_type = seg.get("sdnaEventType", "")

            ET.SubElement(
                asset_clip,
                "marker",
                id=seg_id,
                start=self._seconds_to_fcpx_time(start),
                end=self._seconds_to_fcpx_time(end),
                duration=self._seconds_to_fcpx_time(max(end - start, 0)),
                sdnaEventType=event_type,
                value=text,
            )

        # -----------------------------------------------------
        # Write XML
        # -----------------------------------------------------
        pretty_xml = self._prettify_xml(fcpxml)
        logger.info(f"Generated insights fcpxml : filepath={filepath}")
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(pretty_xml)

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
            video_path = segments[0].get("fullPath", r"D:\SDNA\AI_Spark\test\36_celebraties.mp4") or ""
        video_path = r"D:\SDNA\AI_Spark\test\36_celebraties.mp4" 
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
You are provided with the following files:

1) events.json
   Time-coded AI events detected from the video (OCR, labels, topics, sentiment, speakers, etc.)

2) transcript.json
   Time-coded transcript segments representing spoken dialogue or narration.

3) grounding.txt
   Human-written context describing what this video is and how it should be interpreted.

----------------------------------------------------------------
YOUR ROLE
----------------------------------------------------------------
Help the user explore, analyze, and curate this content using ONLY the provided files.

You are NOT an editor.
You are a reasoning and planning assistant.

All conclusions must be grounded in the supplied data.

----------------------------------------------------------------
GLOBAL RULES (ALWAYS ON)
----------------------------------------------------------------
- Use ONLY the provided JSON files and grounding.txt
- Do NOT use external knowledge
- Do NOT invent facts or details
- When making a claim, reference evidence where possible:
  - timestamps (start–end in seconds)
  - transcript segment IDs
  - event IDs (if available)
- If information is ambiguous or weak, say so clearly

----------------------------------------------------------------
OPERATING MODES
----------------------------------------------------------------
You operate in TWO MODES: EXPLORE MODE and EXPORT MODE.

----------------------------------------------------------------
MODE 1 — EXPLORE MODE (DEFAULT)
----------------------------------------------------------------
This is the default mode.

Behavior:
- Respond normally using text, bullets, or tables
- Explain reasoning and options
- Propose ideas, highlights, summaries, or groupings
- Cite evidence where possible (timestamps, IDs)

Examples of allowed requests:
- "Which celebrities are discussed and in what context?"
- "Find moments related to crime or incarceration"
- "Suggest highlight clips for a trailer"
- "Group segments by theme or topic"

Output format:
- Normal conversational text
- No schema requirements
- No JSON-only restriction

----------------------------------------------------------------
MODE 2 — EXPORT MODE (STRICT PREP MODE)
----------------------------------------------------------------
Export Mode is entered ONLY when the user types this exact line:

MODE: EXPORT

Behavior in Export Mode:
- Stop long explanations
- Prepare to generate a structured import package
- You may ask brief clarifying questions if needed
- You may confirm assumptions briefly

IMPORTANT:
DO NOT output JSON yet.

----------------------------------------------------------------
FINAL EXPORT TRIGGER
----------------------------------------------------------------
Even in Export Mode, DO NOT output JSON until the user types this exact line:

EXPORT_JSON

----------------------------------------------------------------
WHEN EXPORT_JSON IS RECEIVED
----------------------------------------------------------------
You must:

1) Output ONLY valid JSON
2) Output must conform exactly to sdna.spark.import.v1
3) No markdown
4) No commentary
5) No explanations
6) All times must be seconds (float)
7) Every highlight must include evidence references
8) If uncertain, return empty arrays and explain in "notes"

----------------------------------------------------------------
IMPORT SCHEMA (sdna.spark.import.v1)
----------------------------------------------------------------
Return a JSON object with this structure:

{
  "schemaVersion": "sdna.spark.import.v1",
  "asset": {
    "repo_guid": "...",
    "fullPath": "..."
  },
  "highlights": [
    {
      "start": 0.0,
      "end": 0.0,
      "title": "",
      "reason": "",
      "confidence": 0.0,
      "evidence": {
        "transcriptIds": [],
        "eventIds": [],
        "topics": []
      }
    }
  ],
  "notes": []
}

Validation rules:
- start < end
- No overlapping highlights
- All ranges must fit within asset duration (if known)
- Evidence arrays must not be empty unless explained in notes

----------------------------------------------------------------
DEFAULT ASSUMPTION
----------------------------------------------------------------
If the user never types MODE: EXPORT or EXPORT_JSON:
- Stay in Explore Mode forever
- Never output schema JSON

----------------------------------------------------------------
END OF INSTRUCTIONS
----------------------------------------------------------------
"""

            with open(filepath, "w") as f:
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
        
        
