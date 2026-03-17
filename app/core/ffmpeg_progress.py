"""
FFmpeg progress runner — Windows + Linux compatible.

Fixes applied:
1. os.close(fd) immediately after mkstemp  → releases Windows file lock
2. progress_file.replace("\\", "/")        → FFmpeg needs forward slashes on Windows
3. Temp file placed next to output file    → avoids TEMP paths with spaces
4. One-time INFO log confirms file is being written
"""

import logging
import os
import subprocess
import tempfile
import threading
import time
from pathlib import Path
from typing import Callable, Optional
from app.core.logging_config import logger

def _run_ffmpeg_with_progress(
    ffmpeg_cmd: list,
    total_duration: float,
    progress_callback: Optional[Callable[[int], None]] = None,
    *,
    job_id: str = "",
) -> None:
    """
    Run an FFmpeg command and stream 0-99 % progress via *progress_callback*.

    Falls back to plain subprocess.run when callback is None or duration == 0.
    Raises subprocess.CalledProcessError on FFmpeg failure.
    """
    if progress_callback is None or total_duration <= 0:
        subprocess.run(ffmpeg_cmd, check=True, capture_output=True)
        return

    # ── Create temp progress file next to the output file ───────────────
    # Reason: system TEMP on Windows often has spaces in the path which
    # confuses FFmpeg's -progress parser.  Placing the file beside the
    # output is safe because we already know that directory is writable.
    try:
        output_dir = Path(ffmpeg_cmd[-1]).parent
        output_dir.mkdir(parents=True, exist_ok=True)
        fd, progress_file = tempfile.mkstemp(
            suffix=".txt", prefix="ffprog_", dir=str(output_dir)
        )
    except Exception:
        fd, progress_file = tempfile.mkstemp(suffix=".txt", prefix="ffprog_")

    # CRITICAL on Windows: close the fd so FFmpeg can open the same file
    os.close(fd)

    # FFmpeg requires forward slashes even on Windows
    progress_file_ffmpeg = progress_file.replace("\\", "/")

    if job_id:
        logger.info(f"FFmpeg progress file : job_id={job_id} , path={progress_file}")

    # ── Inject -progress / -nostats right after "ffmpeg [-y]" ───────────
    cmd = list(ffmpeg_cmd)
    insert_pos = 2 if (len(cmd) > 1 and cmd[1] == "-y") else 1
    cmd[insert_pos:insert_pos] = ["-progress", progress_file_ffmpeg, "-nostats"]
    
    proc = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    stop_event = threading.Event()
    first_read_logged = threading.Event()

    def _poll() -> None:
        while not stop_event.is_set():
            try:
                with open(progress_file, "r", encoding="utf-8", errors="ignore") as fh:
                    content = fh.read()

                if content and not first_read_logged.is_set():
                    first_read_logged.set()
                    if job_id:
                        logger.info(
                            f"FFmpeg progress data confirmed : job_id={job_id}"
                        )

                for line in reversed(content.strip().splitlines()):
                    line = line.strip()
                    if line.startswith("out_time_ms="):
                        raw = line.split("=", 1)[1].strip()
                        if raw and raw not in ("N/A", ""):
                            try:
                                ms = int(raw)
                                if ms > 0:
                                    pct = min(99, int(ms / (total_duration * 1_000_000) * 100))
                                    progress_callback(pct)
                            except ValueError:
                                pass
                        break
            except (OSError, IOError):
                pass  # file not yet created — normal on first tick

            time.sleep(0.4)

    poll_thread = threading.Thread(target=_poll, daemon=True)
    poll_thread.start()

    stdout, stderr = proc.communicate()

    stop_event.set()
    poll_thread.join(timeout=2)

    # Push a final 99 % so the caller always sees near-complete before done
    if first_read_logged.is_set():
        progress_callback(99)

    try:
        os.unlink(progress_file)
    except OSError:
        pass

    if proc.returncode != 0:
        raise subprocess.CalledProcessError(
            proc.returncode, cmd, output=stdout, stderr=stderr
        )