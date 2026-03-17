"""
Thread-safe in-memory progress tracker for background video jobs.

Design (v2 — per-section progress):
  - One JobProgress entry per split_job_id
  - Each section (full_video, individual_segments, merge_segments, custom_segments)
    has its own SectionProgress so concurrent runs never overwrite each other
  - Overall % = average of all registered section percentages
  - Status endpoint returns both overall % AND a list of per-section statuses
"""

import threading
from dataclasses import dataclass, field
from typing import Dict, List, Optional


# ── Per-section state ────────────────────────────────────────────────────────

@dataclass
class SectionProgress:
    section: str        # "full_video" | "individual_segments" | "merge_segments" | "custom_segments"
    label: str          # human label shown to client, e.g. "Resizing full video (16x9)"
    progress: int = 0   # 0-100 for this section
    current_file: str = ""
    status: str = "running"   # "running" | "done" | "failed"
    ops_total: int = 1
    ops_done: int = 0


# ── Top-level job state ──────────────────────────────────────────────────────

@dataclass
class _JobProgress:
    overall: int = 0              # 0-100 rolled up from all sections
    sections: Dict[str, SectionProgress] = field(default_factory=dict)


class _ProgressTracker:
    """Singleton registry of in-progress job states."""

    def __init__(self):
        self._lock = threading.Lock()
        self._store: Dict[str, _JobProgress] = {}

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def init_job(self, job_id: str, section_ops: Dict[str, int]) -> None:
        """
        Register a job with per-section operation counts.

        section_ops = {
            "full_video": 1,
            "custom_segments": 12,   # math.ceil(duration / clip_duration)
        }
        Only sections with ops > 0 are registered.
        """
        with self._lock:
            sections = {}
            for sec, total in section_ops.items():
                if total > 0:
                    sections[sec] = SectionProgress(
                        section=sec,
                        label=sec.replace("_", " ").title(),
                        ops_total=max(1, total),
                    )
            self._store[job_id] = _JobProgress(sections=sections)

    def finish_job(self, job_id: str) -> None:
        """Force everything to 100 % / done."""
        with self._lock:
            jp = self._store.get(job_id)
            if not jp:
                return
            jp.overall = 100
            for sp in jp.sections.values():
                sp.progress = 100
                sp.status = "done"

    def clear(self, job_id: str) -> None:
        with self._lock:
            self._store.pop(job_id, None)

    # ------------------------------------------------------------------
    # Per-section updates (called from worker coroutines / threads)
    # ------------------------------------------------------------------

    def update_section(
        self,
        job_id: str,
        section: str,           # "full_video" etc.
        *,
        label: Optional[str] = None,
        current_file: Optional[str] = None,
        op_progress: int = 0,   # 0-100 within the current FFmpeg call
    ) -> None:
        """Update one section's progress. Thread-safe."""
        with self._lock:
            jp = self._store.get(job_id)
            if not jp:
                return
            sp = jp.sections.get(section)
            if not sp:
                return
            if label is not None:
                sp.label = label
            if current_file is not None:
                sp.current_file = current_file
            # Fine-grained %  within this section
            combined = sp.ops_done + op_progress / 100.0
            sp.progress = min(99, int(combined / sp.ops_total * 100))
            # Roll up overall
            jp.overall = self._calc_overall(jp)

    def complete_section_op(self, job_id: str, section: str) -> None:
        """One FFmpeg operation inside *section* finished."""
        with self._lock:
            jp = self._store.get(job_id)
            if not jp:
                return
            sp = jp.sections.get(section)
            if not sp:
                return
            sp.ops_done = min(sp.ops_done + 1, sp.ops_total)
            sp.progress = min(99, int(sp.ops_done / sp.ops_total * 100))
            if sp.ops_done >= sp.ops_total:
                sp.status = "done"
                sp.progress = 100
            jp.overall = self._calc_overall(jp)

    def fail_section(self, job_id: str, section: str) -> None:
        """Mark a section as failed and advance its counter."""
        with self._lock:
            jp = self._store.get(job_id)
            if not jp:
                return
            sp = jp.sections.get(section)
            if not sp:
                return
            sp.ops_done = min(sp.ops_done + 1, sp.ops_total)
            sp.status = "failed"
            jp.overall = self._calc_overall(jp)

    # ------------------------------------------------------------------
    # Read (called from async status endpoint — no awaiting needed)
    # ------------------------------------------------------------------

    def get(self, job_id: str) -> dict:
        """
        Return a snapshot safe to serialise as JSON.

        {
          "overall_progress": 47,
          "sections": [
            {"section": "full_video",       "label": "Resizing full video (16x9)",
             "progress": 62, "status": "running", "current_file": "...mp4"},
            {"section": "custom_segments",  "label": "Exporting Custom clip 3/12",
             "progress": 25, "status": "running", "current_file": "...mp4"},
          ]
        }
        """
        with self._lock:
            jp = self._store.get(job_id)
            if not jp:
                return {"overall_progress": 0, "sections": []}
            return {
                "overall_progress": jp.overall,
                "sections": [
                    {
                        "section": sp.section,
                        "label": sp.label,
                        "progress": sp.progress,
                        "status": sp.status,
                        "current_file": sp.current_file,
                    }
                    for sp in jp.sections.values()
                ],
            }

    # ------------------------------------------------------------------
    # Private
    # ------------------------------------------------------------------

    @staticmethod
    def _calc_overall(jp: _JobProgress) -> int:
        if not jp.sections:
            return 0
        total = sum(sp.progress for sp in jp.sections.values())
        return min(99, total // len(jp.sections))


# Module-level singleton
progress_tracker = _ProgressTracker()