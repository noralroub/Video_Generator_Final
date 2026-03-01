"""
Simulation module for testing video generation without running the actual pipeline.

This module provides functions to simulate pipeline progress by creating the files
and database records that the status system checks, allowing testing without API costs.
"""

import json
import time
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from django.conf import settings
from django.utils import timezone as django_timezone

logger = logging.getLogger(__name__)


def create_step_files(output_dir: Path, step_name: str):
    """Create files that indicate a pipeline step has completed."""
    output_dir.mkdir(parents=True, exist_ok=True)
    
    if step_name == "fetch-paper":
        # Step 1: Paper fetched
        paper_file = output_dir / "paper.json"
        paper_file.write_text(json.dumps({
            "pmid": output_dir.name,
            "title": "Simulated Paper Title",
            "authors": ["Simulated Author 1", "Simulated Author 2"],
            "abstract": "This is a simulated paper for testing status updates.",
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }, indent=2))
        logger.info(f"Simulated: Created {paper_file}")
        
    elif step_name == "generate-script":
        # Step 2: Script generated
        # First ensure paper.json exists (prerequisite)
        if not (output_dir / "paper.json").exists():
            create_step_files(output_dir, "fetch-paper")
        
        script_file = output_dir / "script.json"
        script_file.write_text(json.dumps({
            "scenes": [
                {
                    "scene_number": 0,
                    "narration": "Simulated scene narration for testing.",
                    "visual_description": "A simulated visual description",
                }
            ],
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }, indent=2))
        logger.info(f"Simulated: Created {script_file}")
        
    elif step_name == "generate-audio":
        # Step 3: Audio generated
        # Ensure prerequisites exist
        if not (output_dir / "script.json").exists():
            create_step_files(output_dir, "generate-script")
        
        audio_file = output_dir / "audio.wav"
        # Create a minimal valid WAV file (header only, no actual audio)
        wav_header = b'RIFF' + (36).to_bytes(4, 'little') + b'WAVE' + \
                     b'fmt ' + (16).to_bytes(4, 'little') + \
                     (1).to_bytes(2, 'little') + (1).to_bytes(2, 'little') + \
                     (44100).to_bytes(4, 'little') + (88200).to_bytes(4, 'little') + \
                     (2).to_bytes(2, 'little') + (16).to_bytes(2, 'little') + \
                     b'data' + (0).to_bytes(4, 'little')
        audio_file.write_bytes(wav_header)
        logger.info(f"Simulated: Created {audio_file}")
        
        metadata_file = output_dir / "audio_metadata.json"
        metadata_file.write_text(json.dumps({
            "duration": 5.0,
            "sample_rate": 44100,
            "format": "wav",
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }, indent=2))
        logger.info(f"Simulated: Created {metadata_file}")
        
    elif step_name == "generate-videos":
        # Step 4: Videos generated
        # Ensure prerequisites exist
        if not (output_dir / "audio.wav").exists():
            create_step_files(output_dir, "generate-audio")
        
        clips_dir = output_dir / "clips"
        clips_dir.mkdir(parents=True, exist_ok=True)
        
        # Create marker file that indicates videos are complete
        marker_file = clips_dir / ".videos_complete"
        marker_file.write_text("videos generated")
        logger.info(f"Simulated: Created {marker_file}")
        
        # Also create a dummy video metadata file
        video_metadata = clips_dir / "video_metadata.json"
        video_metadata.write_text(json.dumps({
            "clips": ["clip_00.mp4"],
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }, indent=2))
        logger.info(f"Simulated: Created {video_metadata}")
        
    else:
        raise ValueError(f"Unknown step: {step_name}")


def update_job_progress(job, step_name: str):
    """Update a VideoGenerationJob record with progress for a step."""
    from django.db import connections
    
    # Map step names to progress percentages (4 steps: 25%, 50%, 75%, 100%)
    step_progress = {
        "fetch-paper": (25, "fetch-paper"),
        "generate-script": (50, "generate-script"),
        "generate-audio": (75, "generate-audio"),
        "generate-videos": (100, None),  # None means completed, no current step
    }
    
    progress_percent, current_step = step_progress.get(step_name, (0, step_name))
    status = "completed" if progress_percent == 100 else "running"
    
    try:
        # Close any stale database connections
        connections.close_all()
        
        # Refresh job from database to avoid stale data
        job.refresh_from_db()
        
        # Use progress manager for updates
        try:
            from web.progress_manager import update_progress
            update_progress(
                task_id=task_id,
                progress_percent=progress_percent,
                current_step=current_step,
                status=status
            )
            # Refresh to get updated values
            job.refresh_from_db()
            
            # Handle completion-specific fields
            if status == "completed":
                recorded = Path(settings.MEDIA_ROOT) / job.paper_id / "recorded.mp4"
                if recorded.exists():
                    job.final_video_path = str(recorded)
                    job.save(update_fields=['final_video_path', 'updated_at'])
        except Exception as e:
            logger.warning(f"Failed to update progress via manager in simulation, updating directly: {e}")
            # Fallback to direct update
            job.status = status
            job.progress_percent = progress_percent
            job.current_step = current_step
            
            if status == "completed":
                job.completed_at = django_timezone.now()
                recorded = Path(settings.MEDIA_ROOT) / job.paper_id / "recorded.mp4"
                if recorded.exists():
                    job.final_video_path = str(recorded)
            
            job.save(update_fields=['status', 'progress_percent', 'current_step', 'completed_at', 'final_video_path', 'updated_at'])
        logger.info(f"Updated job record: {status} - {progress_percent}% - {current_step}")
    except Exception as e:
        logger.error(f"Error updating job progress: {e}")
        raise  # Re-raise so simulation knows about the error


def simulate_pipeline_progress(pmid: str, output_dir: Path, task_id: str, job=None, delay_per_step: float = 3.0):
    """
    Simulate pipeline progress by creating files and updating job records.
    
    This function progresses through all pipeline steps with delays between each step,
    creating the files that the status system checks.
    
    Args:
        pmid: Paper ID
        output_dir: Output directory path
        task_id: Celery task ID
        job: Optional VideoGenerationJob instance to update
        delay_per_step: Delay in seconds between steps (default: 3.0)
    """
    steps = ["fetch-paper", "generate-script", "generate-audio", "generate-videos"]
    
    logger.info(f"Starting simulation for {pmid} (task {task_id})")
    
    # Create task_result.json file and log file
    task_result_file = output_dir / "task_result.json"
    log_path = output_dir / "pipeline.log"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Write initial log entry (use UTF-8 encoding to avoid Windows charmap issues)
    with open(log_path, "a", encoding="utf-8") as log_file:
        log_file.write(f"[SIMULATION MODE] Starting simulated pipeline for {pmid}\n")
        log_file.write(f"[SIMULATION MODE] Task ID: {task_id}\n")
        log_file.write(f"[SIMULATION MODE] This is a simulation - no actual video generation is occurring\n\n")
    
    for step in steps:
        step_name = step.replace("-", " ").title()
        logger.info(f"Simulating step: {step}")
        
        # Write to log file (use UTF-8 encoding)
        with open(log_path, "a", encoding="utf-8") as log_file:
            log_file.write(f"[SIMULATION] Step: {step_name}\n")
            log_file.write(f"[SIMULATION] Progress: {steps.index(step) + 1}/{len(steps)}\n")
        
        # Create files for this step (and prerequisites if needed)
        try:
            create_step_files(output_dir, step)
        except Exception as e:
            logger.error(f"Failed to create files for step {step}: {e}")
            raise  # Re-raise file creation errors - these are critical
        
        # Update task result file
        status = "completed" if step == steps[-1] else "running"
        task_result = {
            "status": status,
            "pmid": pmid,
            "output_dir": str(output_dir),
            "task_id": task_id,
            "error": None,
            "error_type": None,
        }
        task_result_file.write_text(json.dumps(task_result, indent=2))
        
        # Update job record if provided
        if job:
            try:
                # Refresh job from database to avoid stale data issues
                from web.models import VideoGenerationJob
                job.refresh_from_db()
                update_job_progress(job, step)
            except Exception as e:
                logger.warning(f"Failed to update job record for step {step}: {e}")
                # Don't fail the simulation if job update fails - continue anyway
        
        # Write completion to log (use UTF-8 encoding)
        with open(log_path, "a", encoding="utf-8") as log_file:
            if step == steps[-1]:
                log_file.write(f"[SIMULATION] [OK] All steps complete!\n")
                log_file.write(f"[SIMULATION MODE] Simulation finished successfully\n")
            else:
                log_file.write(f"[SIMULATION] [OK] {step_name} complete\n\n")
        
        # Delay before next step (except after last step)
        if step != steps[-1]:
            time.sleep(delay_per_step)
    
    logger.info(f"Simulation complete for {pmid}")

