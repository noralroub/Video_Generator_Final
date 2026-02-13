"""
Celery tasks for video generation pipeline.

This module contains asynchronous tasks that run the video generation pipeline.
Tasks are executed by Celery workers and survive server restarts.
"""

import json
import logging
import os
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

from celery import shared_task
from django.conf import settings
from django.utils import timezone

logger = logging.getLogger(__name__)


def _parse_pipeline_progress(line: str, current_progress: dict) -> Optional[dict]:
    """
    Parse a single line of pipeline output to detect progress updates.
    
    Args:
        line: A line from pipeline stdout/stderr
        current_progress: Current progress dict with keys:
            - progress_percent: int (0-100)
            - current_step: str or None
            - completed_steps: list of step names
            - status: str (running, failed, etc.)
    
    Returns:
        Updated progress dict if progress changed, None otherwise
    """
    line_lower = line.lower()
    
    # Step completion markers - based on actual pipeline.py logging
    # Progress: 25%, 50%, 75%, 100% for 4 steps
    step_markers = {
        "fetch-paper": {
            "start": "step: fetch-paper",
            "complete": "complete",  # "✓ Complete" or "✓ Already complete, skipping"
            "percent": 25,
        },
        "generate-script-and-html": {
            "start": "step: generate-script-and-html",
            "complete": "complete",
            "percent": 50,
        },
        "generate-audio": {
            "start": "step: generate-audio",
            "complete": "complete",
            "percent": 75,
        },
        "generate-videos": {
            "start": "step: generate-videos",
            "complete": "complete",
            "percent": 100,
        },
    }
    
    # Check for step starts first (before completion checks)
    # Pipeline logs: "Step: fetch-paper" (capital S)
    for step_name, markers in step_markers.items():
        # Check if this step is starting - pipeline logs "Step: <step-name>"
        if f"step: {step_name}" in line_lower:
            completed_steps = current_progress.get("completed_steps", [])
            # Always update current step when pipeline logs it, even if already set
            # This ensures the UI shows the correct step immediately
            updated = current_progress.copy()
            updated["current_step"] = step_name
            # Set progress to previous step's completion percent
            if completed_steps:
                # Find the percent for the last completed step
                last_completed = completed_steps[-1]
                for sname, smarkers in step_markers.items():
                    if sname == last_completed:
                        updated["progress_percent"] = smarkers["percent"]
                        break
            else:
                updated["progress_percent"] = 0
            logger.debug(f"Detected step start: {step_name}")
            return updated
    
    # Check for step completions - look for "✓ Complete" or "✓ Already complete"
    # Pipeline logs: "  ✓ Complete" or "  ✓ Already complete, skipping"
    # Use current_step context to determine which step completed
    current_step = current_progress.get("current_step")
    if "✓" in line and ("complete" in line_lower or "already" in line_lower):
        # Find which step just completed based on current_step or line content
        for step_name, markers in step_markers.items():
            completed_steps = current_progress.get("completed_steps", [])
            # Match if this is the current step being tracked, or step name appears in line
            if (current_step == step_name or step_name in line_lower) and step_name not in completed_steps:
                updated = current_progress.copy()
                updated["progress_percent"] = markers["percent"]
                # Keep current_step until next step starts (don't clear immediately)
                # Only clear if this is the final step (100%)
                if markers["percent"] >= 100:
                    updated["current_step"] = None
                if "completed_steps" not in updated:
                    updated["completed_steps"] = []
                updated["completed_steps"] = completed_steps + [step_name]
                logger.debug(f"Detected step completion: {step_name} -> {markers['percent']}%")
                return updated
    
    # Check for pipeline completion
    if "pipeline complete!" in line_lower:
        updated = current_progress.copy()
        updated["progress_percent"] = 100
        updated["current_step"] = None
        updated["status"] = "completed"
        return updated
    
    # Check for failures
    if "✗" in line or "pipelineerror" in line_lower or ("failed" in line_lower and "step" in line_lower):
        updated = current_progress.copy()
        updated["status"] = "failed"
        return updated
    
    return None  # No progress change


@shared_task(
    bind=True,
    name="web.tasks.generate_video_task",
    autoretry_for=(Exception,),
    retry_kwargs={"max_retries": 0},  # Don't auto-retry, but catch all exceptions
    reject_on_worker_lost=False,  # Don't reject task if worker dies
)
def generate_video_task(self, pmid: str, output_dir: str, user_id: Optional[int] = None) -> Dict:
    """
    Celery task to generate video from a PubMed paper.
    
    This task runs the video generation pipeline in a subprocess and captures
    all output and errors. Errors are stored in a JSON file for retrieval
    by the status endpoint.
    
    Args:
        pmid: PubMed ID or PMC ID of the paper
        output_dir: Directory path where output files will be saved
        user_id: Optional user ID to associate with the job
        
    Returns:
        Dict with status information:
        {
            "status": "completed" | "failed",
            "pmid": str,
            "output_dir": str,
            "error": Optional[str],  # Error message if failed
            "error_type": Optional[str],  # Type of error (user-friendly)
        }
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # File to store task result/error information
    task_result_file = output_path / "task_result.json"
    log_path = output_path / "pipeline.log"
    
    # Initialize task result
    task_result = {
        "status": "running",
        "pmid": pmid,
        "output_dir": str(output_dir),
        "task_id": self.request.id,
        "error": None,
        "error_type": None,
    }
    
    # Update database job record
    job = None
    try:
        if user_id:
            from django.contrib.auth.models import User
            from web.models import VideoGenerationJob
            try:
                user = User.objects.get(pk=user_id)
                job, created = VideoGenerationJob.objects.get_or_create(
                    task_id=self.request.id,
                    defaults={
                        'user': user,
                        'paper_id': pmid,
                        'status': 'running',
                        'progress_percent': 0,
                        'current_step': 'starting',
                    }
                )
                if not created:
                    # Update existing job
                    job.status = 'running'
                    job.progress_percent = 0
                    job.current_step = 'starting'
                    job.save(update_fields=['status', 'progress_percent', 'current_step', 'updated_at'])
            except Exception as e:
                logger.warning(f"Failed to create/update job record: {e}")
    except Exception as e:
        logger.warning(f"Failed to import models for job tracking: {e}")
    
    try:
        logger.info(f"Starting video generation task for {pmid}")
        logger.info(f"Task ID: {self.request.id}")
        logger.info(f"Output directory: {output_dir}")
        
        # Check if simulation mode is enabled
        if settings.SIMULATION_MODE:
            logger.info(f"SIMULATION MODE ENABLED - Simulating pipeline progress instead of running actual pipeline")
            from web.simulation import simulate_pipeline_progress
            
            # Update task state
            self.update_state(
                state="PROGRESS",
                meta={"current_step": "starting", "pmid": pmid}
            )
            
            # Create a log file for simulation (use UTF-8 encoding)
            with open(log_path, "w", encoding="utf-8") as f:
                f.write(f"[SIMULATION MODE] Starting simulated pipeline for {pmid}\n")
            
            # Run simulation
            try:
                # Close any existing database connections before simulation
                from django.db import connections
                connections.close_all()
                
                simulate_pipeline_progress(pmid, output_path, self.request.id, job, delay_per_step=3.0)
                
                # Update task result to completed
                task_result["status"] = "completed"
                logger.info(f"Simulation completed successfully for {pmid}")
                
                # Save final task result
                try:
                    with open(task_result_file, "w") as f:
                        json.dump(task_result, f, indent=2)
                except Exception as e:
                    logger.warning(f"Failed to save final task result: {e}")
                
                return task_result
            except Exception as e:
                logger.exception(f"Simulation failed for {pmid}: {e}")
                task_result["status"] = "failed"
                task_result["error"] = f"Simulation error: {str(e)}"
                task_result["error_type"] = "task_error"
                
                # Save failed task result
                try:
                    with open(task_result_file, "w") as f:
                        json.dump(task_result, f, indent=2)
                except Exception as e:
                    logger.warning(f"Failed to save failed task result: {e}")
                
                # Update job record
                if job:
                    try:
                        from django.db import connections
                        connections.close_all()
                        # Refresh job from database
                        job.refresh_from_db()
                        job.status = 'failed'
                        job.error_message = task_result["error"]
                        job.error_type = task_result["error_type"]
                        job.save(update_fields=['status', 'error_message', 'error_type', 'updated_at'])
                    except Exception as db_error:
                        logger.warning(f"Failed to update job record: {db_error}")
                
                return task_result
        
        # Normal pipeline execution (not simulation)
        # Update task state
        self.update_state(
            state="PROGRESS",
            meta={"current_step": "starting", "pmid": pmid}
        )
        
        # Use the same Python interpreter
        python_exe = sys.executable
        script_path = Path(settings.BASE_DIR) / "pipeline" / "main.py"
        
        # Verify script exists
        if not script_path.exists():
            raise FileNotFoundError(f"Pipeline script not found: {script_path}")
        
        cmd = [python_exe, str(script_path), "generate-video", pmid, str(output_path)]
        
        logger.info(f"Running command: {' '.join(cmd)}")
        logger.info(f"Working directory: {settings.BASE_DIR}")
        
        env = os.environ.copy()
        # Ensure Python doesn't buffer output so we see logs in real-time
        env["PYTHONUNBUFFERED"] = "1"
        
        # Run pipeline and capture output in real-time
        process = None
        log_file = None
        try:
            # Open log file for writing (text mode with UTF-8 encoding)
            log_file = open(log_path, "a", encoding="utf-8")
            
            # Create subprocess with PIPE for stdout so we can read it in real-time
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,  # Changed from log_file to PIPE
                stderr=subprocess.STDOUT,  # Merge stderr into stdout
                env=env,
                cwd=str(settings.BASE_DIR),
                start_new_session=True,  # Create new process group
                text=True,  # Text mode for line-by-line reading
                bufsize=1,  # Line buffered
            )
            
            logger.info(f"Started subprocess with PID: {process.pid}")
            
            # Initialize progress state for real-time tracking
            progress_state = {
                "progress_percent": 0,
                "current_step": "starting",
                "completed_steps": [],
                "status": "running",
            }
            
            def update_progress_from_line(line: str):
                """Update progress state from a pipeline output line."""
                parsed = _parse_pipeline_progress(line, progress_state)
                if parsed:
                    progress_state.update(parsed)
                    
                    # Use progress manager to update database (with queuing)
                    try:
                        from web.progress_manager import queue_progress_update
                        
                        queue_progress_update(
                            task_id=self.request.id,
                            progress_percent=progress_state["progress_percent"],
                            current_step=progress_state.get("current_step"),
                            status=progress_state.get("status", "running")
                        )
                    except Exception as e:
                        logger.warning(f"Failed to queue progress update: {e}", exc_info=True)
            
            # Start thread to read output and update progress in real-time
            def read_output_and_update_progress():
                """Read subprocess output line-by-line and update progress."""
                try:
                    for line in process.stdout:
                        # Write to log file (line already includes newline)
                        log_file.write(line)
                        log_file.flush()
                        
                        # Parse for progress updates
                        update_progress_from_line(line)
                        
                except Exception as e:
                    logger.error(f"Error reading subprocess output: {e}", exc_info=True)
                finally:
                    try:
                        log_file.close()
                    except:
                        pass
            
            # Start output reading thread
            output_thread = threading.Thread(target=read_output_and_update_progress, daemon=True)
            output_thread.start()
            logger.info("Started real-time output parsing thread")
            
            # Also start a background thread to periodically update progress from files
            # This is a fallback in case real-time parsing misses updates
            def update_progress_periodically():
                """Periodically update progress from file existence (fallback)."""
                while process.poll() is None:  # While process is still running
                    try:
                        from django.db import connections
                        connections.close_all()
                        
                        # Update progress based on file existence as fallback
                        update_job_progress_from_files(pmid, self.request.id)
                        
                        connections.close_all()
                        time.sleep(10)  # Update every 10 seconds (less frequent than real-time)
                    except Exception as e:
                        logger.debug(f"Error in periodic progress update: {e}")
                        try:
                            connections.close_all()
                        except:
                            pass
                        time.sleep(10)
            
            progress_fallback_thread = threading.Thread(target=update_progress_periodically, daemon=True)
            progress_fallback_thread.start()
            logger.info("Started fallback progress update thread")
            
            # Wait for process to complete with timeout handling
            timeout_seconds = settings.CELERY_TASK_TIME_LIMIT - 60  # Leave 60s buffer
            try:
                return_code = process.wait(timeout=timeout_seconds)
                logger.info(f"Subprocess completed with return code: {return_code}")
                
                # Wait for output thread to finish reading remaining output
                output_thread.join(timeout=5)
                
                # Final progress update
                if progress_state["progress_percent"] < 100:
                    # Check if final video exists
                    final_video = output_path / "final_video.mp4"
                    if final_video.exists():
                        progress_state["progress_percent"] = 100
                        progress_state["current_step"] = None
                        progress_state["status"] = "completed"
                        update_progress_from_line("Pipeline complete!")
                
            except subprocess.TimeoutExpired:
                logger.error(f"Subprocess timed out after {timeout_seconds} seconds")
                # Try graceful termination first
                try:
                    process.terminate()
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    # Force kill if it doesn't terminate
                    logger.warning("Subprocess didn't terminate, forcing kill")
                    process.kill()
                    process.wait()
                return_code = -1
                raise Exception(f"Pipeline timed out after {timeout_seconds} seconds")
            finally:
                # Ensure log file is closed
                try:
                    if log_file and not log_file.closed:
                        log_file.close()
                except:
                    pass
        except subprocess.SubprocessError as e:
            logger.exception(f"Subprocess error: {e}")
            return_code = -1
            # Clean up process if it exists
            if process and process.poll() is None:
                try:
                    process.terminate()
                    process.wait(timeout=5)
                except:
                    try:
                        process.kill()
                        process.wait()
                    except:
                        pass
            # Ensure log file is closed
            try:
                if log_file and not log_file.closed:
                    log_file.close()
            except:
                pass
            raise Exception(f"Failed to start or run pipeline subprocess: {e}")
        except Exception as e:
            logger.exception(f"Unexpected error during subprocess execution: {e}")
            # Clean up process if it exists
            if process and process.poll() is None:
                try:
                    process.terminate()
                    process.wait(timeout=5)
                except:
                    try:
                        process.kill()
                        process.wait()
                    except:
                        pass
            # Ensure log file is closed
            try:
                if log_file and not log_file.closed:
                    log_file.close()
            except:
                pass
            return_code = -1
            raise
        
        # Process any pending progress updates from the queue
        try:
            from web.progress_manager import process_update_queue
            process_update_queue()
        except Exception as e:
            logger.warning(f"Failed to process update queue: {e}")
        
        # Check if pipeline succeeded
        final_video = output_path / "final_video.mp4"
        
        if return_code == 0 and final_video.exists():
            task_result["status"] = "completed"
            logger.info(f"Video generation completed successfully for {pmid}")
            
            # Upload to cloud storage (R2) or save locally
            if job:
                try:
                    from django.core.files import File
                    from django.core.files.storage import default_storage
                    from datetime import datetime
                    
                    # Refresh job to get latest progress
                    job.refresh_from_db()
                    
                    # Generate unique filename (model's upload_to will add date path automatically)
                    # Format: {pmid}_final_video_{timestamp}.mp4
                    # Model's upload_to='videos/%Y/%m/%d/' will create: videos/2025/01/28/{filename}.mp4
                    video_filename = f"{pmid}_final_video_{datetime.now().strftime('%Y%m%d_%H%M%S')}.mp4"
                    
                    # ============================================================================
                    # CRITICAL VIDEO UPLOAD AND DATABASE SAVE SECTION
                    # ============================================================================
                    # This section MUST ensure both final_video and final_video_path are saved
                    # to the database immediately after R2 upload. Multiple verification steps
                    # are included to guarantee data integrity.
                    # ============================================================================
                    # Upload to cloud storage (R2) or save locally
                    if settings.USE_CLOUD_STORAGE:
                        # Open the local file and upload to cloud storage
                        try:
                            # Open file and upload to R2
                            with open(final_video, 'rb') as f:
                                django_file = File(f, name=video_filename)
                                # Upload to R2 storage (save=False means upload to storage but don't save to DB yet)
                                job.final_video.save(video_filename, django_file, save=False)
                                video_storage_path = job.final_video.name  # Capture the path immediately
                            
                            # Now save to database (file handle is closed, but FileField is set)
                            job.final_video_path = video_storage_path  # Store the storage path
                            logger.info(f"Video uploaded to cloud storage: {video_storage_path}")
                            
                            # CRITICAL: Save BOTH fields to database immediately after R2 upload
                            # Use atomic transaction to ensure both fields are saved together
                            from django.db import transaction
                            try:
                                with transaction.atomic():
                                    job.save(update_fields=['final_video', 'final_video_path', 'updated_at'])
                                
                                # VERIFY the save worked by refreshing from DB
                                job.refresh_from_db()
                                
                                # Verify final_video FileField
                                if job.final_video and job.final_video.name == video_storage_path:
                                    logger.info(f"✅ VERIFIED: final_video saved to database: {job.final_video.name}")
                                else:
                                    logger.error(f"❌ WARNING: final_video mismatch or missing! Expected: {video_storage_path}, Got: {job.final_video.name if job.final_video else 'None'}")
                                    # Try to fix it by setting from storage
                                    try:
                                        from django.core.files.storage import default_storage
                                        if default_storage.exists(video_storage_path):
                                            with default_storage.open(video_storage_path, 'rb') as f:
                                                django_file = File(f, name=os.path.basename(video_storage_path))
                                                job.final_video.save(os.path.basename(video_storage_path), django_file, save=False)
                                            with transaction.atomic():
                                                job.save(update_fields=['final_video', 'updated_at'])
                                            job.refresh_from_db()
                                            if job.final_video and job.final_video.name:
                                                logger.info(f"✅ FIXED: final_video now saved: {job.final_video.name}")
                                    except Exception as fix_error:
                                        logger.error(f"❌ Could not fix final_video: {fix_error}")
                                
                                # Verify final_video_path
                                if job.final_video_path == video_storage_path:
                                    logger.info(f"✅ VERIFIED: final_video_path saved: {job.final_video_path}")
                                else:
                                    logger.error(f"❌ FAILED: final_video_path not saved! Expected: {video_storage_path}, Got: {job.final_video_path}")
                                    # Fix it
                                    job.final_video_path = video_storage_path
                                    with transaction.atomic():
                                        job.save(update_fields=['final_video_path', 'updated_at'])
                                    logger.info(f"✅ FIXED: final_video_path now set: {job.final_video_path}")
                                    
                            except Exception as save_error:
                                logger.critical(f"❌ CRITICAL ERROR saving video to database: {save_error}", exc_info=True)
                                # Emergency fallback: save path at minimum
                                try:
                                    job.final_video_path = video_storage_path
                                    with transaction.atomic():
                                        job.save(update_fields=['final_video_path', 'updated_at'])
                                    logger.warning(f"⚠️ Saved final_video_path as emergency fallback: {job.final_video_path}")
                                except Exception as fallback_error:
                                    logger.critical(f"❌ CRITICAL: Even fallback save failed: {fallback_error}")
                            
                            # Delete local file after successful R2 upload and DB save
                            try:
                                final_video.unlink()
                                logger.info(f"Deleted local file after successful R2 upload: {final_video}")
                            except Exception as cleanup_error:
                                logger.warning(f"Failed to delete local file after R2 upload: {cleanup_error}")
                        except Exception as upload_error:
                            logger.error(f"Failed to upload video to cloud storage: {upload_error}", exc_info=True)
                            if settings.USE_CLOUD_STORAGE:
                                # In production with cloud storage, this is a critical error
                                logger.critical(
                                    f"CRITICAL: R2 upload failed in production mode for {pmid}. "
                                    f"Video may not be accessible. Error: {upload_error}"
                                )
                                # Still save local path as fallback, but log the critical issue
                                job.final_video_path = str(final_video)
                                logger.warning(f"Saved local path as fallback: {job.final_video_path}")
                            else:
                                # Development mode - just use local path
                                job.final_video_path = str(final_video)
                                logger.warning(f"Saved local path as fallback: {job.final_video_path}")
                    else:
                        # Local storage - just save the path
                        job.final_video_path = str(final_video)
                    
                    # FINAL SAFEGUARD: Ensure video fields are saved before marking complete
                    job.refresh_from_db()
                    if settings.USE_CLOUD_STORAGE and job.status == 'completed':
                        # Double-check video path is saved
                        if not job.final_video_path:
                            logger.critical(f"❌ CRITICAL: Job marked complete but final_video_path is EMPTY!")
                            # Try to recover - but this should never happen
                        elif job.final_video_path:
                            logger.info(f"✅ FINAL CHECK: Video path confirmed in database: {job.final_video_path}")
                    
                    # Update job status using progress manager
                    try:
                        from web.progress_manager import update_progress
                        update_progress(
                            task_id=self.request.id,
                            progress_percent=100,
                            current_step=None,
                            status='completed'
                        )
                    except Exception as e:
                        logger.warning(f"Failed to update progress via manager, updating directly: {e}")
                        # Fallback to direct update
                        job.status = 'completed'
                        job.progress_percent = 100
                        job.current_step = None
                        job.completed_at = timezone.now()
                        # Ensure video fields are included in save
                        job.save(update_fields=['final_video', 'final_video_path', 'status', 'progress_percent', 'current_step', 'completed_at', 'updated_at'])
                    else:
                        # Progress manager updated successfully
                        # Final verification: ensure video fields are still saved
                        job.refresh_from_db()
                        if settings.USE_CLOUD_STORAGE:
                            if not job.final_video_path:
                                logger.critical(f"❌ CRITICAL: final_video_path lost after progress update! This should never happen.")
                            elif job.final_video_path:
                                logger.info(f"✅ Final verification: Video path persists: {job.final_video_path}")
                    
                except Exception as e:
                    logger.error(f"Failed to update job record on completion: {e}", exc_info=True)
        else:
            # Pipeline failed - try to extract error from log
            error_message = _extract_error_from_log(log_path)
            error_type = _classify_error(error_message)
            
            task_result["status"] = "failed"
            task_result["error"] = error_message
            task_result["error_type"] = error_type
            
            logger.error(f"Video generation failed for {pmid}: {error_message}")
            
            # Update database job record with failure status
            # But keep the progress that was actually achieved
            if job:
                try:
                    # Refresh job to get latest progress
                    job.refresh_from_db()
                    job.status = 'failed'
                    job.error_message = error_message
                    job.error_type = error_type
                    # Don't reset progress - keep what was actually completed
                    job.save(update_fields=['status', 'error_message', 'error_type', 'updated_at'])
                except Exception as e:
                    logger.warning(f"Failed to update job record on failure: {e}")
            
            # Update task state with error (use PROGRESS state, not FAILURE, to avoid serialization issues)
            # We'll return the failed result instead of raising an exception
            self.update_state(
                state="PROGRESS",
                meta={
                    "pmid": pmid,
                    "error": error_message,
                    "error_type": error_type,
                    "status": "failed",
                }
            )
    
    except KeyboardInterrupt:
        # Handle keyboard interrupt gracefully
        logger.warning(f"Task interrupted for {pmid}")
        task_result["status"] = "failed"
        task_result["error"] = "Task was interrupted"
        task_result["error_type"] = "task_error"
        
        # Update database job record
        if job:
            try:
                job.status = 'failed'
                job.error_message = "Task was interrupted"
                job.error_type = "task_error"
                job.save(update_fields=['status', 'error_message', 'error_type', 'updated_at'])
            except Exception as e:
                logger.warning(f"Failed to update job record on interrupt: {e}")
        
        raise  # Re-raise to let Celery handle it
    except Exception as e:
        # Catch ALL other exceptions to prevent worker crash
        error_message = f"Task execution error: {str(e)}"
        task_result["status"] = "failed"
        task_result["error"] = error_message
        task_result["error_type"] = "task_error"
        
        logger.exception(f"Unexpected error in video generation task for {pmid}")
        
        # Update database job record
        if job:
            try:
                job.status = 'failed'
                job.error_message = error_message
                job.error_type = "task_error"
                job.save(update_fields=['status', 'error_message', 'error_type', 'updated_at'])
            except Exception as e:
                logger.warning(f"Failed to update job record on exception: {e}")
        
        # Update task state (use PROGRESS instead of FAILURE to avoid serialization issues)
        try:
            self.update_state(
                state="PROGRESS",
                meta={
                    "pmid": pmid,
                    "error": error_message,
                    "error_type": "task_error",
                    "status": "failed",
                }
            )
        except Exception as state_error:
            logger.error(f"Failed to update task state: {state_error}")
    
    finally:
        # Save task result to file for status endpoint to read
        try:
            with open(task_result_file, "w") as f:
                json.dump(task_result, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save task result: {e}")
    
    return task_result


def _extract_error_from_log(log_path: Path) -> str:
    """
    Extract error message from pipeline log file.
    
    Args:
        log_path: Path to pipeline.log file
        
    Returns:
        Error message string, or generic message if log can't be read
    """
    if not log_path.exists():
        return "Pipeline log file not found"
    
    try:
        # Read last 8KB of log file
        with open(log_path, "rb") as f:
            f.seek(max(0, f.tell() - 8192))
            log_content = f.read().decode(errors="replace")
        
        # Try to find error messages
        lines = log_content.split("\n")
        
        # Look for common error patterns
        error_keywords = ["Error:", "Failed:", "Exception:", "Traceback", "✗"]
        
        error_lines = []
        for line in reversed(lines):
            if any(keyword in line for keyword in error_keywords):
                error_lines.insert(0, line)
                if len(error_lines) >= 5:  # Get last 5 error lines
                    break
        
        if error_lines:
            return "\n".join(error_lines)
        
        # If no specific error found, return last few lines
        if lines:
            return "\n".join(lines[-10:])
        
        return "Pipeline failed (check log for details)"
    
    except Exception as e:
        return f"Failed to read log file: {str(e)}"


def _classify_error(error_message: str) -> str:
    """
    Classify error type from error message for user-friendly display.
    
    Args:
        error_message: Error message string
        
    Returns:
        User-friendly error type string
    """
    error_lower = error_message.lower()
    
    # Check for specific error types
    if "not available in pubmed central" in error_lower or "pmcnotfounderror" in error_lower:
        return "paper_not_found"
    
    if "api key" in error_lower or "authentication" in error_lower or "unauthorized" in error_lower:
        return "api_key_error"
    
    if "timeout" in error_lower:
        return "timeout"
    
    if "quota" in error_lower or "rate limit" in error_lower:
        return "rate_limit"
    
    if "pipeline" in error_lower and "failed" in error_lower:
        return "pipeline_error"
    
    return "unknown_error"


def get_task_status(pmid: str) -> Optional[Dict]:
    """
    Get the status of a video generation task.
    
    This reads the task result file created by the Celery task.
    
    Args:
        pmid: PubMed ID to check
        
    Returns:
        Dict with task status, or None if task not found
    """
    output_dir = Path(settings.MEDIA_ROOT) / pmid
    task_result_file = output_dir / "task_result.json"
    
    if not task_result_file.exists():
        return None
    
    try:
        with open(task_result_file, "r") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to read task result for {pmid}: {e}")
        return None


@shared_task(bind=True, name="web.tasks.test_volume_write_task")
def test_volume_write_task(self) -> Dict:
    """
    Celery task to test if Railway volume is writable from Celery worker.
    
    This creates a test file in MEDIA_ROOT to verify the volume is working.
    This can be called from Celery to test if it can write to the volume.
    
    Returns:
        Dict with test results
    """
    from django.utils import timezone
    
    try:
        output_path = Path(settings.MEDIA_ROOT)
        
        # Create test directory if needed
        test_dir = output_path / ".volume_test"
        test_dir.mkdir(parents=True, exist_ok=True)
        
        # Write a test file with timestamp
        test_file = test_dir / "test_write_celery.txt"
        task_id = self.request.id if hasattr(self, 'request') else 'N/A'
        test_content = f"Volume test - {timezone.now().isoformat()}\nService: Celery Worker\nMEDIA_ROOT: {output_path}\nTask ID: {task_id}"
        test_file.write_text(test_content)
        
        # Verify we can read it back
        read_back = test_file.read_text()
        
        # Get file stats
        file_stats = test_file.stat()
        
        result = {
            "success": True,
            "message": "Volume write test successful from Celery",
            "service": "Celery Worker",
            "MEDIA_ROOT": str(output_path),
            "test_file_path": str(test_file),
            "test_file_exists": test_file.exists(),
            "test_file_size": file_stats.st_size,
            "test_file_readable": True,
            "test_content_matches": read_back == test_content,
            "task_id": task_id,
            "timestamp": timezone.now().isoformat(),
        }
        
        # Clean up test file (optional - leave it for debugging)
        # test_file.unlink()
        
        return result
    except Exception as e:
        logger.exception(f"Volume write test failed in Celery: {e}")
        return {
            "success": False,
            "error": str(e),
            "type": type(e).__name__,
            "service": "Celery Worker",
            "MEDIA_ROOT": str(Path(settings.MEDIA_ROOT)) if 'settings' in locals() else "unknown",
            "recommendation": "Check that the volume is mounted on Celery service in Railway dashboard.",
        }


def update_job_progress_from_files(pmid: str, task_id: Optional[str] = None) -> None:
    """
    Update job progress in database based on file existence checks.
    
    This function checks which pipeline steps have completed by looking for
    output files, and updates the database job record accordingly.
    
    Args:
        pmid: PubMed ID
        task_id: Optional task ID to find the job record
    """
    from django.db import connections
    
    try:
        # Close any stale connections first (important for threads)
        connections.close_all()
        
        from web.models import VideoGenerationJob
        
        # Find job by paper_id and optionally task_id
        if task_id:
            try:
                job = VideoGenerationJob.objects.get(task_id=task_id)
            except VideoGenerationJob.DoesNotExist:
                logger.debug(f"Job not found for task_id {task_id}")
                return
        else:
            # Try to find most recent job for this paper_id
            try:
                job = VideoGenerationJob.objects.filter(paper_id=pmid).order_by('-created_at').first()
                if not job:
                    logger.debug(f"No job found for paper_id {pmid}")
                    return
            except Exception as e:
                logger.warning(f"Error finding job for {pmid}: {e}")
                return
        
        # Only update if job is still running
        if job.status not in ['pending', 'running']:
            logger.debug(f"Job {job.id} is not in pending/running state (status: {job.status}), skipping update")
            return
        
        output_dir = Path(settings.MEDIA_ROOT) / pmid
        
        # Ensure output directory exists (might not exist yet if pipeline just started)
        if not output_dir.exists():
            logger.debug(f"Output directory does not exist yet: {output_dir}")
            return
        
        # Check pipeline steps (4 steps: 25%, 50%, 75%, 100%)
        steps = [
            ("fetch-paper", 25, lambda d: (d / "paper.json").exists()),
            ("generate-script", 50, lambda d: (d / "script.json").exists()),
            ("generate-audio", 75, lambda d: (d / "audio.wav").exists() and (d / "audio_metadata.json").exists()),
            ("generate-videos", 100, lambda d: (d / "clips" / ".videos_complete").exists() or (d / "final_video.mp4").exists()),
        ]
        
        current_step = None
        progress_percent = 0
        completed_steps = []
        
        # Check each step to determine progress
        for step_name, step_percent, check_func in steps:
            if check_func(output_dir):
                progress_percent = step_percent
                completed_steps.append(step_name)
                logger.debug(f"Step {step_name} completed (progress: {progress_percent}%)")
            else:
                if current_step is None:
                    current_step = step_name
                logger.debug(f"Step {step_name} not yet completed, current step: {current_step}")
                # Don't break - continue checking to see all completed steps
        
        # If all steps are complete, we're at 100%
        if len(completed_steps) == len(steps):
            progress_percent = 100
            current_step = None
        
        # Update job if progress changed
        if job.progress_percent != progress_percent or job.current_step != current_step:
            logger.info(f"Updating job progress: {job.progress_percent}% -> {progress_percent}%, step: {job.current_step} -> {current_step}")
            job.progress_percent = progress_percent
            job.current_step = current_step
            if progress_percent == 100:
                final_video = output_dir / "final_video.mp4"
                if final_video.exists():
                    job.status = 'completed'
                    job.final_video_path = str(final_video)
                    job.completed_at = timezone.now()
                    job.current_step = None
            job.save(update_fields=['progress_percent', 'current_step', 'status', 'final_video_path', 'completed_at', 'updated_at'])
            logger.info(f"Job progress updated successfully")
        else:
            logger.debug(f"Job progress unchanged: {progress_percent}%, step: {current_step}")
    except Exception as e:
        logger.warning(f"Failed to update job progress from files: {e}", exc_info=True)
    finally:
        # Always close database connections when done (critical for threads)
        try:
            connections.close_all()
        except Exception:
            pass


@shared_task(bind=True, name="web.tasks.test_r2_storage_write")
def test_r2_storage_write_task(self) -> Dict:
    """
    Celery task to test if R2 cloud storage is writable from Celery worker.
    
    This creates a test file in R2 to verify cloud storage is working.
    This can be called from Celery to test if it can write to R2.
    
    Returns:
        Dict with test results
    """
    from django.core.files.storage import default_storage
    from django.core.files.base import ContentFile
    
    try:
        # Generate unique test filename
        test_filename = f"test_files/celery_test_{timezone.now().strftime('%Y%m%d_%H%M%S')}.txt"
        task_id = self.request.id if hasattr(self, 'request') else 'N/A'
        
        # Create test content
        test_content = (
            f"R2 Storage Test - {timezone.now().isoformat()}\n"
            f"Service: Celery Worker\n"
            f"Task ID: {task_id}\n"
            f"Storage Backend: {type(default_storage).__name__}\n"
            f"USE_CLOUD_STORAGE: {getattr(settings, 'USE_CLOUD_STORAGE', False)}\n"
        )
        
        # Write to cloud storage
        test_file = default_storage.save(test_filename, ContentFile(test_content.encode('utf-8')))
        
        # Verify we can read it back
        if default_storage.exists(test_file):
            read_back = default_storage.open(test_file).read().decode('utf-8')
            
            result = {
                "success": True,
                "message": "R2 storage write test successful from Celery",
                "service": "Celery Worker",
                "test_file_path": test_file,
                "test_file_exists": True,
                "test_file_readable": True,
                "test_content_matches": read_back == test_content,
                "storage_backend": type(default_storage).__name__,
                "use_cloud_storage": getattr(settings, 'USE_CLOUD_STORAGE', False),
                "task_id": task_id,
                "timestamp": timezone.now().isoformat(),
            }
            
            # Get file URL if available
            try:
                result["test_file_url"] = default_storage.url(test_file)
            except Exception:
                result["test_file_url"] = "N/A (URL generation failed)"
            
            return result
        else:
            return {
                "success": False,
                "error": "File was written but does not exist when checked",
                "service": "Celery Worker",
                "test_file_path": test_file,
            }
            
    except Exception as e:
        import traceback
        return {
            "success": False,
            "error": str(e),
            "type": type(e).__name__,
            "service": "Celery Worker",
            "traceback": traceback.format_exc(),
            "use_cloud_storage": getattr(settings, 'USE_CLOUD_STORAGE', False),
            "storage_backend": type(default_storage).__name__ if 'default_storage' in locals() else "unknown",
        }

