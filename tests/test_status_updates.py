"""
Script to test video generation status updates without actually generating videos.

This script simulates pipeline progress by creating the files and database records
that the status system checks, allowing you to test the status page UI without
incurring API costs for video generation.

Usage:
    python tests/test_status_updates.py <paper_id> [--step <step_name>] [--auto] [--user <username>]

Examples:
    # Simulate pipeline at step 2 (generate-script)
    python tests/test_status_updates.py TEST123 --step generate-script

    # Auto-progress through all steps with delays (good for watching status page)
    python tests/test_status_updates.py TEST123 --auto

    # Create job for specific user
    python tests/test_status_updates.py TEST123 --step fetch-paper --user admin

Steps (in order):
    1. fetch-paper (20%) - Creates paper.json
    2. generate-script (40%) - Creates script.json
    3. generate-audio (60%) - Creates audio.wav and audio_metadata.json
    4. generate-videos (80%) - Creates clips/.videos_complete marker
    (4 steps total, final_video.mp4 created by generate-videos step)
"""

import os
import sys
import json
import time
import argparse
from pathlib import Path
from datetime import datetime, timezone

# Add project root to path so we can import Django modules
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

# Setup Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')

import django
django.setup()

from django.conf import settings
from django.contrib.auth.models import User
from django.utils import timezone as django_timezone
from web.models import VideoGenerationJob
import uuid


def create_step_files(output_dir: Path, step_name: str):
    """Create files that indicate a pipeline step has completed."""
    output_dir.mkdir(parents=True, exist_ok=True)
    
    if step_name == "fetch-paper":
        # Step 1: Paper fetched
        paper_file = output_dir / "paper.json"
        paper_file.write_text(json.dumps({
            "pmid": output_dir.name,
            "title": "Test Paper Title",
            "authors": ["Test Author 1", "Test Author 2"],
            "abstract": "This is a test abstract for status update testing.",
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }, indent=2))
        print(f"[OK] Created {paper_file}")
        
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
                    "narration": "Test scene narration for status testing.",
                    "visual_description": "A test visual description",
                }
            ],
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }, indent=2))
        print(f"[OK] Created {script_file}")
        
    elif step_name == "generate-audio":
        # Step 3: Audio generated
        # Ensure prerequisites exist
        if not (output_dir / "script.json").exists():
            create_step_files(output_dir, "generate-script")
        
        audio_file = output_dir / "audio.wav"
        # Create a minimal valid WAV file (header only, no actual audio)
        # WAV header is 44 bytes minimum
        wav_header = b'RIFF' + (36).to_bytes(4, 'little') + b'WAVE' + \
                     b'fmt ' + (16).to_bytes(4, 'little') + \
                     (1).to_bytes(2, 'little') + (1).to_bytes(2, 'little') + \
                     (44100).to_bytes(4, 'little') + (88200).to_bytes(4, 'little') + \
                     (2).to_bytes(2, 'little') + (16).to_bytes(2, 'little') + \
                     b'data' + (0).to_bytes(4, 'little')
        audio_file.write_bytes(wav_header)
        print(f"[OK] Created {audio_file}")
        
        metadata_file = output_dir / "audio_metadata.json"
        metadata_file.write_text(json.dumps({
            "duration": 5.0,
            "sample_rate": 44100,
            "format": "wav",
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }, indent=2))
        print(f"[OK] Created {metadata_file}")
        
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
        print(f"[OK] Created {marker_file}")
        
        # Also create a dummy video metadata file
        video_metadata = clips_dir / "video_metadata.json"
        video_metadata.write_text(json.dumps({
            "clips": ["clip_00.mp4"],
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }, indent=2))
        print(f"[OK] Created {video_metadata}")
        
    # Note: add-captions step has been removed from pipeline
    # Final video is created by generate-videos step
    else:
        raise ValueError(f"Unknown step: {step_name}")


def create_task_files(output_dir: Path, task_id: str, status: str = "running"):
    """Create task tracking files."""
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Create task_id.txt
    task_id_file = output_dir / "task_id.txt"
    task_id_file.write_text(task_id)
    print(f"[OK] Created {task_id_file}")
    
    # Create task_result.json
    task_result_file = output_dir / "task_result.json"
    task_result = {
        "status": status,
        "pmid": output_dir.name,
        "output_dir": str(output_dir),
        "task_id": task_id,
        "error": None,
        "error_type": None,
    }
    task_result_file.write_text(json.dumps(task_result, indent=2))
    print(f"[OK] Created {task_result_file}")


def update_job_record(paper_id: str, step_name: str, task_id: str, user: User = None):
    """Create or update a VideoGenerationJob record in the database."""
    # Map step names to progress percentages (Sprint 1: 3 real steps to 100%)
    step_progress = {
        "fetch-paper": (33, "fetch-paper"),
        "generate-script": (66, "generate-script"),
        "generate-audio": (100, None),
    }
    
    progress_percent, current_step = step_progress.get(step_name, (0, step_name))
    status = "completed" if progress_percent == 100 else "running"
    
    # Get or create job
    if user:
        job, created = VideoGenerationJob.objects.get_or_create(
            task_id=task_id,
            defaults={
                'user': user,
                'paper_id': paper_id,
                'status': status,
                'progress_percent': progress_percent,
                'current_step': current_step,
            }
        )
        if not created:
            job.status = status
            job.progress_percent = progress_percent
            job.current_step = current_step
            if status == "completed":
                job.completed_at = django_timezone.now()
                job.final_video_path = str(Path(settings.MEDIA_ROOT) / paper_id / "final_video.mp4")
            job.save(update_fields=['status', 'progress_percent', 'current_step', 'completed_at', 'final_video_path', 'updated_at'])
    else:
        # If no user specified, create job with first available user or skip
        try:
            user = User.objects.first()
            if not user:
                print("[WARNING] No users found. Skipping database job creation.")
                print("   Create a user first: python manage.py createsuperuser")
                return
        except Exception as e:
            print(f"[WARNING] Could not get user: {e}")
            return
        
        job, created = VideoGenerationJob.objects.get_or_create(
            task_id=task_id,
            defaults={
                'user': user,
                'paper_id': paper_id,
                'status': status,
                'progress_percent': progress_percent,
                'current_step': current_step,
            }
        )
        if not created:
            job.status = status
            job.progress_percent = progress_percent
            job.current_step = current_step
            if status == "completed":
                job.completed_at = django_timezone.now()
                job.final_video_path = str(Path(settings.MEDIA_ROOT) / paper_id / "final_video.mp4")
            job.save(update_fields=['status', 'progress_percent', 'current_step', 'completed_at', 'final_video_path', 'updated_at'])
    
    print(f"[OK] Updated job record: {job.status} - {job.progress_percent}% - {job.current_step}")


def simulate_progress(paper_id: str, step_name: str, user: User = None):
    """Simulate pipeline progress to a specific step."""
    output_dir = Path(settings.MEDIA_ROOT) / paper_id
    task_id = str(uuid.uuid4())
    
    print(f"\n[INFO] Simulating pipeline progress for: {paper_id}")
    print(f"   Target step: {step_name}")
    print(f"   Output directory: {output_dir}")
    print()
    
    # Create files for all steps up to and including the target step
    steps = ["fetch-paper", "generate-script", "generate-audio"]
    target_index = steps.index(step_name) if step_name in steps else -1
    
    if target_index == -1:
        print(f"[ERROR] Unknown step: {step_name}")
        print(f"   Available steps: {', '.join(steps)}")
        return
    
    # Create files for all steps up to target
    for i in range(target_index + 1):
        create_step_files(output_dir, steps[i])
    
    # Create task tracking files
    status = "completed" if step_name == "generate-videos" else "running"
    create_task_files(output_dir, task_id, status)
    
    # Update database job record
    update_job_record(paper_id, step_name, task_id, user)
    
    print(f"\n[SUCCESS] Simulation complete!")
    print(f"   View status at: http://localhost:8000/status/{paper_id}/")
    print(f"   JSON status: http://localhost:8000/status/{paper_id}/?_json=1")


def auto_progress(paper_id: str, delay: int = 3, user: User = None):
    """Automatically progress through all steps with delays."""
    steps = ["fetch-paper", "generate-script", "generate-audio"]
    
    print(f"\n[INFO] Auto-progressing through all steps for: {paper_id}")
    print(f"   Delay between steps: {delay} seconds")
    print(f"   View status at: http://localhost:8000/status/{paper_id}/")
    print()
    
    for step in steps:
        print(f"\n[STEP] Progressing to: {step}")
        simulate_progress(paper_id, step, user)
        if step != steps[-1]:  # Don't delay after last step
            print(f"   Waiting {delay} seconds before next step...")
            time.sleep(delay)
    
    print(f"\n[SUCCESS] Auto-progress complete!")


def main():
    parser = argparse.ArgumentParser(
        description="Test video generation status updates without generating videos",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument("paper_id", help="Paper ID to simulate (e.g., TEST123)")
    parser.add_argument(
        "--step",
        choices=["fetch-paper", "generate-script", "generate-audio", "generate-videos"],
        help="Simulate progress up to this step"
    )
    parser.add_argument(
        "--auto",
        action="store_true",
        help="Auto-progress through all steps with delays"
    )
    parser.add_argument(
        "--delay",
        type=int,
        default=3,
        help="Delay in seconds between steps when using --auto (default: 3)"
    )
    parser.add_argument(
        "--user",
        help="Username to associate job with (must exist)"
    )
    
    args = parser.parse_args()
    
    # Get user if specified
    user = None
    if args.user:
        try:
            user = User.objects.get(username=args.user)
        except User.DoesNotExist:
            print(f"[ERROR] User '{args.user}' not found.")
            print("   Create a user first: python manage.py createsuperuser")
            sys.exit(1)
    
    if args.auto:
        auto_progress(args.paper_id, args.delay, user)
    elif args.step:
        simulate_progress(args.paper_id, args.step, user)
    else:
        # Default: just create initial state (pending)
        output_dir = Path(settings.MEDIA_ROOT) / args.paper_id
        task_id = str(uuid.uuid4())
        create_task_files(output_dir, task_id, "pending")
        update_job_record(args.paper_id, "fetch-paper", task_id, user)
        print(f"\n[SUCCESS] Created initial state (pending)")
        print(f"   View status at: http://localhost:8000/status/{args.paper_id}/")


if __name__ == "__main__":
    main()

