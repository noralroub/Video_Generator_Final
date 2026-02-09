"""Pipeline orchestration for end-to-end video generation."""

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional

from audio import generate_audio, save_audio_metadata
from pubmed import fetch_paper
from presentation import render_presentation
from scenes import generate_scenes, save_scenes, load_scenes
from video import generate_videos, save_video_metadata

logger = logging.getLogger(__name__)


@dataclass
class PipelineStep:
    """Represents a step in the video generation pipeline."""

    name: str
    description: str
    check_completion: Callable[[], bool]
    execute: Callable[[], None]


class PipelineError(Exception):
    """Raised when a pipeline step fails."""

    pass


def check_paper_fetched(output_dir: Path) -> bool:
    """Check if paper has been fetched."""
    paper_json = output_dir / "paper.json"
    return paper_json.exists()




def check_script_generated(output_dir: Path) -> bool:
    """Check if script has been generated."""
    script_json = output_dir / "script.json"
    return script_json.exists()


def check_audio_generated(output_dir: Path) -> bool:
    """Check if audio has been generated.
    
    More robust check: verifies that the combined audio file exists
    and matches the expected scenes from the script.
    """
    audio_file = output_dir / "audio.wav"
    metadata_file = output_dir / "audio_metadata.json"
    
    # Basic check
    if not (audio_file.exists() and metadata_file.exists()):
        return False
    
    # Verify metadata matches script (optional but helps catch inconsistencies)
    try:
        script_file = output_dir / "script.json"
        if script_file.exists():
            scenes = load_scenes(script_file)
            with open(metadata_file, "r", encoding="utf-8") as f:
                metadata = json.load(f)
            
            # Check if scene count matches
            scene_boundaries = metadata.get("scene_boundaries", [])
            if len(scene_boundaries) != len(scenes):
                logger.warning(
                    f"Audio metadata has {len(scene_boundaries)} scenes but "
                    f"script has {len(scenes)} scenes. Regenerating audio."
                )
                return False
    except Exception as e:
        logger.warning(f"Error validating audio metadata: {e}, assuming incomplete")
        # Don't fail the check on validation errors, just log a warning
    
    return True


def check_videos_generated(output_dir: Path) -> bool:
    """Check if videos have been generated for all scenes.
    
    More robust check: verifies that all expected video clips exist
    based on the script, not just a marker file.
    """
    # First check for marker file (fast path)
    clips_dir = output_dir / "clips"
    marker_path = clips_dir / ".videos_complete"
    if marker_path.exists():
        return True
    
    # If no marker, check if all expected videos exist based on script
    script_file = output_dir / "script.json"
    if not script_file.exists():
        return False
    
    # Load script to determine how many scenes we need
    scenes = load_scenes(script_file)
    if not scenes:
        return False
    
    # Check if all expected video clips exist
    expected_count = len(scenes)
    existing_clips = list(clips_dir.glob("scene_*.mp4")) if clips_dir.exists() else []
    
    # Count unique scene clips (scene_00.mp4, scene_01.mp4, etc.)
    scene_indices = set()
    for clip_path in existing_clips:
        try:
            # Extract scene number from filename like "scene_00.mp4"
            name = clip_path.stem  # "scene_00"
            if name.startswith("scene_"):
                scene_num = int(name.split("_")[1])
                scene_indices.add(scene_num)
        except (ValueError, IndexError):
            continue
    
    # Check if we have all required clips
    all_clips_exist = len(scene_indices) >= expected_count and all(
        i in scene_indices for i in range(expected_count)
    )
    
    if all_clips_exist:
        logger.info(
            f"All {expected_count} video clips found (even without marker file), "
            "skipping video generation"
        )
        # Create marker file for faster future checks
        if not marker_path.exists():
            marker_path.parent.mkdir(parents=True, exist_ok=True)
            marker_path.touch()
            logger.debug(f"Created missing marker file: {marker_path}")
    
    return all_clips_exist


def check_presentation_generated(output_dir: Path) -> bool:
    """Check if HTML presentation has been generated."""
    return (output_dir / "presentation.html").exists()


def _generate_presentation_step(output_dir: Path) -> None:
    """Execute the generate-presentation step."""
    script_path = output_dir / "script.json"
    output_path = output_dir / "presentation.html"
    audio_metadata_path = output_dir / "audio_metadata.json"
    render_presentation(
        script_path=script_path,
        output_path=output_path,
        audio_metadata_path=audio_metadata_path if audio_metadata_path.exists() else None,
        audio_src="audio.wav",
        paper_path=output_dir / "paper.json",
    )
    logger.info("Generated presentation.html with embedded audio and synced slide timing")



def orchestrate_pipeline(
    pmid: str,
    output_dir: Path,
    skip_existing: bool = True,
    stop_after: Optional[str] = None,
    voice: str = "Kore",
    max_workers: int = 5,
    merge: bool = True,
    output_format: str = "video",
) -> None:
    """Orchestrate the complete video generation pipeline.

    Args:
        pmid: PubMed ID or PMC ID of the paper
        output_dir: Directory for all output files
        skip_existing: If True, skip completed steps (idempotent)
        stop_after: Stop after this step name (for debugging)
        voice: Gemini TTS voice to use
        max_workers: Maximum parallel workers for video generation
        merge: If True, concatenate all video clips into a single final video (default: True)
        output_format: "video" (default) or "presentation". When "presentation", generates
            presentation.html instead of video clips and final_video.mp4.

    Raises:
        PipelineError: If any step fails
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    common_steps = [
        PipelineStep(
            name="fetch-paper",
            description=f"Fetching paper {pmid} from PubMed Central",
            check_completion=lambda: check_paper_fetched(output_dir),
            execute=lambda: fetch_paper(pmid, str(output_dir)),
        ),
        PipelineStep(
            name="generate-script",
            description="Generating video script with scenes",
            check_completion=lambda: check_script_generated(output_dir),
            execute=lambda: _generate_script_step(output_dir),
        ),
        PipelineStep(
            name="generate-audio",
            description="Generating audio for all scenes",
            check_completion=lambda: check_audio_generated(output_dir),
            execute=lambda: _generate_audio_step(output_dir, voice),
        ),
    ]

    if output_format == "presentation":
        steps = common_steps + [
            PipelineStep(
                name="generate-presentation",
                description="Generating HTML presentation with synced audio",
                check_completion=lambda: check_presentation_generated(output_dir),
                execute=lambda: _generate_presentation_step(output_dir),
            ),
        ]
    else:
        steps = common_steps + [
            PipelineStep(
                name="generate-videos",
                description="Generating videos for all scenes",
                check_completion=lambda: check_videos_generated(output_dir),
                execute=lambda: _generate_videos_step(output_dir, max_workers, merge),
            ),
        ]

    logger.info(f"Starting pipeline for PMID {pmid}")
    logger.info(f"Output directory: {output_dir}")

    for step in steps:
        logger.info(f"Step: {step.name}")

        # Check if step is already complete
        if skip_existing and step.check_completion():
            logger.info(f"  ✓ Already complete, skipping")
            logger.info(f"  → Reusing existing output files from previous run")
            if stop_after == step.name:
                logger.info(f"Stopping after {step.name} as requested")
                break
            continue

        # Execute step
        logger.info(f"  → {step.description}")
        try:
            step.execute()
            logger.info(f"  ✓ Complete")
        except Exception as e:
            error_msg = f"Step '{step.name}' failed: {e}"
            logger.error(f"  ✗ {error_msg}")
            raise PipelineError(error_msg) from e

        # Stop if requested
        if stop_after == step.name:
            logger.info(f"Stopping after {step.name} as requested")
            break

    logger.info("Pipeline complete!")
    logger.info(f"Output files in: {output_dir}")


def _generate_script_step(output_dir: Path) -> None:
    """Execute the generate-script step."""
    # Load paper data
    paper_file = output_dir / "paper.json"
    with open(paper_file, "r", encoding="utf-8") as f:
        paper_data = json.load(f)

    # Generate scenes
    scene_list = generate_scenes(paper_data)

    # Save to script.json
    script_file = output_dir / "script.json"
    save_scenes(scene_list, script_file)

    logger.info(f"Generated {len(scene_list)} scenes")


def _generate_audio_step(output_dir: Path, voice: str) -> None:
    """Execute the generate-audio step."""
    # Load scenes
    script_file = output_dir / "script.json"
    scenes = load_scenes(script_file)

    # Generate audio
    result = generate_audio(scenes, output_dir, voice=voice)

    # Save metadata
    metadata_file = output_dir / "audio_metadata.json"
    save_audio_metadata(result, metadata_file)

    logger.info(f"Generated audio: {result.total_duration:.2f}s with voice '{voice}'")


def _generate_videos_step(
    output_dir: Path, max_workers: int, merge: bool = True
) -> None:
    """Execute the generate-videos step."""
    # Load audio metadata
    metadata_path = output_dir / "audio_metadata.json"

    # Generate videos with merging
    result = generate_videos(
        metadata_path,
        output_dir=None,
        max_workers=max_workers,
        poll_interval=1,
        merge=merge,
    )

    # Save video metadata
    video_metadata_file = Path(result.output_dir) / "video_metadata.json"
    save_video_metadata(result, video_metadata_file)

    logger.info(f"Generated {result.total_clips} video clips")
    if merge:
        logger.info(f"Final merged video created")
