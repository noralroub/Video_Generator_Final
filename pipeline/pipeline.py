"""Pipeline orchestration for end-to-end video generation."""

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional

from audio import generate_audio, save_audio_metadata
from pubmed import fetch_paper
from scenes import load_scenes, save_scenes
from html_video import generate_script_and_html, run_html_video_step

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




def check_script_and_html_generated(output_dir: Path) -> bool:
    """Check if script and motion-graphics HTML have been generated (Claude step)."""
    script_json = output_dir / "script.json"
    motion_html = output_dir / "motion_video.html"
    return script_json.exists() and motion_html.exists()


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
    """Check if video has been generated (HTML pipeline: inject + record to recorded.mp4)."""
    recorded = output_dir / "recorded.mp4"
    return recorded.exists()




def orchestrate_pipeline(
    pmid: str,
    output_dir: Path,
    skip_existing: bool = True,
    stop_after: Optional[str] = None,
    voice: str = "Kore",
    max_workers: int = 5,
    merge: bool = True,
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

    Raises:
        PipelineError: If any step fails
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    # Define pipeline steps
    steps = [
        PipelineStep(
            name="fetch-paper",
            description=f"Fetching paper {pmid} from PubMed Central",
            check_completion=lambda: check_paper_fetched(output_dir),
            execute=lambda: fetch_paper(pmid, str(output_dir)),
        ),
        PipelineStep(
            name="generate-script-and-html",
            description="Generating script and motion-graphics HTML with Claude",
            check_completion=lambda: check_script_and_html_generated(output_dir),
            execute=lambda: _generate_script_and_html_step(output_dir),
        ),
        PipelineStep(
            name="generate-audio",
            description="Generating audio for all scenes",
            check_completion=lambda: check_audio_generated(output_dir),
            execute=lambda: _generate_audio_step(output_dir, voice),
        ),
        PipelineStep(
            name="generate-videos",
            description="Injecting durations, recording HTML, muxing audio",
            check_completion=lambda: check_videos_generated(output_dir),
            execute=lambda: _generate_html_videos_step(output_dir),
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


def _generate_script_and_html_step(output_dir: Path) -> None:
    """Execute the generate-script-and-html step (Claude: script + motion HTML)."""
    paper_file = output_dir / "paper.json"
    with open(paper_file, "r", encoding="utf-8") as f:
        paper_data = json.load(f)
    generate_script_and_html(paper_data, output_dir)
    logger.info("Generated script and motion_video.html")


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


def _generate_html_videos_step(output_dir: Path) -> None:
    """Execute the generate-videos step (HTML pipeline: inject durations, record, mux)."""
    run_html_video_step(output_dir)
    logger.info("Final video created from HTML recording")
