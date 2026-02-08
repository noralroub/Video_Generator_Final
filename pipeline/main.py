import json
import logging
import os
import time
from pathlib import Path

import click
from runwayml import RunwayML

# Load environment variables from .env file (if it exists)
try:
    from dotenv import load_dotenv
    # Load .env from project root (parent directory)
    env_path = Path(__file__).parent.parent / ".env"
    if env_path.exists():
        load_dotenv(env_path)
except ImportError:
    pass  # python-dotenv not installed, skip

from pubmed import fetch_paper, PMCNotFoundError
from scenes import generate_scenes, save_scenes, load_scenes
from audio import generate_audio, save_audio_metadata
from video import generate_videos, save_video_metadata
from pipeline import orchestrate_pipeline, PipelineError
from presentation import render_presentation

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


@click.group()
def cli():
    """Gemini Video Generation CLI - Generate videos using Google's Veo model."""
    pass


@cli.command()
@click.option("--prompt", "-p", required=True, help="Text prompt for video generation")
@click.option(
    "--output",
    "-o",
    default="output.mp4",
    help="Output video file path (default: output.mp4)",
)
@click.option("--model", "-m", default="veo3.1_fast", help="Model to use for generation (default: veo3.1_fast)")
@click.option(
    "--poll-interval",
    default=1,
    type=int,
    help="Seconds between status checks (default: 1)",
)
def generate(prompt, output, model, poll_interval):
    """Generate a video from a text prompt using Runway."""
    client = RunwayML()

    click.echo(f"Starting video generation with model: {model}")
    click.echo(f"Prompt: {prompt}")

    # Start video generation
    task = client.text_to_video.create(
        model=model,
        prompt=prompt,
        ratio="1280:720",
    )
    task_id = task.id

    # Poll the operation status until the video is ready
    click.echo(f"Task ID: {task_id}")
    time.sleep(poll_interval)
    task = client.tasks.retrieve(task_id)

    with click.progressbar(length=100, label="Generating video") as bar:
        while task.status not in ["SUCCEEDED", "FAILED"]:
            time.sleep(poll_interval)
            task = client.tasks.retrieve(task_id)

    if task.status == "FAILED":
        click.secho(f"✗ Video generation failed: {task}", fg="red", err=True)
        raise click.Abort()

    # Download the generated video
    click.echo("Video generation complete! Downloading...")
    video_url = task.output[0] if isinstance(task.output, list) else task.output

    import requests

    response = requests.get(video_url)
    response.raise_for_status()

    with open(output, "wb") as f:
        f.write(response.content)

    click.secho(f"✓ Generated video saved to {output}", fg="green", bold=True)


@cli.command()
@click.argument("prompt_file", type=click.File("r"))
@click.option("--output", "-o", default="output.mp4", help="Output video file path")
@click.option("--model", "-m", default="veo3.1_fast", help="Model to use for generation (default: veo3.1_fast)")
@click.option(
    "--poll-interval", default=1, type=int, help="Seconds between status checks"
)
def generate_from_file(prompt_file, output, model, poll_interval):
    """Generate a video from a prompt in a text file."""
    prompt = prompt_file.read().strip()

    if not prompt:
        click.secho("Error: Prompt file is empty", fg="red", err=True)
        raise click.Abort()

    # Call the generate function with the file content
    ctx = click.get_current_context()
    ctx.invoke(
        generate, prompt=prompt, output=output, model=model, poll_interval=poll_interval
    )


@cli.command()
@click.argument("paper_id")
@click.argument("output_dir")
def fetch_paper_cmd(paper_id, output_dir):
    """Fetch a paper from PubMed Central by PMID or PMCID.

    Retrieves full text, figures, and metadata from PMC and saves to OUTPUT_DIR.
    Only works with open-access papers available in PubMed Central.

    Examples:
        python main.py fetch-paper 33963468 ./output
        python main.py fetch-paper PMC12510764 ./output
    """
    try:
        click.echo(f"Fetching paper {paper_id} from PubMed Central...")

        paper_data = fetch_paper(paper_id, output_dir)

        click.secho(f"\n✓ Successfully fetched paper!", fg="green", bold=True)
        click.echo(f"  Title: {paper_data['title']}")
        click.echo(f"  PMCID: {paper_data['pmcid']}")
        click.echo(f"  Full text length: {len(paper_data['full_text'])} characters")
        click.echo(f"  Figures found: {len(paper_data['figures'])}")
        click.echo(f"\n  Output saved to: {output_dir}/")
        click.echo(f"    - paper.json (metadata + full text)")
        click.echo(f"    - paper.xml (raw XML)")

        if paper_data["figures"]:
            click.echo(f"\n  Figure URLs:")
            for fig in paper_data["figures"]:
                click.echo(f"    - {fig['id']}: {fig['url']}")

    except PMCNotFoundError as e:
        click.secho(f"✗ Error: {e}", fg="red", err=True)
        raise click.Abort()
    except Exception as e:
        click.secho(f"✗ Unexpected error: {e}", fg="red", err=True)
        raise click.Abort()


@cli.command()
@click.argument("paper_dir", type=click.Path(exists=True))
def generate_script(paper_dir):
    """Generate video script from paper data in PAPER_DIR.

    PAPER_DIR: Directory containing paper.json (from fetch-paper command)

    This will generate a script.json file with 4-10 scenes for video creation.
    Each scene contains narration text and a video generation prompt.

    Example:
        python main.py generate-script ./my_paper
    """
    try:
        paper_path = Path(paper_dir)
        paper_file = paper_path / "paper.json"

        if not paper_file.exists():
            click.secho(
                f"✗ Error: paper.json not found in {paper_dir}", fg="red", err=True
            )
            click.echo("Run 'fetch-paper' command first to download paper data")
            raise click.Abort()

        # Load paper data
        click.echo(f"Loading paper from {paper_file}...")
        with open(paper_file, "r", encoding="utf-8") as f:
            paper_data = json.load(f)

        click.echo(f"Paper: {paper_data['title']}")
        click.echo(f"Generating scenes using Gemini...")

        # Generate scenes
        scene_list = generate_scenes(paper_data)

        # Save to script.json
        script_file = paper_path / "script.json"
        save_scenes(scene_list, script_file)

        click.secho(f"\n✓ Generated {len(scene_list)} scenes", fg="green", bold=True)
        click.secho(f"✓ Saved to: {script_file}", fg="green", bold=True)

        # Preview scenes
        click.echo("\n" + "=" * 60)
        click.echo("SCENE PREVIEW")
        click.echo("=" * 60)

        for i, scene in enumerate(scene_list, 1):
            click.echo(f"\nScene {i}:")
            click.echo(f"  Text: {scene.text}")
            click.echo(f"  Visual: {scene.visual_type}")
            prompt_preview = (
                scene.visual_content[:100] + "..."
                if len(scene.visual_content) > 100
                else scene.visual_content
            )
            click.echo(f"  Prompt: {prompt_preview}")

        click.echo("\n" + "=" * 60)

    except Exception as e:
        logging.exception("Error generating script")
        click.secho(f"✗ Error: {e}", fg="red", err=True)
        raise click.Abort()


@cli.command()
@click.argument("paper_dir", type=click.Path(exists=True))
@click.option(
    "--voice", "-v", default="Kore", help="Voice to use for TTS (default: Kore)"
)
def generate_audio_cmd(paper_dir, voice):
    """Generate audio narration from script in PAPER_DIR.

    PAPER_DIR: Directory containing script.json (from generate-script command)

    This will generate:
    - audio.wav: Full continuous narration audio
    - audio_metadata.json: Scene boundary timings

    The audio generation uses proportional splitting:
    1. Generate full continuous TTS for natural flow
    2. Generate individual scenes in parallel for timing proportions
    3. Calculate scene boundaries based on proportional durations

    Example:
        python main.py generate-audio ./my_paper
        python main.py generate-audio ./my_paper --voice Puck
    """
    try:
        paper_path = Path(paper_dir)
        script_file = paper_path / "script.json"

        if not script_file.exists():
            click.secho(
                f"✗ Error: script.json not found in {paper_dir}", fg="red", err=True
            )
            click.echo("Run 'generate-script' command first to create the script")
            raise click.Abort()

        # Load scenes
        click.echo(f"Loading script from {script_file}...")
        scenes = load_scenes(script_file)

        click.echo(f"Generating audio for {len(scenes)} scenes with voice '{voice}'...")
        click.echo("(This may take a minute...)")

        # Generate audio
        result = generate_audio(scenes, paper_path, voice=voice)

        # Save metadata
        metadata_file = paper_path / "audio_metadata.json"
        save_audio_metadata(result, metadata_file)

        click.secho(f"\n✓ Audio generation complete!", fg="green", bold=True)
        click.echo(f"  Total duration: {result.total_duration:.2f}s")
        click.echo(f"  Voice: {result.voice}")
        click.secho(f"\n✓ Output saved to:", fg="green", bold=True)
        click.echo(f"  - {result.full_audio_path}")
        click.echo(f"  - {metadata_file}")

        # Display scene timing summary
        click.echo("\n" + "=" * 60)
        click.echo("SCENE TIMING & VISUALS")
        click.echo("=" * 60)

        for sb in result.scene_boundaries:
            click.echo(
                f"\nScene {sb.scene_index}: {sb.start_time:.2f}s - {sb.end_time:.2f}s (clip: {sb.clip_duration:.2f}s)"
            )
            text_preview = sb.text[:70] + "..." if len(sb.text) > 70 else sb.text
            click.echo(f"  Text: {text_preview}")
            click.echo(f"  Visual: {sb.visual_type}")
            visual_preview = (
                sb.visual_content[:80] + "..."
                if len(sb.visual_content) > 80
                else sb.visual_content
            )
            click.echo(f"  Prompt: {visual_preview}")

        click.echo("\n" + "=" * 60)

    except Exception as e:
        logging.exception("Error generating audio")
        click.secho(f"✗ Error: {e}", fg="red", err=True)
        raise click.Abort()


@cli.command()
@click.argument("metadata_file", type=click.Path(exists=True))
@click.option(
    "--output-dir",
    "-o",
    default=None,
    help="Output directory for video clips (default: clips/ in same directory as metadata)",
)
@click.option(
    "--max-workers",
    "-w",
    default=5,
    type=int,
    help="Maximum parallel video generations (default: 5)",
)
@click.option(
    "--poll-interval",
    "-p",
    default=1,
    type=int,
    help="Seconds between status checks (default: 1)",
)
def generate_videos_cmd(metadata_file, output_dir, max_workers, poll_interval):
    """Generate video clips from audio metadata.

    METADATA_FILE: Path to audio_metadata.json (from generate-audio command)

    This will generate video clips for all scenes in parallel using Runway Veo 3.1 Fast.
    Videos are generated with 9:16 aspect ratio (portrait/vertical) for TikTok.

    Output:
    - Individual video clips (scene_00.mp4, scene_01.mp4, etc.)
    - video_metadata.json with clip information

    Example:
        python main.py generate-videos ./my_paper/audio_metadata.json
        python main.py generate-videos ./my_paper/audio_metadata.json -o ./my_paper/clips
    """
    try:
        metadata_path = Path(metadata_file)

        if output_dir:
            output_path = Path(output_dir)
        else:
            output_path = None  # Will default to clips/ subdirectory

        click.echo(f"Loading metadata from {metadata_path}...")
        click.echo(f"Maximum parallel generations: {max_workers}")
        click.echo(f"Poll interval: {poll_interval}s")
        click.echo("\nGenerating video clips with Runway Veo 3.1 Fast...")
        click.echo("(This may take several minutes depending on number of scenes...)")
        click.echo("")

        # Generate videos
        result = generate_videos(
            metadata_path,
            output_dir=output_path,
            max_workers=max_workers,
            poll_interval=poll_interval,
        )

        # Save video metadata
        video_metadata_file = Path(result.output_dir) / "video_metadata.json"
        save_video_metadata(result, video_metadata_file)

        click.secho(f"\n✓ Video generation complete!", fg="green", bold=True)
        click.echo(f"  Total clips: {result.total_clips}")
        click.echo(f"  Output directory: {result.output_dir}")
        click.secho(f"\n✓ Saved metadata to:", fg="green", bold=True)
        click.echo(f"  - {video_metadata_file}")

        # Display clip summary
        click.echo("\n" + "=" * 60)
        click.echo("GENERATED CLIPS")
        click.echo("=" * 60)

        for clip in result.clips:
            click.echo(f"\nScene {clip.scene_index}: {clip.visual_type}")
            if clip.clip_path:
                click.echo(f"  File: {clip.clip_path}")
                click.echo(f"  Duration: {clip.duration:.2f}s")
                prompt_preview = (
                    clip.prompt[:80] + "..." if len(clip.prompt) > 80 else clip.prompt
                )
                click.echo(f"  Prompt: {prompt_preview}")
            else:
                click.echo(f"  (Figure - will be added during composition)")

        click.echo("\n" + "=" * 60)

    except Exception as e:
        logging.exception("Error generating videos")
        click.secho(f"✗ Error: {e}", fg="red", err=True)
        raise click.Abort()


@cli.command()
@click.argument("pmid")
@click.argument("output_dir", type=click.Path())
@click.option(
    "--skip-existing/--no-skip-existing",
    default=True,
    help="Skip already completed steps (default: skip)",
)
@click.option(
    "--stop-after",
    type=click.Choice(
        [
            "fetch-paper",
            "generate-script",
            "generate-audio",
            "generate-videos",
            "generate-presentation",
        ]
    ),
    help="Stop pipeline after this step",
)
@click.option(
    "--output-format",
    "-f",
    type=click.Choice(["video", "presentation"]),
    default="video",
    help="Output format: 'video' (Veo clips + merge) or 'presentation' (HTML with synced audio). Default: video",
)
@click.option(
    "--voice", "-v", default="Kore", help="Gemini TTS voice to use (default: Kore)"
)
@click.option(
    "--max-workers",
    "-w",
    default=5,
    type=int,
    help="Maximum parallel video generation workers (default: 5)",
)
@click.option(
    "--no-merge",
    is_flag=True,
    help="Skip merging video clips into a single final video",
)
def generate_video(
    pmid: str,
    output_dir: str,
    skip_existing: bool,
    stop_after: str,
    output_format: str,
    voice: str,
    max_workers: int,
    no_merge: bool,
):
    """Generate complete video from PubMed paper (end-to-end pipeline).

    This command orchestrates the entire pipeline:
    1. fetch-paper: Download paper from PubMed Central
    2. generate-script: Create video script with scenes
    3. generate-audio: Generate TTS audio for each scene
    4. generate-videos: Create video clips with Runway Veo 3.1 Fast

    By default, the pipeline is idempotent - it will skip steps that
    have already been completed. Use --no-skip-existing to force
    re-execution of all steps.

    Examples:

        # Generate complete video
        python main.py generate-video PMC10979640 tmp/PMC10979640

        # Generate with custom voice
        python main.py generate-video PMC10979640 tmp/PMC10979640 --voice Puck

        # Stop after script generation (for testing)
        python main.py generate-video PMC10979640 tmp/PMC10979640 --stop-after generate-script

        # Force re-generation of everything
        python main.py generate-video PMC10979640 tmp/PMC10979640 --no-skip-existing
    """
    output_path = Path(output_dir)

    try:
        orchestrate_pipeline(
            pmid=pmid,
            output_dir=output_path,
            skip_existing=skip_existing,
            stop_after=stop_after,
            voice=voice,
            max_workers=max_workers,
            merge=not no_merge,
            output_format=output_format,
        )
        click.secho(
            f"✓ Pipeline complete! Output in {output_path}", fg="green", bold=True
        )
        if output_format == "presentation":
            click.secho(
                f"✓ Presentation: {output_path}/presentation.html (with audio.wav)",
                fg="green",
                bold=True,
            )
        elif not no_merge:
            click.secho(
                f"✓ Final merged video: {output_path}/final_video.mp4",
                fg="green",
                bold=True,
            )
    except PipelineError as e:
        click.secho(f"✗ Pipeline failed: {e}", fg="red", err=True)
        raise click.Abort()
    except Exception as e:
        click.secho(f"✗ Unexpected error: {e}", fg="red", err=True)
        logging.exception("Unexpected error in pipeline")
        raise click.Abort()


@cli.command("generate-presentation")
@click.argument("output_dir", type=click.Path(exists=True, file_okay=False))
@click.option(
    "--audio-src",
    default="audio.wav",
    help="Audio filename or URL for the presentation (default: audio.wav)",
)
def generate_presentation_cmd(output_dir: str, audio_src: str) -> None:
    """Generate HTML presentation from existing script and audio in OUTPUT_DIR.

    Requires script.json in OUTPUT_DIR. If audio_metadata.json exists, slide
    timing will be synced to the narration; otherwise uses default durations.

    Example:

        python main.py generate-presentation ./my_paper
    """
    output_path = Path(output_dir)
    script_path = output_path / "script.json"
    if not script_path.exists():
        click.secho(f"✗ Script not found: {script_path}", fg="red", err=True)
        raise click.Abort()
    html_path = output_path / "presentation.html"
    audio_metadata_path = output_path / "audio_metadata.json"
    try:
        render_presentation(
            script_path=script_path,
            output_path=html_path,
            audio_metadata_path=audio_metadata_path if audio_metadata_path.exists() else None,
            audio_src=audio_src,
        )
        click.secho(f"✓ Presentation written to {html_path}", fg="green", bold=True)
    except Exception as e:
        click.secho(f"✗ Error: {e}", fg="red", err=True)
        raise click.Abort()


if __name__ == "__main__":
    cli()
