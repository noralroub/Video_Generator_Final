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
from scenes import save_scenes, load_scenes
from html_video import generate_script_and_html, run_html_video_step
from audio import generate_audio, save_audio_metadata
from pipeline import orchestrate_pipeline, PipelineError

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
    """Generate script and motion-graphics HTML from paper data (Claude).

    PAPER_DIR: Directory containing paper.json (from fetch-paper command)

    Writes script.json and motion_video.html (with placeholder durations).
    Run generate-audio next, then generate-videos to produce final_video.mp4.

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

        click.echo(f"Loading paper from {paper_file}...")
        with open(paper_file, "r", encoding="utf-8") as f:
            paper_data = json.load(f)

        click.echo(f"Paper: {paper_data['title']}")
        click.echo("Generating script and motion HTML with Claude...")
        generate_script_and_html(paper_data, paper_path)
        script_file = paper_path / "script.json"
        scene_list = load_scenes(script_file)

        click.secho(f"\n✓ Generated {len(scene_list)} scenes and motion_video.html", fg="green", bold=True)
        click.secho(f"✓ Saved to: {script_file}, {paper_path / 'motion_video.html'}", fg="green", bold=True)

        click.echo("\n" + "=" * 60)
        click.echo("SCENE PREVIEW")
        click.echo("=" * 60)
        for i, scene in enumerate(scene_list, 1):
            click.echo(f"\nScene {i}: {scene.text[:80]}...")
        click.echo("\n" + "=" * 60)

    except Exception as e:
        logging.exception("Error generating script and HTML")
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
@click.argument("paper_dir", type=click.Path(exists=True))
def generate_videos_cmd(paper_dir):
    """Produce final video from HTML (inject durations, record, mux audio).

    PAPER_DIR: Directory containing motion_video.html, audio_metadata.json, audio.wav
    (from generate-script and generate-audio commands).

    Output: final_video.mp4 in PAPER_DIR.

    Example:
        python main.py generate-videos ./my_paper
    """
    try:
        output_path = Path(paper_dir)
        click.echo(f"Running HTML video step in {output_path}...")
        click.echo("(Injecting durations, recording viewport, muxing audio...)")
        run_html_video_step(output_path)
        click.secho(f"\n✓ Video complete: {output_path / 'final_video.mp4'}", fg="green", bold=True)
    except Exception as e:
        logging.exception("Error generating video from HTML")
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
            "generate-script-and-html",
            "generate-audio",
            "generate-videos",
        ]
    ),
    help="Stop pipeline after this step",
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
    voice: str,
    max_workers: int,
    no_merge: bool,
):
    """Generate complete video from PubMed paper (end-to-end pipeline).

    This command orchestrates the entire pipeline:
    1. fetch-paper: Download paper from PubMed Central
    2. generate-script-and-html: Create script and motion HTML with Claude
    3. generate-audio: Generate TTS audio for each scene (Gemini)
    4. generate-videos: Inject durations, record HTML, mux audio to final_video.mp4

    By default, the pipeline is idempotent. Use --no-skip-existing to force
    re-execution of all steps.

    Examples:

        # Generate complete video
        python main.py generate-video PMC10979640 tmp/PMC10979640

        # Generate with custom voice
        python main.py generate-video PMC10979640 tmp/PMC10979640 --voice Puck

        # Stop after script+HTML (for testing)
        python main.py generate-video PMC10979640 tmp/PMC10979640 --stop-after generate-script-and-html

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
        )
        click.secho(
            f"✓ Pipeline complete! Videos in {output_path}", fg="green", bold=True
        )
        click.secho(
            f"✓ Final video: {output_path}/final_video.mp4",
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


if __name__ == "__main__":
    cli()
