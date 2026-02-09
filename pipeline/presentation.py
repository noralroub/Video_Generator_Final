"""HTML presentation generation from script and audio metadata."""

import json
import logging
from pathlib import Path
from typing import Any, Optional

from scenes import load_scenes

logger = logging.getLogger(__name__)

# Default duration per scene when no audio metadata (seconds)
DEFAULT_SCENE_DURATION = 6.0


def _load_audio_durations(metadata_path: Path) -> tuple[list[float], float]:
    """Load scene durations and total duration from audio_metadata.json."""
    with open(metadata_path, "r", encoding="utf-8") as f:
        metadata = json.load(f)
    boundaries = metadata.get("scene_boundaries", [])
    durations = [float(sb.get("duration", sb.get("clip_duration", DEFAULT_SCENE_DURATION))) for sb in boundaries]
    total = float(metadata.get("total_duration", sum(durations)))
    return durations, total


def _prepare_scenes(script_path: Path, audio_metadata_path: Optional[Path] = None) -> tuple[list[dict[str, Any]], list[float], float, str]:
    """
    Load script and optional audio metadata; return scenes for template, durations, total_duration, title.
    """
    scenes = load_scenes(script_path)
    if not scenes:
        raise ValueError("Script has no scenes")

    if audio_metadata_path and audio_metadata_path.exists():
        scene_durations, total_duration = _load_audio_durations(audio_metadata_path)
        # Pad or trim to match scene count
        n = len(scenes)
        if len(scene_durations) < n:
            scene_durations = scene_durations + [DEFAULT_SCENE_DURATION] * (n - len(scene_durations))
        else:
            scene_durations = scene_durations[:n]
        total_duration = sum(scene_durations)
    else:
        scene_durations = [DEFAULT_SCENE_DURATION] * len(scenes)
        total_duration = sum(scene_durations)

    # Build list of scene dicts: text, duration_seconds, slide_type (hook | content | cta)
    scene_list: list[dict[str, Any]] = []
    for i, scene in enumerate(scenes):
        if i == 0:
            slide_type = "hook"
        elif i == len(scenes) - 1:
            slide_type = "cta"
        else:
            slide_type = "content"
        entry = {
            "text": scene.text,
            "duration_seconds": scene_durations[i] if i < len(scene_durations) else DEFAULT_SCENE_DURATION,
            "slide_type": slide_type,
        }
        if scene.key_stat:
            entry["key_stat"] = scene.key_stat
        if scene.bullets:
            entry["bullets"] = scene.bullets
        scene_list.append(entry)

    title = scene_list[0]["text"] if scene_list else "Presentation"
    if len(title) > 80:
        title = title[:77] + "..."

    return scene_list, scene_durations, total_duration, title


def render_presentation(
    script_path: Path,
    output_path: Path,
    audio_metadata_path: Optional[Path] = None,
    audio_src: str = "audio.wav",
) -> None:
    """
    Render an HTML presentation from script.json and optional audio metadata.

    Args:
        script_path: Path to script.json (from generate-script step).
        output_path: Path to write presentation.html.
        audio_metadata_path: Optional path to audio_metadata.json (for scene durations).
        audio_src: Filename or URL for the audio element (e.g. "audio.wav").

    Raises:
        FileNotFoundError: If script_path does not exist.
        ValueError: If script has no scenes.
    """
    try:
        from jinja2 import Environment, FileSystemLoader, select_autoescape
    except ImportError:
        raise ImportError("jinja2 is required for presentation generation. Install with: pip install jinja2") from None

    scene_list, scene_durations, total_duration, title = _prepare_scenes(script_path, audio_metadata_path)

    template_dir = Path(__file__).resolve().parent / "templates"
    if not template_dir.exists():
        raise FileNotFoundError(f"Template directory not found: {template_dir}")

    env = Environment(
        loader=FileSystemLoader(str(template_dir)),
        autoescape=select_autoescape(("html", "xml")),
    )
    template = env.get_template("presentation.html.j2")

    html = template.render(
        title=title,
        scenes=scene_list,
        scene_durations=scene_durations,
        total_duration=total_duration,
        audio_src=audio_src,
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html, encoding="utf-8")
    logger.info(f"Rendered presentation to {output_path} ({len(scene_list)} scenes, {total_duration:.1f}s)")
