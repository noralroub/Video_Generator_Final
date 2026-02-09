"""HTML presentation generation from script and audio metadata."""

import json
import logging
import re
from pathlib import Path
from typing import Any, Optional
from urllib.request import Request, urlopen

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


def _load_figures(paper_path: Path) -> list[dict[str, Any]]:
    """Load figures list from paper.json. Return [] if file missing or invalid."""
    if not paper_path.exists():
        return []
    try:
        with open(paper_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        figures = data.get("figures", [])
        return [{"id": f.get("id", ""), "url": f.get("url", ""), "caption": f.get("caption", "")} for f in figures]
    except (json.JSONDecodeError, OSError) as e:
        logger.warning(f"Could not load figures from {paper_path}: {e}")
        return []


def _apply_figures_to_scenes(scene_list: list[dict[str, Any]], figures: list[dict[str, Any]]) -> None:
    """Assign first N figures to first N content scenes (in place)."""
    content_indices = [i for i, s in enumerate(scene_list) if s.get("slide_type") == "content"]
    for idx, scene_idx in enumerate(content_indices):
        if idx < len(figures):
            fig = figures[idx]
            scene_list[scene_idx]["figure_url"] = fig.get("url", "")
            scene_list[scene_idx]["figure_caption"] = fig.get("caption", "")


def _extension_from_content_type(content_type: str) -> str:
    """Return a safe file extension from Content-Type header."""
    if not content_type:
        return "png"
    ct = content_type.split(";")[0].strip().lower()
    if "png" in ct:
        return "png"
    if "jpeg" in ct or "jpg" in ct:
        return "jpg"
    if "gif" in ct:
        return "gif"
    if "webp" in ct:
        return "webp"
    return "png"


def _download_figure(url: str, figures_dir: Path, index: int, timeout: int = 15) -> Optional[str]:
    """Download image from url to figures_dir/fig_<index>.<ext>. Return filename (e.g. fig_0.png) or None."""
    try:
        req = Request(url, headers={"User-Agent": "Mozilla/5.0 (compatible; VideoGenerator/1.0)"})
        with urlopen(req, timeout=timeout) as resp:
            content = resp.read()
            content_type = resp.headers.get("Content-Type", "")
        ext = _extension_from_content_type(content_type)
        safe_ext = ext if re.match(r"^[a-z]+$", ext) else "png"
        filename = f"fig_{index}.{safe_ext}"
        out_path = figures_dir / filename
        out_path.write_bytes(content)
        return filename
    except Exception as e:
        logger.warning(f"Could not download figure from {url[:60]}...: {e}")
        return None


def _download_figures_to_output(scene_list: list[dict[str, Any]], output_dir: Path) -> None:
    """Download remote figure URLs to output_dir/figures/ and set figure_url to relative path (in place)."""
    figures_dir = output_dir / "figures"
    figures_dir.mkdir(parents=True, exist_ok=True)
    fig_index = 0
    for scene in scene_list:
        if scene.get("slide_type") != "content":
            continue
        url = scene.get("figure_url")
        if not url or not str(url).startswith("http"):
            continue
        filename = _download_figure(url, figures_dir, fig_index)
        if filename:
            scene["figure_url"] = f"figures/{filename}"
        else:
            scene.pop("figure_url", None)
            scene.pop("figure_caption", None)
        fig_index += 1


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
    paper_path: Optional[Path] = None,
) -> None:
    """
    Render an HTML presentation from script.json and optional audio metadata.

    Args:
        script_path: Path to script.json (from generate-script step).
        output_path: Path to write presentation.html.
        audio_metadata_path: Optional path to audio_metadata.json (for scene durations).
        audio_src: Filename or URL for the audio element (e.g. "audio.wav").
        paper_path: Optional path to paper.json; if set and file exists, figures are loaded
            and assigned to content scenes (figure_url, figure_caption).
    """
    try:
        from jinja2 import Environment, FileSystemLoader, select_autoescape
    except ImportError:
        raise ImportError("jinja2 is required for presentation generation. Install with: pip install jinja2") from None

    scene_list, scene_durations, total_duration, title = _prepare_scenes(script_path, audio_metadata_path)
    if paper_path:
        figures = _load_figures(paper_path)
        if figures:
            _apply_figures_to_scenes(scene_list, figures)
            output_dir = output_path.parent
            _download_figures_to_output(scene_list, output_dir)

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
