"""HTML presentation generation from script and audio metadata."""

import json
import logging
import re
from pathlib import Path
from typing import Any, Optional
from urllib.request import Request, urlopen

from scenes import load_scenes

logger = logging.getLogger(__name__)

# Placeholder contract: must match claude_presentation.py. Python replaces these
# in Claude's full HTML with title, audio_src, scene_durations JSON, total_duration JSON.
from claude_presentation import (
    PLACEHOLDER_AUDIO_SRC,
    PLACEHOLDER_SCENE_DURATIONS_JSON,
    PLACEHOLDER_TITLE,
    PLACEHOLDER_TOTAL_DURATION_JSON,
    generate_presentation_html,
)

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


def render_presentation_claude(
    script_path: Path,
    output_path: Path,
    audio_metadata_path: Optional[Path] = None,
    audio_src: str = "audio.wav",
    paper_path: Optional[Path] = None,
) -> None:
    """
    Render an HTML presentation: Claude generates full HTML with placeholders;
    we replace placeholders with title, audio_src, scene_durations, total_duration and write the file.
    """
    html = generate_presentation_html(script_path, paper_path)

    scenes = load_scenes(script_path)
    if not scenes:
        raise ValueError("Script has no scenes")
    title = scenes[0].text if scenes else "Presentation"
    if len(title) > 80:
        title = title[:77] + "..."

    if audio_metadata_path and audio_metadata_path.exists():
        scene_durations, total_duration = _load_audio_durations(audio_metadata_path)
        n = len(scenes)
        if len(scene_durations) < n:
            scene_durations = scene_durations + [DEFAULT_SCENE_DURATION] * (n - len(scene_durations))
        else:
            scene_durations = scene_durations[:n]
        total_duration = sum(scene_durations)
    else:
        scene_durations = [DEFAULT_SCENE_DURATION] * len(scenes)
        total_duration = sum(scene_durations)

    html = (
        html.replace(PLACEHOLDER_TITLE, title)
        .replace(PLACEHOLDER_AUDIO_SRC, audio_src)
        .replace(PLACEHOLDER_SCENE_DURATIONS_JSON, json.dumps(scene_durations))
        .replace(PLACEHOLDER_TOTAL_DURATION_JSON, json.dumps(total_duration))
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html, encoding="utf-8")
    logger.info(f"Rendered Claude presentation to {output_path} ({len(scenes)} scenes, {total_duration:.1f}s)")
