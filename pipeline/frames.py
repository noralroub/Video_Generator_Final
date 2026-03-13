"""HTML frame generation and presentation composition for the paper narration pipeline."""

from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import List, Literal

from scenes import Scene, load_scenes


@dataclass(frozen=True)
class Frame:
    """Represents a single 9:16 HTML frame for a scene."""

    scene_id: int
    layout: Literal["title_body", "quote"]
    headline: str
    body: str
    accent_text: str | None = None
    theme: Literal["light", "dark", "journal_brand"] = "light"
    animation: Literal["fade", "slide_up"] = "fade"


def generate_frames(scenes: List[Scene]) -> List[Frame]:
    """Deterministically map storyboard scenes to simple frame data."""
    frames: List[Frame] = []

    total = len(scenes)
    for idx, scene in enumerate(scenes):
        text = scene.text.strip()

        # Simple heuristic split: first sentence as headline, rest as body
        parts = [p.strip() for p in text.replace("\n", " ").split(".") if p.strip()]
        if parts:
            headline = parts[0]
            body = ". ".join(parts[1:])
            if body:
                body = body + "."
        else:
            headline = text
            body = ""

        # Very lightweight "role" mapping just to vary layout a bit
        if idx == 0:
            layout: Literal["title_body", "quote"] = "title_body"
        elif idx == total - 1:
            layout = "quote"
        else:
            layout = "title_body"

        accent_text = None
        # Use a short preview of the visual description as accent, if available
        visual_preview = (scene.visual_content or "").strip()
        if visual_preview:
            accent_text = (visual_preview[:80] + "…") if len(visual_preview) > 80 else visual_preview

        frame = Frame(
            scene_id=idx,
            layout=layout,
            headline=headline,
            body=body,
            accent_text=accent_text,
            theme="light",
            animation="fade",
        )
        frames.append(frame)

    return frames


def save_frames(frames: List[Frame], output_path: Path) -> None:
    """Save frames list to frames.json."""
    data = [asdict(f) for f in frames]
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def load_frames(input_path: Path) -> List[Frame]:
    """Load frames list from frames.json."""
    if not input_path.exists():
        raise FileNotFoundError(f"Frame file not found: {input_path}")
    with open(input_path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    return [Frame(**item) for item in raw]


def _load_frame_template() -> str:
    """Load the base HTML template used for all frames."""
    template_path = Path(__file__).parent / "templates" / "frame_base.html"
    return template_path.read_text(encoding="utf-8")


def render_frame_html(frame: Frame, template: str) -> str:
    """Render a single frame to HTML using a very small placeholder format.

    The template is expected to contain the placeholders:
    {{ layout }}, {{ theme }}, {{ animation }}, {{ headline }}, {{ body }}, {{ accent_text }}.
    """
    html = template
    replacements = {
        "{{ layout }}": frame.layout,
        "{{ theme }}": frame.theme,
        "{{ animation }}": frame.animation,
        "{{ headline }}": frame.headline,
        "{{ body }}": frame.body,
        "{{ accent_text }}": frame.accent_text or "",
    }
    for placeholder, value in replacements.items():
        html = html.replace(placeholder, value)
    return html


def generate_frames_artifacts(output_dir: Path) -> List[Frame]:
    """Generate frames.json and per-scene HTML files for a pipeline run.

    Expects script.json to already exist in output_dir.
    """
    script_file = output_dir / "script.json"
    scenes = load_scenes(script_file)

    frames = generate_frames(scenes)

    # Save frames.json
    frames_json = output_dir / "frames.json"
    save_frames(frames, frames_json)

    # Render individual HTML files
    frames_dir = output_dir / "frames"
    frames_dir.mkdir(parents=True, exist_ok=True)
    template = _load_frame_template()

    for frame in frames:
        file_path = frames_dir / f"scene_{frame.scene_id:02d}.html"
        html = render_frame_html(frame, template)
        file_path.write_text(html, encoding="utf-8")

    return frames


def build_presentation(output_dir: Path) -> dict:
    """Compose frames + audio metadata into presentation.json."""
    frames_path = output_dir / "frames.json"
    audio_meta_path = output_dir / "audio_metadata.json"

    frames = load_frames(frames_path)

    scene_boundaries = []
    if audio_meta_path.exists():
        with open(audio_meta_path, "r", encoding="utf-8") as f:
            audio_meta = json.load(f)
        scene_boundaries = audio_meta.get("scene_boundaries", [])

    items = []
    for frame in frames:
        idx = frame.scene_id
        timing = scene_boundaries[idx] if idx < len(scene_boundaries) else {}
        start_time = timing.get("start_time", 0.0)
        end_time = timing.get("end_time", timing.get("clip_duration", start_time))

        items.append(
            {
                "scene_id": idx,
                "headline": frame.headline,
                "body": frame.body,
                "accent_text": frame.accent_text,
                "layout": frame.layout,
                "theme": frame.theme,
                "animation": frame.animation,
                "frame_html_path": f"frames/scene_{idx:02d}.html",
                "start_time": start_time,
                "end_time": end_time,
            }
        )

    presentation = {
        "audio": "audio.wav",
        "frames": items,
        "created_at": datetime.utcnow().isoformat() + "Z",
    }

    presentation_path = output_dir / "presentation.json"
    with open(presentation_path, "w", encoding="utf-8") as f:
        json.dump(presentation, f, indent=2, ensure_ascii=False)

    return presentation

