"""Structured frame generation and presentation composition for paper videos."""

from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass, asdict
from datetime import datetime
from html import escape
from pathlib import Path
from typing import Any, List, Literal

from scenes import Scene, load_scenes

logger = logging.getLogger(__name__)
_CLAUDE_FRAME_MODEL_CACHE: str | None = None

LayoutName = Literal["title_hook", "problem", "key_finding", "source_figure", "source_table", "comparison", "process_timeline", "impact_takeaway"]
LAYOUT_CHOICES: tuple[str, ...] = ("title_hook", "problem", "key_finding", "source_figure", "source_table", "comparison", "process_timeline", "impact_takeaway")
LAYOUT_LABELS: dict[str, str] = {
    "title_hook": "Title/Hook", "problem": "Problem", "key_finding": "Key Finding", "source_figure": "Source Figure",
    "source_table": "Source Table", "comparison": "Comparison", "process_timeline": "Process/Timeline", "impact_takeaway": "Impact/Takeaway",
}

@dataclass(frozen=True)
class Frame:
    """Strict editable schema for one 9:16 video frame."""
    scene_id: int
    order: int
    layout: LayoutName
    headline: str
    narration: str
    body: str = ""
    key_points: list[str] | None = None
    visual_prompt: str = ""
    evidence_table: dict[str, Any] | None = None
    evidence_figure: dict[str, Any] | None = None
    evidence_video: dict[str, Any] | None = None
    accent_text: str = ""
    theme: Literal["light", "dark", "journal_brand"] = "dark"
    animation: Literal["fade", "slide_up"] = "fade"
    html_override: str = ""

    @property
    def text(self) -> str:
        return self.narration

def _sentences(text: str) -> list[str]:
    parts = re.split(r"(?<=[.!?])\s+", (text or "").strip())
    return [part.strip() for part in parts if part.strip()]

def _compact(text: str, limit: int) -> str:
    text = " ".join((text or "").split())
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)].rstrip() + "…"

def _layout_for_scene(idx: int, total: int, scene: Scene) -> LayoutName:
    if getattr(scene, "source_figure", None): return "source_figure"
    if getattr(scene, "source_table", None): return "source_table"
    if idx == 0: return "title_hook"
    if idx == total - 1: return "impact_takeaway"
    text = f"{scene.text} {scene.visual_content}".lower()
    if any(word in text for word in ["compared", "versus", "vs", "more than", "less than"]): return "comparison"
    if any(word in text for word in ["then", "next", "step", "process", "over time"]): return "process_timeline"
    if any(word in text for word in ["found", "showed", "result", "increase", "decrease", "risk", "effect"]): return "key_finding"
    return "problem"

def generate_frames(scenes: List[Scene]) -> List[Frame]:
    frames: List[Frame] = []
    total = len(scenes)
    for idx, scene in enumerate(scenes):
        narration = (scene.text or "").strip()
        parts = _sentences(narration)
        headline = _compact(parts[0] if parts else narration, 82)
        body = _compact(" ".join(parts[1:]) if len(parts) > 1 else "", 220)
        visual_prompt = (scene.visual_content or "").strip()
        layout = _layout_for_scene(idx, total, scene)
        key_points = [_compact(point, 92) for point in _sentences(body)[:3]]
        frames.append(Frame(
            scene_id=idx, order=idx, layout=layout, headline=headline, narration=narration, body=body,
            key_points=key_points, visual_prompt=visual_prompt, evidence_table=getattr(scene, "source_table", None),
            evidence_figure=getattr(scene, "source_figure", None), evidence_video=getattr(scene, "source_video", None), accent_text=LAYOUT_LABELS.get(layout, "Research story"),
            theme="dark" if idx % 2 == 0 else "journal_brand", animation="fade", html_override="",
        ))
    return frames

def frame_to_dict(frame: Frame) -> dict[str, Any]:
    data = asdict(frame)
    data["schema_version"] = 2
    return data

def save_frames(frames: List[Frame], output_path: Path) -> None:
    data = [frame_to_dict(f) for f in sorted(frames, key=lambda item: item.order)]
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def _coerce_layout(value: str) -> LayoutName:
    return value if value in LAYOUT_CHOICES else "key_finding"  # type: ignore[return-value]

def _frame_from_dict(item: dict[str, Any], index: int) -> Frame:
    narration = item.get("narration") or item.get("text") or item.get("headline") or ""
    return Frame(
        scene_id=int(item.get("scene_id", index)), order=int(item.get("order", index)), layout=_coerce_layout(item.get("layout", "key_finding")),
        headline=item.get("headline") or _compact(narration, 82), narration=narration, body=item.get("body", ""),
        key_points=item.get("key_points") or [], visual_prompt=item.get("visual_prompt") or item.get("accent_text") or "",
        evidence_table=item.get("evidence_table") or item.get("source_table"), evidence_figure=item.get("evidence_figure") or item.get("source_figure"), evidence_video=item.get("evidence_video") or item.get("source_video"),
        accent_text=item.get("accent_text") or LAYOUT_LABELS.get(item.get("layout", ""), "Research story"),
        theme=item.get("theme", "dark") if item.get("theme") in {"light", "dark", "journal_brand"} else "dark",
        animation=item.get("animation", "fade") if item.get("animation") in {"fade", "slide_up"} else "fade",
        html_override=item.get("html_override", ""),
    )

def load_frames(input_path: Path) -> List[Frame]:
    if not input_path.exists():
        raise FileNotFoundError(f"Frame file not found: {input_path}")
    with open(input_path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    return [_frame_from_dict(item, idx) for idx, item in enumerate(raw)]

def _load_frame_template() -> str:
    return (Path(__file__).parent / "templates" / "frame_base.html").read_text(encoding="utf-8")

def _looks_like_visual_instruction(text: str, visual_prompt: str = "") -> bool:
    compact = " ".join((text or "").split()).strip()
    if not compact:
        return False
    prompt = " ".join((visual_prompt or "").split()).strip()
    if prompt and compact == prompt:
        return True
    lower = compact.lower()
    instruction_starts = (
        "abstract representation",
        "abstract visual",
        "animated abstract",
        "render ",
        "show ",
        "use ",
        "create ",
        "visualize ",
    )
    instruction_terms = (
        "visualization",
        "glowing",
        "swirling",
        "soft colors",
        "data points",
        "network",
        "nodes",
        "overlaid",
        "animation",
    )
    return lower.startswith(instruction_starts) and any(term in lower for term in instruction_terms)


def _display_body(frame: Frame) -> str:
    return "" if _looks_like_visual_instruction(frame.body, frame.visual_prompt) else frame.body

def _render_key_points(points: list[str] | None, visual_prompt: str = "") -> str:
    filtered = [
        point
        for point in (points or [])
        if point and not _looks_like_visual_instruction(point, visual_prompt)
    ]
    return "".join(f"<li>{escape(point)}</li>" for point in filtered)

def _render_table(table: dict[str, Any] | None) -> str:
    if not table: return ""
    rows = []
    for raw_row in (table.get("text") or "").splitlines()[:5]:
        cells = [escape(cell.strip()) for cell in raw_row.split("|") if cell.strip()]
        if cells:
            rows.append("<tr>" + "".join(f"<td>{cell}</td>" for cell in cells[:4]) + "</tr>")
    return f"""
      <div class=\"evidence evidence-table\">
        <strong>{escape(table.get('label') or table.get('id') or 'Source table')}</strong>
        <span>{escape(table.get('caption') or '')}</span>
        <table>{''.join(rows)}</table>
      </div>
    """

def _render_figure(figure: dict[str, Any] | None) -> str:
    if not figure: return ""
    url = escape(figure.get("url") or "")
    label = escape(figure.get("id") or "Source figure")
    image = f'<img src="{url}" alt="{label}">' if url else ""
    return f"""
      <div class=\"evidence evidence-figure\">
        <strong>{label}</strong>
        {image}
        <span>{escape(figure.get('caption') or '')}</span>
      </div>
    """


def _render_video(video: dict[str, Any] | None) -> str:
    if not video: return ""
    url = escape(video.get("url") or "")
    label = escape(video.get("label") or video.get("name") or "Source video")
    media = f'<video src="{url}" controls muted playsinline></video>' if url else ""
    return f"""
      <div class=\"evidence evidence-video\">
        <strong>{label}</strong>
        {media}
        <span>{escape(video.get('caption') or '')}</span>
      </div>
    """

def _render_generated_visual(prompt: str) -> str:
    """Convert visual notes into a non-text HTML/CSS visual motif."""
    prompt = (prompt or "").strip()
    if not prompt:
        return ""
    lower = prompt.lower()
    classes = ["generated-visual"]
    if any(term in lower for term in ["smile", "smiling", "face", "happy"]):
        classes.append("visual-face")
        inner = '<div class="face"><span class="eye eye-left"></span><span class="eye eye-right"></span><span class="smile"></span></div>'
    elif any(term in lower for term in ["network", "node", "connection", "connected", "social"]):
        classes.append("visual-network")
        inner = ''.join(f'<span class="node node-{idx}"></span>' for idx in range(1, 7)) + ''.join(f'<span class="link link-{idx}"></span>' for idx in range(1, 6))
    elif any(term in lower for term in ["chart", "graph", "bar", "data", "trend"]):
        classes.append("visual-chart")
        inner = ''.join(f'<span class="bar bar-{idx}"></span>' for idx in range(1, 6))
    else:
        classes.append("visual-abstract")
        inner = '<span class="orb orb-a"></span><span class="orb orb-b"></span><span class="orb orb-c"></span><span class="path"></span>'
    return f'<div class="{" ".join(classes)}" aria-hidden="true">{inner}</div>'

def render_frame_html(frame: Frame, template: str | None = None) -> str:
    # HTML overrides are preserved in old frame files for compatibility, but the
    # editor now uses structured fields only so visual notes never become slide text.
    template = template or _load_frame_template()
    evidence = _render_video(frame.evidence_video) or _render_figure(frame.evidence_figure) or _render_table(frame.evidence_table)
    replacements = {
        "{{ layout }}": escape(frame.layout), "{{ layout_label }}": escape(LAYOUT_LABELS.get(frame.layout, frame.layout.replace("_", " ").title())),
        "{{ theme }}": escape(frame.theme), "{{ animation }}": escape(frame.animation), "{{ headline }}": escape(frame.headline),
        "{{ narration }}": escape(frame.narration), "{{ body }}": escape(_display_body(frame)), "{{ key_points }}": _render_key_points(frame.key_points, frame.visual_prompt),
        "{{ visual_prompt }}": escape(frame.visual_prompt), "{{ generated_visual }}": "" if evidence else _render_generated_visual(frame.visual_prompt), "{{ evidence }}": evidence,
        "{{ accent_text }}": escape(frame.accent_text or LAYOUT_LABELS.get(frame.layout, "Research story")),
    }
    html = template
    for placeholder, value in replacements.items():
        html = html.replace(placeholder, value)
    return html

FRAME_HTML_PROMPT = """You are generating ONE self-contained HTML/CSS slide for a vertical scientific explainer video.

Return raw HTML only. Do not use markdown fences. Do not include external assets, network calls, or libraries.

Frame data:
{frame_json}

Design rules:
1. Create a polished 9:16 motion-graphic frame, 360px by 640px, using HTML and CSS only.
2. The narration is context for the voiceover. Do NOT put the full narration on screen.
3. Use at most one short headline or phrase from the headline/body, ideally under 12 words.
4. Translate visual_notes into the actual visual system: shapes, icons, motion, charts, diagrams, character-like CSS figures, color, layout, and atmosphere. Do not display visual_notes as text.
5. If evidence_table is present, make the table/data card the dominant visual. The table region must use at least 78% of the 360x640 stage, with no large headline competing for space. Use condensed fonts, small padding, abbreviated cell text, and responsive scaling so the supplied table content is visible without clipping. If the table is too large, prioritize showing the complete structure and most informative rows/columns over decorative elements.
6. If evidence_figure is present, make the image the dominant visual using its URL and object-fit: contain; keep caption tiny or omit it if long.
7. Use CSS animations subtly. Avoid looking like a PowerPoint slide.
8. Include the brand word Infodemica only in a tiny footer if it fits.
9. The result must be a complete HTML document with <style> in <head> and visible content in <body>.
"""

FRAME_DECK_HTML_PROMPT = """You are generating a complete set of self-contained HTML/CSS slides for a vertical scientific explainer video.

Return valid JSON only. Do not use markdown fences. The JSON shape must be:
{{
  "frames": [
    {{"scene_id": 0, "html": "<!DOCTYPE html>...complete document...</html>"}}
  ]
}}

Frame data:
{frames_json}

Design rules:
1. Create one polished 9:16 motion-graphic frame per item, 360px by 640px, using HTML and CSS only.
2. Each html value must be a complete HTML document with <style> in <head> and visible content in <body>.
3. Keep each html value compact. Prefer CSS shapes, gradients, diagrams, motion paths, charts, and simple character-like figures over long text.
4. The narration is context for the voiceover. Do NOT put the full narration on screen.
5. Use at most one short headline or phrase from the headline/body per frame, ideally under 12 words.
6. Translate visual_notes into the actual visual system. Do not display visual_notes as text.
7. If evidence_table is present, make the table/data card the dominant visual. The table region must use at least 78% of the 360x640 stage, with no large headline competing for space. Use condensed fonts, small padding, abbreviated cell text, and responsive scaling so the supplied table content is visible without clipping. If the table is too large, prioritize showing the complete structure and most informative rows/columns over decorative elements.
8. If evidence_figure is present, make the image the dominant visual using its URL and object-fit: contain; keep caption tiny or omit it if long.
9. Use CSS animations subtly. Avoid looking like a PowerPoint slide.
10. Include the brand word Infodemica only in a tiny footer if it fits.
11. Return exactly {frame_count} frame objects, in the same order as the input.
"""


def _frame_prompt_data(frame: Frame) -> dict[str, Any]:
    return {
        "headline": frame.headline,
        "narration_context": frame.narration,
        "supporting_text": _display_body(frame),
        "visual_notes": frame.visual_prompt,
        "theme": frame.theme,
        "evidence_table": frame.evidence_table,
        "evidence_figure": frame.evidence_figure,
        "evidence_video": frame.evidence_video,
    }


def _claude_client_and_model(api_key: str | None = None) -> tuple[Any, str]:
    if api_key is None:
        api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY environment variable not set")
    try:
        from anthropic import Anthropic
    except ImportError as exc:
        raise ImportError("Install the anthropic package to generate Claude frame HTML") from exc

    client = Anthropic(api_key=api_key)
    model = _claude_frame_model(client, os.getenv("ANTHROPIC_FRAME_MODEL", os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-6")))
    return client, model


def _strip_code_fence(text: str) -> str:
    stripped = (text or "").strip()
    if not stripped.startswith("```"):
        return stripped
    lines = stripped.splitlines()
    if lines and lines[0].startswith("```"):
        lines = lines[1:]
    if lines and lines[-1].strip() == "```":
        lines = lines[:-1]
    return "\n".join(lines).strip()


def _parse_json_response(text: str) -> dict[str, Any]:
    stripped = _strip_code_fence(text)
    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        start = stripped.find("{")
        end = stripped.rfind("}")
        if start == -1 or end == -1 or end <= start:
            raise
        return json.loads(stripped[start : end + 1])


def _is_complete_frame_html(html: str) -> bool:
    lower = (html or "").lower()
    return "<!doctype" in lower and "</html>" in lower and "<body" in lower and "</body>" in lower


def _claude_frame_model(client: Any, preferred: str) -> str:
    global _CLAUDE_FRAME_MODEL_CACHE
    if _CLAUDE_FRAME_MODEL_CACHE:
        return _CLAUDE_FRAME_MODEL_CACHE
    try:
        models = list(client.models.list(limit=50).data)
    except Exception:
        _CLAUDE_FRAME_MODEL_CACHE = preferred
        return preferred
    ids = [getattr(model, "id", "") for model in models]
    if preferred in ids:
        _CLAUDE_FRAME_MODEL_CACHE = preferred
        return preferred
    for needle in ("sonnet", "haiku", "opus"):
        for model_id in ids:
            if needle in model_id:
                _CLAUDE_FRAME_MODEL_CACHE = model_id
                return model_id
    _CLAUDE_FRAME_MODEL_CACHE = ids[0] if ids else preferred
    return _CLAUDE_FRAME_MODEL_CACHE


def render_frame_html_claude(frame: Frame, api_key: str | None = None) -> str:
    client, model = _claude_client_and_model(api_key)
    prompt = FRAME_HTML_PROMPT.format(frame_json=json.dumps(_frame_prompt_data(frame), indent=2, ensure_ascii=False))
    max_tokens = int(os.getenv("ANTHROPIC_FRAME_MAX_TOKENS", "8000"))
    html = _strip_code_fence(_claude_stream_text(client, model, prompt, max_tokens))
    if _is_complete_frame_html(html):
        return html

    repair_prompt = (
        "Your previous response was incomplete. Return ONE COMPLETE raw HTML document now. "
        "It must include <!DOCTYPE html>, <html>, <head>, <style>, <body>, </body>, and </html>. "
        "Keep it compact, but do not omit the visual. Do not use markdown fences.\n\n"
        + prompt
    )
    html = _strip_code_fence(_claude_stream_text(client, model, repair_prompt, max_tokens))
    if not _is_complete_frame_html(html):
        raise ValueError("Claude returned incomplete frame HTML after repair retry")
    return html


def _extract_deck_html(response_text: str, expected_count: int) -> list[str]:
    payload = _parse_json_response(response_text)
    raw_frames = payload.get("frames")
    if not isinstance(raw_frames, list):
        raise ValueError("Claude deck response did not include a frames list")
    if len(raw_frames) != expected_count:
        raise ValueError(f"Claude deck response included {len(raw_frames)} frames, expected {expected_count}")

    html_frames: list[str] = []
    for index, item in enumerate(raw_frames):
        if not isinstance(item, dict):
            raise ValueError(f"Claude deck response frame {index} is not an object")
        html = _strip_code_fence(str(item.get("html") or ""))
        if not _is_complete_frame_html(html):
            raise ValueError(f"Claude deck response frame {index} was incomplete")
        html_frames.append(html)
    return html_frames


def _claude_stream_text(client: Any, model: str, prompt: str, max_tokens: int) -> str:
    chunks: list[str] = []
    with client.messages.stream(
        model=model,
        max_tokens=max_tokens,
        messages=[{"role": "user", "content": prompt}],
    ) as stream:
        for text in stream.text_stream:
            chunks.append(text)
    return "".join(chunks)


def render_frame_deck_html_claude(frames: list[Frame], api_key: str | None = None) -> list[str]:
    ordered_frames = sorted(frames, key=lambda item: item.order)
    if not ordered_frames:
        return []
    client, model = _claude_client_and_model(api_key)
    frame_payload = [_frame_prompt_data(frame) for frame in ordered_frames]
    prompt = FRAME_DECK_HTML_PROMPT.format(
        frames_json=json.dumps(frame_payload, indent=2, ensure_ascii=False),
        frame_count=len(ordered_frames),
    )
    max_tokens = int(os.getenv("ANTHROPIC_DECK_MAX_TOKENS", "24000"))
    response_text = _claude_stream_text(client, model, prompt, max_tokens)
    try:
        return _extract_deck_html(response_text, len(ordered_frames))
    except Exception as exc:
        logger.warning("Claude deck generation returned invalid output; requesting compact repair: %s", exc)

    repair_prompt = (
        "Your previous response was invalid or incomplete. Return valid JSON only, with exactly "
        f"{len(ordered_frames)} frames. Each html string must be a complete compact HTML document "
        "with <!DOCTYPE html>, <html>, <head>, <style>, <body>, </body>, and </html>. "
        "Do not use markdown fences.\n\n"
        + prompt
    )
    response_text = _claude_stream_text(client, model, repair_prompt, max_tokens)
    return _extract_deck_html(response_text, len(ordered_frames))


def render_frame_html_for_output(frame: Frame, template: str | None = None) -> str:
    renderer = os.getenv("FRAME_RENDERER", "claude").lower()
    if renderer == "claude":
        return render_frame_html_claude(frame)
    if renderer == "claude_with_fallback":
        try:
            return render_frame_html_claude(frame)
        except Exception as exc:
            logger.warning("Claude frame generation failed for scene %s; using fallback renderer: %s", frame.scene_id, exc)
    return render_frame_html(frame, template)


def render_frame_files(frames: List[Frame], output_dir: Path) -> None:
    frames_dir = output_dir / "frames"
    frames_dir.mkdir(parents=True, exist_ok=True)
    template = _load_frame_template()
    renderer = os.getenv("FRAME_RENDERER", "claude").lower()
    records = []
    ordered_frames = sorted(frames, key=lambda item: item.order)
    if renderer in {"claude", "claude_with_fallback"}:
        try:
            html_outputs = render_frame_deck_html_claude(ordered_frames)
            renderer_used = "claude"
        except Exception as exc:
            if renderer == "claude":
                raise
            logger.warning("Claude deck generation failed; using fallback renderer: %s", exc)
            html_outputs = [render_frame_html(frame, template) for frame in ordered_frames]
            renderer_used = "templates"
    else:
        html_outputs = [render_frame_html(frame, template) for frame in ordered_frames]
        renderer_used = renderer

    for index, html in enumerate(html_outputs):
        (frames_dir / f"scene_{index:02d}.html").write_text(html, encoding="utf-8")
        records.append({"scene_id": index, "renderer": renderer_used, "complete_html": _is_complete_frame_html(html)})
    with open(output_dir / "frame_render_metadata.json", "w", encoding="utf-8") as f:
        json.dump({"renderer": renderer_used, "generation_mode": "deck" if renderer_used == "claude" else "frame", "frames": records}, f, indent=2)

def generate_frames_artifacts(output_dir: Path) -> List[Frame]:
    frames = generate_frames(load_scenes(output_dir / "script.json"))
    save_frames(frames, output_dir / "frames.json")
    render_frame_files(frames, output_dir)
    return frames

def build_presentation(output_dir: Path) -> dict:
    frames = load_frames(output_dir / "frames.json")
    scene_boundaries = []
    audio_meta_path = output_dir / "audio_metadata.json"
    if audio_meta_path.exists():
        with open(audio_meta_path, "r", encoding="utf-8") as f:
            scene_boundaries = json.load(f).get("scene_boundaries", [])
    items = []
    for output_index, frame in enumerate(sorted(frames, key=lambda item: item.order)):
        timing = scene_boundaries[output_index] if output_index < len(scene_boundaries) else {}
        start_time = timing.get("start_time", 0.0)
        end_time = timing.get("end_time", timing.get("clip_duration", start_time))
        items.append({**frame_to_dict(frame), "scene_id": output_index, "frame_html_path": f"frames/scene_{output_index:02d}.html", "start_time": start_time, "end_time": end_time})
    presentation = {"audio": "audio.wav", "frames": items, "created_at": datetime.utcnow().isoformat() + "Z", "schema_version": 2}
    with open(output_dir / "presentation.json", "w", encoding="utf-8") as f:
        json.dump(presentation, f, indent=2, ensure_ascii=False)
    return presentation
