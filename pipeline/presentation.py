"""Render Claude-generated HTML presentations with local audio timing."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from html import escape
from pathlib import Path

from claude_presentation import (
    PLACEHOLDER_AUDIO_SRC,
    PLACEHOLDER_SCENE_DURATIONS_JSON,
    PLACEHOLDER_TITLE,
    PLACEHOLDER_TOTAL_DURATION_JSON,
    _ensure_contract,
    generate_presentation_html,
)
from scenes import compute_phrase_timings, enrich_scene, load_scenes, wrap_emphasis_html

logger = logging.getLogger(__name__)

DEFAULT_SCENE_DURATION = 6.0


def _load_audio_durations(metadata_path: Path, scene_count: int) -> tuple[list[float], float]:
    if not metadata_path.exists():
        durations = [DEFAULT_SCENE_DURATION] * scene_count
        return durations, sum(durations)

    with open(metadata_path, "r", encoding="utf-8") as f:
        metadata = json.load(f)

    boundaries = metadata.get("scene_boundaries", [])
    durations = [
        float(item.get("duration", item.get("clip_duration", DEFAULT_SCENE_DURATION)))
        for item in boundaries
    ]
    if len(durations) < scene_count:
        durations.extend([DEFAULT_SCENE_DURATION] * (scene_count - len(durations)))
    else:
        durations = durations[:scene_count]
    return durations, sum(durations)


def _load_title(paper_path: Path | None, fallback: str) -> str:
    if not paper_path or not paper_path.exists():
        return fallback
    try:
        with open(paper_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return fallback
    return data.get("title") or fallback


def _ambient_markup(accent_a: str, accent_b: str) -> str:
    return f"""
        <div class="ambient" aria-hidden="true">
          <div class="ambient-orb" style="width:260px;height:260px;top:-60px;right:-80px;background:radial-gradient(circle,{accent_a},transparent 70%);"></div>
          <div class="ambient-orb" style="width:220px;height:220px;bottom:-40px;left:-60px;background:radial-gradient(circle,{accent_b},transparent 70%);animation-delay:-6s;"></div>
        </div>"""


def _layout_component_markup(scene, layout: str, chart_data: dict | None) -> str:
    chart_data = chart_data or {}
    source_table = getattr(scene, "source_table", None) or {}
    source_figure = getattr(scene, "source_figure", None) or {}

    if layout == "figure_focus" and source_figure:
        label = escape(source_figure.get("id") or "Source figure")
        caption = escape(source_figure.get("caption") or "")
        image_url = escape(source_figure.get("url") or "")
        image = f'<img src="{image_url}" alt="{label}">' if image_url else ""
        return f"""
        <div class="figure-panel evidence-panel ken-burns" data-reveal data-enter-ms="0">
          {image}
          <div class="figure-label">{label}{(" — " + caption) if caption else ""}</div>
        </div>"""

    if layout == "mini_table" and source_table:
        label = escape(source_table.get("label") or source_table.get("id") or "Data")
        rows = []
        for raw_row in (source_table.get("text") or "").splitlines()[:4]:
            cells = [escape(cell.strip()) for cell in raw_row.split("|") if cell.strip()]
            if cells:
                rows.append("<tr>" + "".join(f"<td>{cell}</td>" for cell in cells[:4]) + "</tr>")
        if rows:
            return f"""
        <div class="mini-table-wrap evidence-panel" data-reveal data-enter-ms="0">
          <table class="mini-table">{"".join(rows)}</table>
        </div>"""

    if layout == "stat_counter":
        value = chart_data.get("value", 0)
        try:
            target = int(float(str(value).replace(",", "")))
        except (TypeError, ValueError):
            target = 0
        label = escape(str(chart_data.get("label") or ""))
        label_markup = f'<p class="stat-counter-label" data-reveal data-enter-ms="400">{label}</p>' if label else ""
        return f"""
        <div class="stat-counter" data-animate="counter" data-target="{target}" data-reveal data-enter-ms="0">0</div>
        {label_markup}"""

    if layout == "stat_pair":
        values = chart_data.get("values") or [0, 0]
        labels = chart_data.get("labels") or ["A", "B"]
        items = []
        for idx, val in enumerate(values[:2]):
            try:
                target = int(float(str(val).replace(",", "")))
            except (TypeError, ValueError):
                target = 0
            lbl = escape(str(labels[idx] if idx < len(labels) else f"Value {idx + 1}"))
            enter = idx * 200
            items.append(f"""
          <div class="stat-grid-item" data-reveal data-enter-ms="{enter}">
            <div class="stat-grid-val" data-animate="counter" data-target="{target}">0</div>
            <div class="stat-grid-label">{lbl}</div>
          </div>""")
        return f'<div class="stat-grid">{"".join(items)}</div>'

    if layout == "bar_comparison":
        rows = chart_data.get("rows") or []
        if not rows and chart_data.get("values"):
            values = chart_data.get("values") or []
            labels = chart_data.get("labels") or ["A", "B"]
            rows = [
                {
                    "label": str(labels[i] if i < len(labels) else f"Row {i + 1}"),
                    "values": [values[i]],
                }
                for i in range(min(len(values), 4))
            ]
        bar_rows = []
        for idx, row in enumerate(rows[:4]):
            label = escape(str(row.get("label") or f"Row {idx + 1}"))
            row_values = row.get("values") or [0]
            try:
                width = min(100, max(0, float(row_values[0])))
            except (TypeError, ValueError):
                width = 0
            bar_rows.append(f"""
          <div class="bar-row" data-reveal data-enter-ms="{idx * 250}">
            <div class="bar-label">{label}</div>
            <div class="bar-track"><div class="bar-fill" data-animate="bar" data-value="{width}"></div></div>
          </div>""")
        if bar_rows:
            return f'<div class="bar-chart evidence-panel">{"".join(bar_rows)}</div>'

    return ""


def _phrase_markup(phrases: list[dict]) -> str:
    lines = []
    for idx, phrase in enumerate(phrases):
        text_html = wrap_emphasis_html(phrase.get("text", ""), phrase.get("emphasis") or [])
        enter_ms = int(phrase.get("enter_ms", idx * 400))
        tag = "h1" if idx == 0 and len(phrases) <= 2 else "p"
        css_class = "headline" if tag == "h1" else "subtext"
        lines.append(
            f'<{tag} class="{css_class}" data-reveal data-enter-ms="{enter_ms}">{text_html}</{tag}>'
        )
    return "\n        ".join(lines)


def _fallback_html(title: str, scenes: list, scene_durations: list[float], total_duration: float, audio_src: str) -> str:
    """Create a deterministic HTML video when Claude output is incomplete."""
    accents = [
        ("rgba(255,107,53,0.28)", "rgba(53,208,255,0.22)"),
        ("rgba(53,208,255,0.3)", "rgba(53,255,176,0.18)"),
        ("rgba(255,51,85,0.28)", "rgba(236,72,153,0.18)"),
        ("rgba(255,209,102,0.28)", "rgba(99,102,241,0.2)"),
        ("rgba(99,102,241,0.28)", "rgba(34,197,94,0.18)"),
    ]
    scene_markup = []
    for idx, scene in enumerate(scenes):
        enriched = enrich_scene(scene)
        duration = scene_durations[idx] if idx < len(scene_durations) else DEFAULT_SCENE_DURATION
        phrases = compute_phrase_timings(enriched.display_phrases or [], duration)
        accent_a, accent_b = accents[idx % len(accents)]
        active = " active" if idx == 0 else ""
        layout = enriched.visual_layout or "headline_only"
        component = _layout_component_markup(enriched, layout, enriched.chart_data)
        scene_markup.append(
            f"""
      <section class="scene{active}" data-scene-index="{idx + 1}" style="--accent:#35d0ff;">
        {_ambient_markup(accent_a, accent_b)}
        {component}
        <div class="narration-block">
          {_phrase_markup(phrases)}
        </div>
      </section>"""
        )

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{PLACEHOLDER_TITLE}</title>
<style>
body {{ margin: 0; }}
audio {{ display: none; }}
.controls {{
  position: absolute;
  z-index: 10;
  left: 18px;
  right: 18px;
  bottom: 18px;
  display: flex;
  align-items: center;
  gap: 12px;
}}
.play {{
  width: 46px;
  height: 46px;
  border: 0;
  border-radius: 999px;
  color: #020617;
  background: white;
  font-size: 18px;
  cursor: pointer;
}}
.progress {{
  flex: 1;
  height: 6px;
  overflow: hidden;
  border-radius: 999px;
  background: rgba(255,255,255,.18);
}}
.progress-fill {{
  width: 0%;
  height: 100%;
  background: linear-gradient(90deg, #fff, #93c5fd, #f0abfc);
}}
.time {{
  width: 42px;
  font-size: 12px;
  color: rgba(255,255,255,.72);
  text-align: right;
}}
</style>
</head>
<body>
  <main class="stage">
    <audio id="narration" src="{PLACEHOLDER_AUDIO_SRC}" preload="auto"></audio>
    {"".join(scene_markup)}
    <div class="controls">
      <button class="play" data-play-toggle aria-label="Play or pause">▶</button>
      <div class="progress"><div class="progress-fill" data-progress-bar></div></div>
      <div class="time" data-time>0:00</div>
    </div>
  </main>
<script>
  var sceneDurations = {PLACEHOLDER_SCENE_DURATIONS_JSON};
  var totalDuration = {PLACEHOLDER_TOTAL_DURATION_JSON};
</script>
</body>
</html>"""
    return _ensure_contract(html)


def render_presentation_claude(
    script_path: Path,
    output_path: Path,
    audio_metadata_path: Path | None = None,
    audio_src: str = "audio.wav",
    paper_path: Path | None = None,
) -> None:
    """Generate and write a Claude HTML presentation."""
    scenes = load_scenes(script_path)
    if not scenes:
        raise ValueError("script.json has no scenes")

    html = generate_presentation_html(script_path, paper_path, output_dir=script_path.parent)
    scene_durations, total_duration = _load_audio_durations(
        audio_metadata_path or Path(""), len(scenes)
    )

    title = _load_title(paper_path, scenes[0].text)
    html = (
        html.replace(PLACEHOLDER_TITLE, title)
        .replace(PLACEHOLDER_AUDIO_SRC, audio_src)
        .replace(PLACEHOLDER_SCENE_DURATIONS_JSON, json.dumps(scene_durations))
        .replace(PLACEHOLDER_TOTAL_DURATION_JSON, json.dumps(total_duration))
    )

    if html.count('class="scene') < len(scenes) or "</html>" not in html or "</body>" not in html:
        logger.warning("Claude presentation was incomplete; using deterministic HTML video fallback")
        html = _fallback_html(title, scenes, scene_durations, total_duration, audio_src)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html, encoding="utf-8")
    logger.info(
        "Rendered Claude presentation to %s (%s scenes, %.1fs)",
        output_path,
        len(scenes),
        total_duration,
    )


def build_presentation_metadata(output_dir: Path) -> dict:
    """Write presentation.json metadata for a Claude-generated video."""
    script_path = output_dir / "script.json"
    scenes = load_scenes(script_path)
    scene_boundaries = []
    audio_meta_path = output_dir / "audio_metadata.json"
    if audio_meta_path.exists():
        with open(audio_meta_path, "r", encoding="utf-8") as f:
            scene_boundaries = json.load(f).get("scene_boundaries", [])

    presentation = {
        "schema_version": 3,
        "audio": "audio.wav",
        "presentation_html": "presentation.html",
        "scene_count": len(scenes),
        "scene_boundaries": scene_boundaries,
        "created_at": datetime.utcnow().isoformat() + "Z",
    }
    with open(output_dir / "presentation.json", "w", encoding="utf-8") as f:
        json.dump(presentation, f, indent=2, ensure_ascii=False)
    return presentation
