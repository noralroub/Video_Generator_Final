"""Generate audio-synced HTML video presentations with Claude."""

from __future__ import annotations

import json
import logging
import os
import re
from pathlib import Path
from typing import Any

from scenes import compute_phrase_timings, enrich_scene, load_scenes

logger = logging.getLogger(__name__)

PLACEHOLDER_TITLE = "__PRESENTATION_TITLE__"
PLACEHOLDER_AUDIO_SRC = "__AUDIO_SRC__"
PLACEHOLDER_SCENE_DURATIONS_JSON = "__SCENE_DURATIONS_JSON__"
PLACEHOLDER_TOTAL_DURATION_JSON = "__TOTAL_DURATION_JSON__"

SYSTEM_FONT_STACK = (
    'Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif'
)

PROMPT_TEMPLATE = """You are generating a complete, self-contained HTML/CSS/JavaScript vertical video for a scientific paper explainer.

Paper title:
{title}

Scenes (one `.scene` per segment):
{scenes_json}

Style reference excerpt (quality bar — do NOT copy content, colors, or exact layout):
{sample_excerpt}

Component catalog (use kit classes — do NOT invent new chart types when visual_layout is set):
- headline_only: centered `.headline` / `.subtext` phrase reveals in `.narration-block`
- stat_counter: `.stat-counter` with `data-animate="counter" data-target="N"` plus optional `.stat-counter-label`
- stat_pair: `.stat-grid` with two `.stat-grid-item` cells and counter values
- bar_comparison: `.bar-chart` with `.bar-row` / `.bar-fill data-animate="bar" data-value="0-100"`
- mini_table: `.mini-table-wrap` with `.mini-table` (2-4 rows)
- figure_focus: `.figure-panel.ken-burns` with `<img object-fit contain>` and `.figure-label`

Creative direction:
1. Match the reference craft: cinematic motion, kinetic typography, ambient backgrounds, evidence panels when data/images are provided.
2. Invent a UNIQUE visual identity for THIS paper — fresh palette and motifs. Every paper should look different.
3. Full-bleed 9:16 stage (width 100%, aspect-ratio 9/16). Center the stage on the page. No phone-chrome borders.
4. System fonts only: {font_stack}. No external fonts, CDNs, or libraries.
5. REQUIRED on every scene:
   - `.ambient` wrapper with 2-3 `.ambient-orb` divs (opacity 0.25-0.35, pointer-events none, slow drift)
   - On-screen text from `display_phrases` using `[data-reveal]` and each phrase's `enter_ms`
   - Wrap each `emphasis` word in `<span class="emph">` inside its phrase element
   - Source figure `<img>` tags wrapped in `.ken-burns`
   - When `visual_layout` is set, render the matching catalog component using `chart_data`
6. Typography: max ~12 words per visible line; 2-4 phrase reveals; centered flex layout. Do not dump full narration as a wall of text.
7. Translate visual_description into HTML/CSS visuals. Do not display visual_description as visible text.
8. Keep visuals and narration vertically centered together in each scene — use `.scene` flex gap, not top/bottom split.
9. Do NOT duplicate kit CSS for ambient, reveals, headline, subtext, stat-counter, bar-chart, mini-table — those are injected automatically. Scene-specific accent colors via CSS variables are fine.

Audio-sync contract (NON-NEGOTIABLE — do not use setTimeout or data-dur for scene advance):
1. Output one complete HTML document: <!DOCTYPE html>, <html>, <head>, <style>, <body>, <script>. No markdown fences.
2. Exactly one element with class `scene` per scene. Each must have `data-scene-index="N"` (1-based). First scene also has class `active`.
3. Include `<audio id="narration" src="__AUDIO_SRC__">` (hidden via CSS).
4. Include play/pause button with `data-play-toggle`, progress bar with `data-progress-bar`, and optional time display.
5. In the script, include these exact lines on separate lines:
   var sceneDurations = __SCENE_DURATIONS_JSON__;
   var totalDuration = __TOTAL_DURATION_JSON__;
6. Scene switching MUST be driven by `narration.currentTime` and cumulative sceneDurations — NOT timers.
7. Query scenes with `document.querySelectorAll('.scene')`.
8. No inline event handlers; use addEventListener.
9. Do not mention Claude, Anthropic, or implementation details in visible UI.

Output only raw HTML."""


def _sample_path() -> Path:
    return Path(__file__).parent / "templates" / "sample_presentation.html"


def _motion_snippet_path() -> Path:
    return Path(__file__).parent / "templates" / "motion_sync_snippet.html"


def _load_motion_css() -> str:
    path = _motion_snippet_path()
    if not path.exists():
        return ""
    text = path.read_text(encoding="utf-8")
    match = re.search(r"<style>(.*?)</style>", text, re.DOTALL | re.IGNORECASE)
    return match.group(1).strip() if match else ""


def _load_motion_js() -> str:
    path = _motion_snippet_path()
    if not path.exists():
        return ""
    text = path.read_text(encoding="utf-8")
    match = re.search(r"<script>(.*?)</script>", text, re.DOTALL | re.IGNORECASE)
    return match.group(1).strip() if match else ""


def _motion_sync_script() -> str:
    """Injectable kit sync script with placeholders for audio timing."""
    js = _load_motion_js()
    if not js:
        return _reveal_sync_script()
    js = js.replace("__SCENE_DURATIONS_JSON__", PLACEHOLDER_SCENE_DURATIONS_JSON)
    js = js.replace("__TOTAL_DURATION_JSON__", PLACEHOLDER_TOTAL_DURATION_JSON)
    return f'\n<script data-infodemica-reveals="true">\n{js}\n</script>\n'


def _load_motion_example_markup() -> str:
    path = _motion_snippet_path()
    if not path.exists():
        return ""
    text = path.read_text(encoding="utf-8")
    examples = re.findall(r"<!-- ── Example.*?(?=\n<!-- ──|\n<script>)", text, re.DOTALL)
    return "\n\n".join(example.strip() for example in examples if example.strip())


def _load_sample_excerpt(max_chars: int = 12000) -> str:
    """Load a curated excerpt from the sample presentation for style reference."""
    path = _sample_path()
    if not path.exists():
        return "(No sample available.)"
    text = path.read_text(encoding="utf-8")
    lines = text.splitlines()
    css_end = 0
    for idx, line in enumerate(lines):
        if "</style>" in line.lower():
            css_end = idx + 1
            break
    css_block = "\n".join(lines[: min(css_end, 200)])
    scene_start = next((i for i, line in enumerate(lines) if "SCENE 1" in line), None)
    scene_block = ""
    if scene_start is not None:
        scene_block = "\n".join(lines[scene_start : scene_start + 15])
    motion_example = _load_motion_example_markup()
    excerpt = f"{css_block}\n\n<!-- Example scene markup -->\n{scene_block}"
    if motion_example:
        excerpt += f"\n\n{motion_example}"
    if len(excerpt) > max_chars:
        excerpt = excerpt[:max_chars] + "\n<!-- truncated -->"
    return excerpt


def _load_paper(paper_path: Path | None) -> dict[str, Any]:
    if not paper_path or not paper_path.exists():
        return {"title": "Research explainer"}
    try:
        with open(paper_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return {"title": "Research explainer"}
    return {"title": data.get("title") or "Research explainer"}


def _resolve_source_figure(output_dir: Path, figure: dict[str, Any] | None, index: int) -> dict[str, Any] | None:
    if not figure:
        return None
    resolved = dict(figure)
    figures_dir = output_dir / "source_figures"
    if figures_dir.exists():
        for path in sorted(figures_dir.glob(f"figure_{index}.*")):
            resolved["url"] = f"source_figures/{path.name}"
            return resolved
        for path in sorted(figures_dir.glob("figure_*.*")):
            resolved["url"] = f"source_figures/{path.name}"
            return resolved
    url = (figure.get("url") or "").strip()
    if url and not url.startswith(("http://", "https://", "/")):
        resolved["url"] = url
    return resolved


def _load_scene_durations(metadata_path: Path | None, scene_count: int, default: float = 6.0) -> list[float]:
    if not metadata_path or not metadata_path.exists():
        return [default] * scene_count
    try:
        with open(metadata_path, "r", encoding="utf-8") as f:
            metadata = json.load(f)
    except (OSError, json.JSONDecodeError):
        return [default] * scene_count

    boundaries = metadata.get("scene_boundaries", [])
    durations = [
        float(item.get("duration", item.get("clip_duration", default)))
        for item in boundaries
    ]
    if len(durations) < scene_count:
        durations.extend([default] * (scene_count - len(durations)))
    return durations[:scene_count]


def _build_scenes_payload(
    output_dir: Path,
    script_path: Path,
    scene_durations: list[float] | None = None,
) -> list[dict[str, Any]]:
    scenes = load_scenes(script_path)
    if scene_durations is None:
        scene_durations = _load_scene_durations(output_dir / "audio_metadata.json", len(scenes))
    payload = []
    for idx, scene in enumerate(scenes):
        enriched = enrich_scene(scene)
        phrases = compute_phrase_timings(
            enriched.display_phrases or [],
            scene_durations[idx] if idx < len(scene_durations) else 6.0,
        )
        payload.append(
            {
                "index": idx + 1,
                "narration": enriched.text,
                "visual_description": enriched.visual_content,
                "display_phrases": phrases,
                "visual_layout": enriched.visual_layout,
                "chart_data": enriched.chart_data,
                "source_table": enriched.source_table,
                "source_figure": _resolve_source_figure(output_dir, enriched.source_figure, idx),
            }
        )
    return payload


def _strip_code_fence(text: str) -> str:
    stripped = text.strip()
    if not stripped.startswith("```"):
        return stripped
    lines = stripped.splitlines()
    if lines and lines[0].startswith("```"):
        lines = lines[1:]
    if lines and lines[-1].strip() == "```":
        lines = lines[:-1]
    return "\n".join(lines).strip()


def _reveal_sync_script() -> str:
    """Standalone reveal handler — additive when Claude provides its own scene sync."""
    return f"""
<script data-infodemica-reveals="true">
(function () {{
  var narration = document.getElementById('narration');
  var scenes = Array.prototype.slice.call(document.querySelectorAll('.scene'));
  var sceneDurations = {PLACEHOLDER_SCENE_DURATIONS_JSON};
  var lastSceneIndex = -1;

  function sceneIndexForTime(time) {{
    var elapsed = 0;
    for (var i = 0; i < sceneDurations.length; i += 1) {{
      elapsed += Number(sceneDurations[i]) || 0;
      if (time <= elapsed) return i;
    }}
    return Math.max(0, scenes.length - 1);
  }}

  function sceneStartTime(index) {{
    var start = 0;
    for (var i = 0; i < index; i += 1) {{
      start += Number(sceneDurations[i]) || 0;
    }}
    return start;
  }}

  function resetReveals() {{
    document.querySelectorAll('[data-reveal], .reveal-line').forEach(function (el) {{
      el.classList.remove('is-visible');
    }});
  }}

  function updateReveals(sceneIndex, currentTime) {{
    var localMs = (currentTime - sceneStartTime(sceneIndex)) * 1000;
    var scene = scenes[sceneIndex];
    if (!scene) return;
    scene.querySelectorAll('[data-reveal], .reveal-line').forEach(function (el) {{
      var enterMs = parseFloat(el.getAttribute('data-enter-ms') || '0');
      if (localMs >= enterMs) el.classList.add('is-visible');
    }});
  }}

  function update() {{
    if (!narration || !scenes.length) return;
    var current = narration.currentTime || 0;
    var idx = sceneIndexForTime(current);
    if (idx !== lastSceneIndex) {{
      lastSceneIndex = idx;
      resetReveals();
    }}
    updateReveals(idx, current);
  }}

  if (narration) {{
    narration.addEventListener('timeupdate', update);
    narration.addEventListener('seeked', update);
    narration.addEventListener('ended', update);
  }}
  update();
}}());
</script>
"""


def _has_builtin_audio_sync(html: str) -> bool:
    """True when HTML already includes a narration-driven scene sync script."""
    lower = html.lower()
    has_narration = (
        'id="narration"' in lower
        or "getelementbyid('narration')" in lower
        or 'getelementbyid("narration")' in lower
    )
    has_durations = (
        PLACEHOLDER_SCENE_DURATIONS_JSON in html
        or re.search(r"scenedurations\s*=", lower.replace(" ", "")) is not None
    )
    has_timeupdate = "timeupdate" in lower
    has_scenes = 'class="scene' in lower or "queryselectorall('.scene')" in lower
    return has_narration and has_durations and has_timeupdate and has_scenes


def _has_claude_audio_sync(html: str) -> bool:
    """True when a non-kit script already drives narration/scene sync."""
    without_kit = re.sub(
        r'<script\s+data-infodemica-reveals="true"\s*>.*?</script>\s*',
        "",
        html,
        count=1,
        flags=re.DOTALL | re.IGNORECASE,
    )
    return _has_builtin_audio_sync(without_kit)


def sanitize_presentation_html(html: str) -> str:
    """Drop injected kit sync when Claude already shipped a complete audio player."""
    if not _has_claude_audio_sync(html) or "data-infodemica-reveals" not in html:
        return html
    stripped = re.sub(
        r'<script\s+data-infodemica-reveals="true"\s*>.*?</script>\s*',
        "",
        html,
        count=1,
        flags=re.DOTALL | re.IGNORECASE,
    )
    return stripped


def _fallback_sync_script() -> str:
    """Full audio sync with scene switching, controls, reveals, and kit components."""
    return _motion_sync_script()


def _ensure_motion(html: str) -> str:
    """Inject or refresh shared motion CSS and audio-synced reveal handling."""
    css = _load_motion_css()
    if css:
        motion_style = f'<style data-infodemica-motion="true">\n{css}\n</style>'
        if re.search(r'<style\s+data-infodemica-motion="true"\s*>', html, re.IGNORECASE):
            html = re.sub(
                r'<style\s+data-infodemica-motion="true"\s*>.*?</style>\s*',
                motion_style + "\n",
                html,
                count=1,
                flags=re.DOTALL | re.IGNORECASE,
            )
        elif "</head>" in html:
            html = html.replace("</head>", motion_style + "\n</head>", 1)

    if "data-infodemica-reveals" not in html and not _has_claude_audio_sync(html) and "</body>" in html:
        html = html.replace("</body>", _motion_sync_script() + "</body>", 1)

    return html


def _ensure_contract(html: str) -> str:
    """Ensure Claude's HTML contains the placeholders needed for audio sync."""
    if PLACEHOLDER_TITLE not in html:
        if "<title>" in html and "</title>" in html:
            start = html.find("<title>") + len("<title>")
            end = html.find("</title>", start)
            html = html[:start] + PLACEHOLDER_TITLE + html[end:]
        elif "</head>" in html:
            html = html.replace("</head>", f"<title>{PLACEHOLDER_TITLE}</title></head>", 1)

    if PLACEHOLDER_AUDIO_SRC not in html:
        audio_markup = (
            f'<audio id="narration" src="{PLACEHOLDER_AUDIO_SRC}" '
            'preload="auto" style="display:none"></audio>'
        )
        if "<body" in html:
            body_close = html.find(">", html.find("<body"))
            html = html[: body_close + 1] + audio_markup + html[body_close + 1 :]
        else:
            html = audio_markup + html

    needs_sync_injection = (
        PLACEHOLDER_SCENE_DURATIONS_JSON not in html
        or PLACEHOLDER_TOTAL_DURATION_JSON not in html
    ) and not _has_claude_audio_sync(html)

    if needs_sync_injection:
        sync_script = _fallback_sync_script()
        if "</body>" in html:
            html = html.replace("</body>", sync_script + "</body>", 1)
        else:
            html += sync_script

    html = _ensure_motion(html)
    html = _ensure_visible_code_cleanup(html)
    return sanitize_presentation_html(html)


def _ensure_visible_code_cleanup(html: str) -> str:
    """Remove accidental visible CSS/code text from Claude-generated presentations."""
    marker = "data-infodemica-code-cleanup"
    if marker in html:
        return html

    cleanup_script = """
<script data-infodemica-code-cleanup="true">
(function () {
  function looksLikeLeakedCode(text) {
    var normalized = String(text || '').replace(/\\s+/g, ' ').trim();
    if (!normalized) return false;
    if (/@keyframes\\b|}\\s*\\.[A-Za-z0-9_-]+\\s*\\{|\\b(animation|transform|opacity|background|font-size|z-index)\\s*:/.test(normalized)) return true;
    return /[.#]?[A-Za-z0-9_-]+\\s*\\{[^}]{12,}\\}/.test(normalized);
  }

  function cleanNode(node) {
    if (!node) return;
    var skipped = /^(SCRIPT|STYLE|NOSCRIPT|TEXTAREA|SVG)$/;
    if (node.nodeType === Node.TEXT_NODE) {
      if (looksLikeLeakedCode(node.nodeValue)) node.nodeValue = '';
      return;
    }
    if (node.nodeType !== Node.ELEMENT_NODE || skipped.test(node.tagName)) return;
    Array.prototype.slice.call(node.childNodes).forEach(cleanNode);
  }

  cleanNode(document.body);
}());
</script>
"""
    if "</body>" in html:
        return html.replace("</body>", cleanup_script + "</body>", 1)
    return html + cleanup_script


def _pick_available_model(client: Any) -> str | None:
    """Choose a model visible to this API key, preferring Sonnet for HTML generation."""
    try:
        models = list(client.models.list(limit=50).data)
    except Exception as exc:
        logger.warning("Could not list Anthropic models for fallback: %s", exc)
        return None

    ids = [getattr(model, "id", "") for model in models]
    for needle in ("sonnet", "haiku", "opus"):
        for model_id in ids:
            if needle in model_id:
                return model_id
    return ids[0] if ids else None


def _scene_element_count(html: str) -> int:
    return len(
        re.findall(
            r"<(?:div|section|article)\b[^>]*class=[\"'][^\"']*\bscene\b[^\"']*[\"']",
            html,
            flags=re.IGNORECASE,
        )
    )


def _is_complete_html(html: str, expected_scenes: int) -> bool:
    lower = html.lower()
    return (
        "<!doctype html" in lower
        and "</body>" in lower
        and "</html>" in lower
        and _scene_element_count(html) >= expected_scenes
    )


def generate_presentation_html(
    script_path: Path,
    paper_path: Path | None = None,
    api_key: str | None = None,
    output_dir: Path | None = None,
) -> str:
    """Call Claude to generate a self-contained HTML video presentation."""
    if api_key is None:
        api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY environment variable not set")

    output_dir = output_dir or script_path.parent
    scenes = load_scenes(script_path)
    if not scenes:
        raise ValueError("script.json has no scenes")

    paper = _load_paper(paper_path)
    scene_durations = _load_scene_durations(output_dir / "audio_metadata.json", len(scenes))
    scenes_data = _build_scenes_payload(output_dir, script_path, scene_durations)

    prompt = PROMPT_TEMPLATE.format(
        title=paper["title"],
        scenes_json=json.dumps(scenes_data, indent=2, ensure_ascii=False),
        sample_excerpt=_load_sample_excerpt(),
        font_stack=SYSTEM_FONT_STACK,
    )

    try:
        from anthropic import Anthropic
    except ImportError as exc:
        raise ImportError("Install the anthropic package to generate Claude presentations") from exc

    client = Anthropic(api_key=api_key)
    model = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-6")
    max_tokens = int(os.getenv("ANTHROPIC_MAX_TOKENS", "16000"))
    logger.info("Calling Claude to generate HTML presentation with model %s", model)
    try:
        response = client.messages.create(
            model=model,
            max_tokens=max_tokens,
            messages=[{"role": "user", "content": prompt}],
        )
    except Exception as exc:
        if "not_found_error" not in str(exc) and "model:" not in str(exc):
            raise
        fallback_model = _pick_available_model(client)
        if not fallback_model or fallback_model == model:
            raise
        logger.warning("Model %s unavailable; retrying with %s", model, fallback_model)
        response = client.messages.create(
            model=fallback_model,
            max_tokens=max_tokens,
            messages=[{"role": "user", "content": prompt}],
        )
    html = _strip_code_fence(response.content[0].text)
    html = _ensure_contract(html)
    if not _is_complete_html(html, len(scenes)):
        logger.warning("Claude returned incomplete HTML; retrying with stricter prompt")
        repair_prompt = (
            "Your previous HTML was incomplete. Return a COMPLETE raw HTML document now. "
            f"It must include exactly {len(scenes)} elements with class \"scene\", one per scene, "
            "plus </body> and </html>. Use rich motion graphics but keep audio-sync contract. "
            "Do not omit any scene. Use the exact audio/sync placeholders from the original instructions.\n\n"
            + prompt
        )
        response = client.messages.create(
            model=model,
            max_tokens=max_tokens,
            messages=[{"role": "user", "content": repair_prompt}],
        )
        html = _ensure_contract(_strip_code_fence(response.content[0].text))
    return html
