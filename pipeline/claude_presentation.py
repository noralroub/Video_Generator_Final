"""Generate audio-synced HTML video presentations with Claude."""

from __future__ import annotations

import json
import logging
import os
import re
from pathlib import Path
from typing import Any

from scenes import load_scenes

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

Creative direction:
1. Match the reference's craft: cinematic motion graphics, kinetic typography, ambient backgrounds, per-scene choreography, evidence panels when data/images are provided.
2. Invent a UNIQUE visual identity for THIS paper — fresh palette, motifs, and scene designs suited to the topic. Every paper should look different.
3. Use a full-bleed 9:16 stage (width 100%, aspect-ratio 9/16, no phone-chrome mockup borders). Center the stage on the page.
4. Use system fonts only: {font_stack}. Do NOT load external fonts, CDNs, libraries, or network assets.
5. REQUIRED minimum motion on every scene:
   - Each `.scene` includes a `.ambient` wrapper with 2 `.ambient-orb` divs (continuous background drift).
   - All on-screen text uses `data-reveal` with staggered `data-enter-ms` (phrase-by-phrase, synced to narration pacing within the scene).
   - Source figure `<img>` tags are wrapped in `.ken-burns`.
   Additional per-scene animation (counters, bars, SVG rings, etc.) is encouraged where it fits the content.
6. Use narration as primary on-screen text — large, readable, mobile-friendly. Do not dump full narration as a wall of text.
7. Translate visual_description into HTML/CSS visuals. Do not display visual_description as visible text.
8. If source_table is present, render a compact readable data card (2-4 key rows).
9. If source_figure is present with a url, render it as the dominant evidence image (object-fit: contain) with a short label.
10. When source evidence exists, give it a distinct panel above narration in a lower safe area.

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


def _load_motion_example_markup() -> str:
    path = _motion_snippet_path()
    if not path.exists():
        return ""
    lines = path.read_text(encoding="utf-8").splitlines()
    start = next((i for i, line in enumerate(lines) if "Example scene markup" in line), None)
    if start is None:
        return ""
    return "\n".join(lines[start : start + 12])


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


def _build_scenes_payload(output_dir: Path, script_path: Path) -> list[dict[str, Any]]:
    scenes = load_scenes(script_path)
    payload = []
    for idx, scene in enumerate(scenes):
        payload.append(
            {
                "index": idx + 1,
                "narration": scene.text,
                "visual_description": scene.visual_content,
                "source_table": scene.source_table,
                "source_figure": _resolve_source_figure(output_dir, scene.source_figure, idx),
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


def _fallback_sync_script() -> str:
    """Full audio sync with scene switching, controls, and staggered reveals."""
    return f"""
<script data-infodemica-reveals="true">
(function () {{
  var narration = document.getElementById('narration');
  var scenes = Array.prototype.slice.call(document.querySelectorAll('.scene'));
  var sceneDurations = {PLACEHOLDER_SCENE_DURATIONS_JSON};
  var totalDuration = {PLACEHOLDER_TOTAL_DURATION_JSON};
  var lastSceneIndex = -1;
  var playButton = document.querySelector('[data-play-toggle]') || document.querySelector('.play-button') || document.querySelector('button');
  var progressBar = document.querySelector('[data-progress-bar]') || document.querySelector('.progress-fill') || document.querySelector('.progress-bar');
  var timeEl = document.querySelector('[data-time]');

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

  function format(seconds) {{
    seconds = Math.max(0, Math.floor(seconds || 0));
    return Math.floor(seconds / 60) + ':' + String(seconds % 60).padStart(2, '0');
  }}

  function setActiveScene(index) {{
    scenes.forEach(function (scene, i) {{
      scene.classList.toggle('active', i === index);
    }});
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
      setActiveScene(idx);
    }}
    updateReveals(idx, current);
    if (progressBar && totalDuration > 0) {{
      progressBar.style.width = Math.min(100, (current / totalDuration) * 100) + '%';
    }}
    if (timeEl) timeEl.textContent = format(current);
  }}

  if (narration) {{
    narration.addEventListener('timeupdate', update);
    narration.addEventListener('seeked', update);
    narration.addEventListener('ended', update);
  }}
  if (playButton && narration) {{
    playButton.addEventListener('click', function () {{
      if (narration.paused) {{ narration.play(); playButton.textContent = 'Ⅱ'; }}
      else {{ narration.pause(); playButton.textContent = '▶'; }}
    }});
  }}
  setActiveScene(0);
  update();
}}());
</script>
"""


def _ensure_motion(html: str) -> str:
    """Inject shared motion CSS and audio-synced reveal handling."""
    if "data-infodemica-motion" not in html:
        css = _load_motion_css()
        if css and "</head>" in html:
            motion_style = f'<style data-infodemica-motion="true">\n{css}\n</style>'
            html = html.replace("</head>", motion_style + "\n</head>", 1)

    if "data-infodemica-reveals" not in html and "</body>" in html:
        html = html.replace("</body>", _reveal_sync_script() + "</body>", 1)

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

    if (
        PLACEHOLDER_SCENE_DURATIONS_JSON not in html
        or PLACEHOLDER_TOTAL_DURATION_JSON not in html
    ):
        sync_script = _fallback_sync_script()
        if "</body>" in html:
            html = html.replace("</body>", sync_script + "</body>", 1)
        else:
            html += sync_script

    html = _ensure_motion(html)
    html = _ensure_visible_code_cleanup(html)
    return html


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
    scenes_data = _build_scenes_payload(output_dir, script_path)

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
