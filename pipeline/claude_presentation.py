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

PROMPT_TEMPLATE = """You are generating a complete, self-contained HTML/CSS/JavaScript vertical video experience for a scientific paper.

Paper title:
{title}

Scenes, one scene per segment:
{scenes_json}

Write a concise HTML document that feels like a polished short-form explainer video, not a slide deck. It should resemble an animated social-media research video built from HTML.

Requirements:
1. Output one complete HTML document only. Include <!DOCTYPE html>, <html>, <head>, <style>, <body>, and <script>. Do not use markdown fences.
2. Use a 9:16 vertical stage, centered on the page, with cinematic motion-graphics styling.
3. Include exactly one `.scene` element per scene. Each scene must have `data-scene-index="N"` where N is 1-based. The first scene must also have class `active`.
4. Use reusable CSS classes and CSS variables for scene variation. Do not write long bespoke CSS blocks for every scene. Use CSS transitions/animations so scene changes feel like a video: fades, scale, moving abstract shapes, timeline progress, kinetic type. Avoid external assets, external fonts, network calls, or libraries.
5. Use the narration text as the main readable on-screen text. Keep text large, uncluttered, and mobile-friendly.
6. Translate each visual description into HTML/CSS visuals: abstract shapes, charts, molecule-like diagrams, data cards, icons made from CSS/text, gradients, grids, and motion. Do not display the visual description itself as visible text. Use emoji only sparingly, never as the whole visual design.
6a. If a scene includes `source_table`, render a compact, readable table/data-card in that scene using the provided label, caption, and text. Emphasize 2-4 key rows or values rather than crowding the full source table.
6b. If a scene includes `source_figure`, render it as the main evidence image for that scene when `url` is present. Use object-fit: contain and include a short source label/caption, but do not show long captions over the narration.
6c. When rendering a source table or source figure, reserve a distinct evidence panel above the narration and keep narration in a lower safe area. Do not absolutely position source evidence over narration or generated imagery. If source evidence is selected, let it replace the generic generated visual for that scene.
7. Include an audio element with id `narration` and src exactly `__AUDIO_SRC__`. Hide the default audio element.
8. Include a play/pause button, current scene indicator, and progress bar.
9. In the script, include these exact lines on separate lines:
   var sceneDurations = __SCENE_DURATIONS_JSON__;
   var totalDuration = __TOTAL_DURATION_JSON__;
10. The script must use audio currentTime to toggle the active scene. Query scenes with `document.querySelectorAll('.scene')`.
11. Do not mention Claude, Anthropic, Gemini, Runway, or implementation details in the visible presentation.
12. Keep all code self-contained, robust, and compact enough to complete in one response. No inline event handlers; use addEventListener.

Output only raw HTML."""


def _load_paper(paper_path: Path | None) -> dict[str, Any]:
    if not paper_path or not paper_path.exists():
        return {"title": "Research explainer"}
    try:
        with open(paper_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return {"title": "Research explainer"}
    return {"title": data.get("title") or "Research explainer"}


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
        sync_script = f"""
<script>
(function () {{
  var narration = document.getElementById('narration');
  var scenes = Array.prototype.slice.call(document.querySelectorAll('.scene'));
  var sceneDurations = {PLACEHOLDER_SCENE_DURATIONS_JSON};
  var totalDuration = {PLACEHOLDER_TOTAL_DURATION_JSON};
  var playButton = document.querySelector('[data-play-toggle]') || document.querySelector('.play-button') || document.querySelector('button');
  var progressBar = document.querySelector('[data-progress-bar]') || document.querySelector('.progress-fill') || document.querySelector('.progress-bar');

  function sceneIndexForTime(time) {{
    var elapsed = 0;
    for (var i = 0; i < sceneDurations.length; i += 1) {{
      elapsed += Number(sceneDurations[i]) || 0;
      if (time <= elapsed) return i;
    }}
    return Math.max(0, scenes.length - 1);
  }}

  function setActiveScene(index) {{
    scenes.forEach(function (scene, i) {{
      scene.classList.toggle('active', i === index);
    }});
  }}

  function update() {{
    if (!narration || !scenes.length) return;
    setActiveScene(sceneIndexForTime(narration.currentTime || 0));
    if (progressBar && totalDuration > 0) {{
      progressBar.style.width = Math.min(100, ((narration.currentTime || 0) / totalDuration) * 100) + '%';
    }}
  }}

  if (narration) {{
    narration.addEventListener('timeupdate', update);
    narration.addEventListener('ended', update);
  }}
  if (playButton && narration) {{
    playButton.addEventListener('click', function () {{
      if (narration.paused) narration.play(); else narration.pause();
    }});
  }}
  setActiveScene(0);
}}());
</script>
"""
        if "</body>" in html:
            html = html.replace("</body>", sync_script + "</body>", 1)
        else:
            html += sync_script

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
) -> str:
    """Call Claude to generate a self-contained HTML video presentation."""
    if api_key is None:
        api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY environment variable not set")

    scenes = load_scenes(script_path)
    if not scenes:
        raise ValueError("script.json has no scenes")

    paper = _load_paper(paper_path)
    scenes_data = [
        {
            "index": idx + 1,
            "narration": scene.text,
            "visual_description": scene.visual_content,
            "source_table": scene.source_table,
            "source_figure": scene.source_figure,
        }
        for idx, scene in enumerate(scenes)
    ]

    prompt = PROMPT_TEMPLATE.format(
        title=paper["title"],
        scenes_json=json.dumps(scenes_data, indent=2, ensure_ascii=False),
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
        logger.warning("Claude returned incomplete HTML; retrying with stricter compact prompt")
        repair_prompt = (
            "Your previous HTML was incomplete. Return a COMPLETE raw HTML document now. "
            f"It must include exactly {len(scenes)} elements with class \"scene\", one per scene, "
            "plus </body> and </html>. Use compact reusable CSS. Do not omit any scene. "
            "Use the exact audio/sync placeholders from the original instructions.\n\n"
            + prompt
        )
        response = client.messages.create(
            model=model,
            max_tokens=max_tokens,
            messages=[{"role": "user", "content": repair_prompt}],
        )
        html = _ensure_contract(_strip_code_fence(response.content[0].text))
    return html
