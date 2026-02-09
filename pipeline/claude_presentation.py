"""Generate presentation HTML from script and paper using Claude (Haiku)."""

import json
import logging
import os
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)

# Placeholder contract: Python replaces these in the generated HTML for sync/title/audio.
# Do not change these strings without updating presentation.py substitution.
PLACEHOLDER_TITLE = "__PRESENTATION_TITLE__"
PLACEHOLDER_AUDIO_SRC = "__AUDIO_SRC__"
PLACEHOLDER_SCENE_DURATIONS_JSON = "__SCENE_DURATIONS_JSON__"
PLACEHOLDER_TOTAL_DURATION_JSON = "__TOTAL_DURATION_JSON__"

PROMPT_TEMPLATE = """You are generating a complete, self-contained HTML presentation that will be synced to audio. The presentation is about a research paper.

Paper title: {title}

Scenes (each scene = one slide; order matters):
{scenes_json}

Optional figures from the paper (id, caption only; do not embed images by URL):
{figures_json}

IMPORTANT: Ignore all figures completely. Do not reference them, include them, or create any visual representations based on them. Focus only on the scenes provided.

Output a COMPLETE HTML document. Requirements:

1. Structure: Include <!DOCTYPE html>, <html>, <head>, and <body>. In <head> include <title> and a <style> block with all CSS. In <body> include the slide container, audio element, progress bar, play button, and a <script> block that implements audio-synced slide advancement.

2. Placeholders (use these EXACT strings; they will be replaced by the system):
   - In <title>: __PRESENTATION_TITLE__
   - In the <audio> element: use src="__AUDIO_SRC__" (and give the audio element id="narration")
   - In the sync script, on two separate lines use exactly:
     var sceneDurations = __SCENE_DURATIONS_JSON__;
     var totalDuration = __TOTAL_DURATION_JSON__;

3. Slides (sync contract):
   - One slide per scene. Each slide must be: <div class="scene" data-scene-index="N">...</div> with N 1-based (1, 2, 3, ...).
   - The first slide (data-scene-index="1") must also have class "active" so it is visible initially.
   - Use semantic classes where helpful: "scene-hook", "scene-content", "scene-cta".
   - Inside each slide, present the scene text clearly. If a scene has "key_stat", show it prominently. If "bullets", show as a list.
   - For visual content descriptions: Use the most suitable emoji that represents the visual content. Place the emoji prominently in the slide (e.g., as a large decorative element or alongside the text).
   - Ensure the number of slide divs equals the number of scenes ({num_scenes}).

4. Sync script behavior: The script must drive slide changes from the audio. Use sceneDurations (array of per-scene durations in seconds) and totalDuration (total seconds). As playback time advances, toggle the "active" class on the slide whose cumulative duration contains the current time. Include play/pause button and a progress bar that reflects currentTime/totalDuration. Query slides with document.querySelectorAll('.scene').

5. Styling: Modern, readable slide deck. Use a single container (e.g. .video-container) for the slides. Hide the audio element (e.g. display: none). Style progress bar and play button appropriately.

6. Visual content handling: When a scene includes "visual_content", represent it using the most suitable emoji. Choose an emoji that best matches the visual description (e.g., 🧬 for biology, 📊 for data/charts, 🔬 for experiments, 🏥 for medical, etc.). Display the emoji prominently in the slide.

7. Figures: Completely ignore all figures provided. Do not reference, include, or create any content based on the figures list.

Output ONLY the raw HTML document, no markdown code fence or explanation."""


def _load_paper(paper_path: Optional[Path]) -> dict[str, Any]:
    """Load paper.json; return dict with title and figures, or empty defaults."""
    if not paper_path or not paper_path.exists():
        return {"title": "Presentation", "figures": []}
    with open(paper_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return {
        "title": data.get("title", "Presentation"),
        "figures": [
            {"id": fig.get("id", ""), "caption": fig.get("caption", "")}
            for fig in data.get("figures", [])
        ],
    }


def generate_presentation_html(
    script_path: Path,
    paper_path: Optional[Path] = None,
    api_key: Optional[str] = None,
) -> str:
    """
    Call Claude Haiku to generate a full HTML presentation document from script and paper.

    The returned HTML must contain the exact placeholders PLACEHOLDER_* so that
    presentation.py can replace them with title, audio_src, scene_durations JSON, and total_duration JSON.

    Args:
        script_path: Path to script.json (scenes from Gemini).
        paper_path: Optional path to paper.json (title, figures).
        api_key: Anthropic API key (defaults to ANTHROPIC_API_KEY env).

    Returns:
        Full HTML document string (with placeholders), to be post-processed by presentation.py.
    """
    if api_key is None:
        api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY environment variable not set")

    from scenes import load_scenes

    scenes = load_scenes(script_path)
    if not scenes:
        raise ValueError("Script has no scenes")

    paper = _load_paper(paper_path)
    title = paper["title"]
    figures = paper["figures"]

    # Build scene list for the prompt (serializable dicts).
    scenes_data = []
    for i, s in enumerate(scenes):
        d = {
            "index": i + 1,
            "text": s.text,
            "visual_content": s.visual_content,
            "key_stat": s.key_stat,
            "bullets": s.bullets,
        }
        scenes_data.append(d)
    scenes_json = json.dumps(scenes_data, indent=2, ensure_ascii=False)
    figures_json = json.dumps(figures, indent=2, ensure_ascii=False)
    num_scenes = len(scenes)

    prompt = PROMPT_TEMPLATE.format(
        title=title,
        scenes_json=scenes_json,
        figures_json=figures_json,
        num_scenes=num_scenes,
    )

    try:
        from anthropic import Anthropic
    except ImportError:
        raise ImportError(
            "anthropic is required for Claude presentation. Install with: pip install anthropic"
        ) from None

    client = Anthropic(api_key=api_key)
    logger.info("Calling Claude Haiku to generate full presentation HTML")
    response = client.messages.create(
        model="claude-3-5-haiku-20241022",
        max_tokens=8192,
        messages=[{"role": "user", "content": prompt}],
    )
    text = response.content[0].text
    # Strip markdown code fence if present
    if text.strip().startswith("```"):
        lines = text.strip().split("\n")
        if lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines)
    return text.strip()
