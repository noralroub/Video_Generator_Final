"""Scene generation module for paper video summarization."""

import json
import logging
import os
import re
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Literal, List

logger = logging.getLogger(__name__)

# Maximum length for paper text to avoid context limits
MAX_PAPER_LENGTH = 50000

VISUAL_LAYOUTS = (
    "headline_only",
    "stat_counter",
    "stat_pair",
    "bar_comparison",
    "mini_table",
    "figure_focus",
)

_NUMBER_PATTERN = re.compile(
    r"\d[\d,]*\.?\d*\s*(?:%|percent|million|billion|thousand|x|fold)?",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class Scene:
    """Represents a single scene in the video summary."""

    text: str
    visual_type: Literal["generated"]
    visual_content: str
    source_table: dict[str, Any] | None = None
    source_figure: dict[str, Any] | None = None
    source_video: dict[str, Any] | None = None
    display_phrases: list[dict[str, Any]] | None = None
    visual_layout: str | None = None
    chart_data: dict[str, Any] | None = None


def _split_phrases(text: str) -> list[dict[str, Any]]:
    """Split narration into 2-4 short phrase chunks."""
    text = (text or "").strip()
    if not text:
        return [{"text": "", "emphasis": []}]

    parts = [part.strip() for part in re.split(r"(?<=[.!?])\s+|,\s+", text) if part.strip()]
    if len(parts) == 1 and len(parts[0].split()) > 8:
        words = parts[0].split()
        mid = max(1, len(words) // 2)
        parts = [" ".join(words[:mid]), " ".join(words[mid:])]
    if len(parts) > 4:
        merged: list[str] = []
        chunk_size = max(1, (len(parts) + 3) // 4)
        for idx in range(0, len(parts), chunk_size):
            merged.append(" ".join(parts[idx : idx + chunk_size]))
        parts = merged[:4]
    return [{"text": part, "emphasis": extract_emphasis_words(part)} for part in parts]


def extract_emphasis_words(text: str, limit: int = 3) -> list[str]:
    """Pick numbers and strong terms to emphasize on screen."""
    emphasis: list[str] = []
    for match in _NUMBER_PATTERN.finditer(text or ""):
        token = match.group(0).strip()
        if token and token not in emphasis:
            emphasis.append(token)
    for word in re.findall(r"\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?\b", text or ""):
        if word not in emphasis:
            emphasis.append(word)
    for word in re.findall(r"\b[a-z]{5,}\b", text or ""):
        if word.lower() in {
            "researchers",
            "patients",
            "study",
            "results",
            "treatment",
            "disease",
            "cancer",
            "virus",
            "risk",
            "reduced",
            "increased",
        } and word not in emphasis:
            emphasis.append(word)
    return emphasis[:limit]


def infer_visual_layout(scene: Scene) -> str:
    """Infer a presentation layout when Gemini did not provide one."""
    if scene.visual_layout in VISUAL_LAYOUTS:
        return scene.visual_layout
    if scene.source_figure:
        return "figure_focus"
    if scene.source_table:
        return "mini_table"
    numbers = _NUMBER_PATTERN.findall(scene.text or "")
    if len(numbers) >= 2:
        return "stat_pair"
    if len(numbers) == 1:
        return "stat_counter"
    if scene.chart_data:
        rows = scene.chart_data.get("rows") or []
        if rows:
            return "bar_comparison"
    return "headline_only"


def infer_chart_data(scene: Scene) -> dict[str, Any] | None:
    """Build simple chart data from narration numbers when missing."""
    if scene.chart_data:
        return scene.chart_data
    numbers = _NUMBER_PATTERN.findall(scene.text or "")
    if scene.visual_layout == "stat_counter" or (not scene.visual_layout and len(numbers) == 1):
        raw = numbers[0].replace(",", "").split()[0]
        try:
            return {"value": float(raw), "label": scene.text[:60]}
        except ValueError:
            return None
    if len(numbers) >= 2:
        values = []
        for token in numbers[:2]:
            raw = token.replace(",", "").split()[0]
            try:
                values.append(float(raw))
            except ValueError:
                continue
        if len(values) >= 2:
            return {"values": values, "labels": ["A", "B"]}
    return None


def enrich_scene(scene: Scene) -> Scene:
    """Fill optional presentation fields when Gemini omitted them."""
    phrases = scene.display_phrases
    if not phrases:
        phrases = _split_phrases(scene.text)
    else:
        phrases = [
            {
                "text": p.get("text", ""),
                "emphasis": p.get("emphasis") or extract_emphasis_words(p.get("text", "")),
            }
            for p in phrases
        ]

    layout = infer_visual_layout(scene)
    chart_data = infer_chart_data(
        Scene(
            text=scene.text,
            visual_type=scene.visual_type,
            visual_content=scene.visual_content,
            source_table=scene.source_table,
            source_figure=scene.source_figure,
            source_video=scene.source_video,
            display_phrases=phrases,
            visual_layout=layout,
            chart_data=scene.chart_data,
        )
    )

    return Scene(
        text=scene.text,
        visual_type=scene.visual_type,
        visual_content=scene.visual_content,
        source_table=scene.source_table,
        source_figure=scene.source_figure,
        source_video=scene.source_video,
        display_phrases=phrases,
        visual_layout=layout,
        chart_data=chart_data,
    )


def compute_phrase_timings(phrases: list[dict[str, Any]], duration_sec: float) -> list[dict[str, Any]]:
    """Assign enter_ms to each phrase proportional to word count within a scene."""
    if not phrases:
        return []

    word_counts = [max(1, len((p.get("text") or "").split())) for p in phrases]
    total_words = sum(word_counts)
    duration_ms = max(0, duration_sec * 1000)
    elapsed_words = 0
    timed: list[dict[str, Any]] = []

    for idx, phrase in enumerate(phrases):
        enter_ms = int((elapsed_words / total_words) * duration_ms) if total_words else 0
        timed.append({**phrase, "enter_ms": enter_ms})
        elapsed_words += word_counts[idx]

    return timed


def wrap_emphasis_html(text: str, emphasis: list[str]) -> str:
    """Wrap emphasis tokens in span.emph for on-screen pop animation."""
    from html import escape

    if not text:
        return ""
    if not emphasis:
        return escape(text)

    result = escape(text)
    for token in sorted(set(emphasis), key=len, reverse=True):
        escaped = escape(token)
        if escaped in result:
            result = result.replace(
                escaped,
                f'<span class="emph">{escaped}</span>',
                1,
            )
    return result


def generate_scenes(paper_data: dict, api_key: str | None = None) -> List[Scene]:
    """
    Generate 4-10 scenes from paper data using Gemini.

    Args:
        paper_data: Dictionary containing paper information with keys:
            - title: Paper title
            - full_text: Full paper text
            - figures: List of figure dicts with id, url, caption
        api_key: Gemini API key (defaults to GEMINI_API_KEY env var)

    Returns:
        List of Scene objects

    Raises:
        ValueError: If API key is missing or paper_data is invalid
        Exception: If Gemini API call fails
    """
    if api_key is None:
        api_key = os.getenv("GEMINI_API_KEY")

    if not api_key:
        raise ValueError("GEMINI_API_KEY environment variable not set")

    if not paper_data.get("title") or not paper_data.get("full_text"):
        raise ValueError("paper_data must contain 'title' and 'full_text' keys")

    try:
        from google import genai
        from google.genai import types
    except ImportError as exc:
        raise ImportError("Install google-genai to generate scenes") from exc

    # Configure Gemini client
    client = genai.Client(api_key=api_key)

    # Truncate paper text if too long
    full_text = paper_data["full_text"]
    if len(full_text) > MAX_PAPER_LENGTH:
        logger.warning(
            f"Paper text exceeds {MAX_PAPER_LENGTH} chars, truncating from {len(full_text)}"
        )
        full_text = full_text[:MAX_PAPER_LENGTH]

    # Construct prompt
    evidence_items = []
    for figure in (paper_data.get("figures") or [])[:6]:
        evidence_items.append(
            {
                "type": "figure",
                "id": figure.get("id") or "figure",
                "caption": (figure.get("caption") or "")[:900],
            }
        )
    for table in (paper_data.get("tables") or [])[:6]:
        evidence_items.append(
            {
                "type": "table",
                "id": table.get("label") or table.get("id") or "table",
                "caption": (table.get("caption") or "")[:700],
                "preview": (table.get("text") or "")[:1000],
            }
        )
    evidence_text = (
        json.dumps(evidence_items, indent=2, ensure_ascii=False)
        if evidence_items
        else "No structured figures or tables were extracted."
    )

    prompt = f"""You are creating a short social media video script (TikTok/Instagram style) that tells the story of a scientific paper.

Paper Title: {paper_data['title']}

Paper Content:
{full_text}

Available source evidence from the paper:
{evidence_text}

Create 4-10 scenes that tell a compelling story following this narrative structure:

1. THE PROBLEM/HOOK (1-2 scenes): Start with why we should care. What's the real-world problem or challenge? Make it relatable and urgent.
   Example: "Tsetse flies are a huge problem in Tanzania, spreading diseases that kill livestock and harm people."

2. THE RESEARCH (2-3 scenes): Introduce the study with credibility. Mention the journal where it was published and/or the research team/institution.
   Example: "A new study in Nature shows that researchers from Yale developed a trap design to reduce fly populations."
   Example: "Professor Sarah Chen and her team at MIT recently investigated whether..."

   IMPORTANT: Almost always mention the journal name and/or lead researchers/institutions to establish credibility.

3. KEY FINDINGS (2-3 scenes): What did they discover? What were the main results?
   Example: "They found that the new traps caught three times more flies than traditional methods."

4. THE IMPACT/WHAT'S NEXT (1-2 scenes): Tie it back to the research. What did these researchers show? What's the significance of their work? What questions remain or what are scientists working on next?
   Example: "What Professor Chen showed here is the first time scientists have seen this mechanism in action."
   Example: "More research is needed - the team is now investigating whether this works in other species."
   Example: "This is a breakthrough, but researchers still need to figure out why it works at higher temperatures."

   IMPORTANT: Ground the ending in the research itself - mention what the researchers demonstrated, what remains unknown, or what future studies will explore.

Guidelines:
- Use short, punchy sentences (social media style)
- Avoid jargon - explain concepts simply
- Make it conversational and engaging (but not overly exclamatory)
- Focus on the human/real-world angle, not just the science
- Vary sentence structure to maintain interest

Video Generation Content Policy:
- AVOID prompts with medical imagery: pills, syringes, needles, medical procedures
- AVOID prompts with identifiable people, brands, or copyrighted material
- Use abstract or metaphorical visuals instead of literal medical equipment
- Focus on emotions, environments, and general human activities
- Example: Instead of "person holding a syringe", use "person looking concerned while making a health decision"

For each scene, describe the on-slide HTML visual layout and data to show (not a video-generation prompt).
If a specific table or figure from the source evidence would make a scene clearer, attach it via source_table or source_figure. Only use tables/figures when they support the actual narration.

For each scene also provide:
- display_phrases: split narration into 2-4 short on-screen phrases. Each phrase has "text" and "emphasis" (1-3 key words: numbers, percents, important terms).
- visual_layout: one of headline_only, stat_counter, stat_pair, bar_comparison, mini_table, figure_focus.
- chart_data: optional structured numbers when visual_layout needs them (e.g. {{"value": 1155735}} for stat_counter, {{"values": [27, 15], "labels": ["A", "B"]}} for stat_pair, {{"rows": [{{"label": "2010", "values": [48, 44]}}]}} for bar_comparison).

Return ONLY a JSON object with this structure:
{{
  "scenes": [
    {{
      "text": "Short, engaging sentence for narration",
      "visual_type": "generated",
      "visual_content": "Brief description of on-slide visual layout and motifs",
      "display_phrases": [
        {{"text": "They found", "emphasis": []}},
        {{"text": "statins reduced heart attacks by 27 percent", "emphasis": ["statins", "27 percent"]}}
      ],
      "visual_layout": "stat_counter",
      "chart_data": {{"value": 27, "label": "reduction"}}
    }}
  ]
}}"""

    # Generate scenes using Gemini with retry logic
    logger.info("Calling Gemini API to generate scenes")
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            response = client.models.generate_content(
                model="gemini-2.5-flash",
                contents=prompt,
                config=types.GenerateContentConfig(response_mime_type="application/json"),
            )

            # Try to parse response with error recovery
            response_text = response.text.strip()
            
            # Try to extract JSON if it's wrapped in markdown code blocks
            if response_text.startswith("```"):
                # Remove markdown code blocks
                lines = response_text.split("\n")
                if lines[0].startswith("```"):
                    lines = lines[1:]
                if lines and lines[-1].strip() == "```":
                    lines = lines[:-1]
                response_text = "\n".join(lines)
            
            # Try to parse JSON
            try:
                response_data = json.loads(response_text)
            except json.JSONDecodeError as json_err:
                # Try to fix common JSON issues
                logger.warning(f"Initial JSON parse failed: {json_err}. Attempting to fix...")
                
                # Try to fix missing commas between objects in arrays
                # Fix missing comma before closing brace in arrays: }] -> },]
                fixed_text = re.sub(r'}\s*\]', r'},]', response_text)
                # Fix missing comma after closing brace: }" -> }," (but not at end)
                fixed_text = re.sub(r'}\s*"', r'},"', fixed_text)
                # Fix missing comma between array elements: }" -> }," (in middle of array)
                fixed_text = re.sub(r'}\s*\n\s*"', r'},\n"', fixed_text)
                
                try:
                    response_data = json.loads(fixed_text)
                    logger.info("Successfully fixed JSON parsing issue")
                except json.JSONDecodeError:
                    # If still failing, try to extract JSON from the response
                    # Look for JSON object boundaries
                    start_idx = response_text.find("{")
                    end_idx = response_text.rfind("}")
                    if start_idx != -1 and end_idx != -1 and end_idx > start_idx:
                        json_candidate = response_text[start_idx:end_idx + 1]
                        try:
                            response_data = json.loads(json_candidate)
                            logger.info("Successfully extracted JSON from response")
                        except json.JSONDecodeError:
                            if retry_count < max_retries - 1:
                                logger.warning(f"JSON parsing failed, retrying ({retry_count + 1}/{max_retries})...")
                                retry_count += 1
                                continue
                            else:
                                logger.error(f"Failed to parse JSON after {max_retries} attempts. Response text: {response_text[:500]}")
                                raise Exception(f"Invalid JSON response from Gemini after {max_retries} attempts: {json_err}")
                    else:
                        if retry_count < max_retries - 1:
                            logger.warning(f"Could not find JSON in response, retrying ({retry_count + 1}/{max_retries})...")
                            retry_count += 1
                            continue
                        else:
                            logger.error(f"Could not extract JSON from response. Response text: {response_text[:500]}")
                            raise Exception(f"Invalid JSON response from Gemini: Could not extract valid JSON. Error: {json_err}")
            
            scenes_data = response_data.get("scenes", [])

            if not scenes_data:
                if retry_count < max_retries - 1:
                    logger.warning(f"Gemini returned no scenes, retrying ({retry_count + 1}/{max_retries})...")
                    retry_count += 1
                    continue
                else:
                    raise ValueError("Gemini returned no scenes after all retries")

            logger.info(f"Generated {len(scenes_data)} scenes")

            # Validate and create Scene objects
            scenes = []
            for scene_data in scenes_data:
                if not all(
                    k in scene_data for k in ["text", "visual_type", "visual_content"]
                ):
                    logger.warning(f"Skipping invalid scene: {scene_data}")
                    continue

                if scene_data["visual_type"] != "generated":
                    logger.warning(
                        f"Invalid visual_type '{scene_data['visual_type']}', defaulting to 'generated'"
                    )
                    scene_data["visual_type"] = "generated"

                scenes.append(
                    enrich_scene(
                        Scene(
                            text=scene_data["text"],
                            visual_type=scene_data["visual_type"],
                            visual_content=scene_data["visual_content"],
                            source_table=scene_data.get("source_table"),
                            source_figure=scene_data.get("source_figure"),
                            source_video=scene_data.get("source_video"),
                            display_phrases=scene_data.get("display_phrases"),
                            visual_layout=scene_data.get("visual_layout"),
                            chart_data=scene_data.get("chart_data"),
                        )
                    )
                )

            if not scenes:
                if retry_count < max_retries - 1:
                    logger.warning(f"No valid scenes generated, retrying ({retry_count + 1}/{max_retries})...")
                    retry_count += 1
                    continue
                else:
                    raise ValueError("No valid scenes generated after all retries")

            return scenes

        except json.JSONDecodeError as e:
            if retry_count < max_retries - 1:
                logger.warning(f"JSON decode error, retrying ({retry_count + 1}/{max_retries}): {e}")
                retry_count += 1
                continue
            else:
                logger.error(f"Failed to parse Gemini response as JSON after {max_retries} attempts: {e}")
                raise Exception(f"Invalid JSON response from Gemini after {max_retries} attempts: {e}")
        except Exception as e:
            if retry_count < max_retries - 1 and "Invalid JSON" not in str(e):
                logger.warning(f"Error generating scenes, retrying ({retry_count + 1}/{max_retries}): {e}")
                retry_count += 1
                continue
            else:
                logger.error(f"Error generating scenes: {e}")
                raise
    
    # Should not reach here, but just in case
    raise Exception(f"Failed to generate scenes after {max_retries} attempts")


def save_scenes(scenes: List[Scene], output_path: Path) -> None:
    """
    Save scenes to JSON file.

    Args:
        scenes: List of Scene objects
        output_path: Path to output JSON file

    Raises:
        IOError: If file cannot be written
    """
    try:
        scenes_data = [asdict(scene) for scene in scenes]
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(scenes_data, f, indent=2, ensure_ascii=False)

        logger.info(f"Saved {len(scenes)} scenes to {output_path}")

    except Exception as e:
        logger.error(f"Failed to save scenes: {e}")
        raise IOError(f"Could not write scenes to {output_path}: {e}")


def load_scenes(input_path: Path) -> List[Scene]:
    """
    Load scenes from JSON file.

    Args:
        input_path: Path to input JSON file

    Returns:
        List of Scene objects

    Raises:
        FileNotFoundError: If file doesn't exist
        ValueError: If JSON is invalid
    """
    if not input_path.exists():
        raise FileNotFoundError(f"Scene file not found: {input_path}")

    try:
        with open(input_path, "r", encoding="utf-8") as f:
            scenes_data = json.load(f)

        scenes = [
            enrich_scene(
                Scene(
                    text=scene_data.get("text", ""),
                    visual_type=scene_data.get("visual_type", "generated"),
                    visual_content=scene_data.get("visual_content", ""),
                    source_table=scene_data.get("source_table"),
                    source_figure=scene_data.get("source_figure"),
                    source_video=scene_data.get("source_video"),
                    display_phrases=scene_data.get("display_phrases"),
                    visual_layout=scene_data.get("visual_layout"),
                    chart_data=scene_data.get("chart_data"),
                )
            )
            for scene_data in scenes_data
        ]
        logger.info(f"Loaded {len(scenes)} scenes from {input_path}")

        return scenes

    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in {input_path}: {e}")
        raise ValueError(f"Could not parse scene file: {e}")
    except TypeError as e:
        logger.error(f"Invalid scene data structure: {e}")
        raise ValueError(f"Scene data missing required fields: {e}")
