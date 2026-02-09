"""Scene generation module for paper video summarization."""

import json
import logging
import os
import re
from dataclasses import dataclass, asdict, fields
from pathlib import Path
from typing import List, Literal, Optional

from google import genai
from google.genai import types

logger = logging.getLogger(__name__)

# Maximum length for paper text to avoid context limits
MAX_PAPER_LENGTH = 50000


@dataclass(frozen=True)
class Scene:
    """Represents a single scene in the video summary."""

    text: str
    visual_type: Literal["generated"]
    visual_content: str
    key_stat: Optional[str] = None
    bullets: Optional[List[str]] = None


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
    prompt = f"""You are creating a short social media video script (TikTok/Instagram style) that tells the story of a scientific paper.

Paper Title: {paper_data['title']}

Paper Content:
{full_text}

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

Video generation – visuals that support the story:
- Goal: Each scene's visual should help the viewer understand and relate to what they're hearing and what the paper is about. Prefer visuals that clearly illustrate the topic when that helps comprehension.
- Content: Literal, topic-relevant imagery is fine when it matches the narration: e.g. medical or lab equipment, procedures, organisms, data, environments—as long as they fit the scene and the paper.
- Style: You can use either literal visuals (e.g. "researcher pipetting in a lab", "MRI machine", "bacteria under microscope") or more abstract or metaphorical visuals when that better conveys the idea. Choose whatever best supports understanding.
- AVOID identifiable real people, specific brands, or copyrighted characters so the video stays safe and reusable.

For each scene, write a clear Veo video generation prompt that describes the visual content and supports what the narrator is saying.

Optional animation data (include only when they fit the scene):
- key_stat: If the scene mentions a clear statistic or number (e.g. "3x more", "50%", "12"), add it as a short string for an on-screen counter.
- bullets: If the scene has list-like or multi-point content, add a list of 2-5 short bullet strings for staggered reveal.

Return ONLY a JSON object with this structure:
{{
  "scenes": [
    {{
      "text": "Short, engaging sentence for narration",
      "visual_type": "generated",
      "visual_content": "detailed video generation prompt",
      "key_stat": "optional e.g. 3x or 50% or omit",
      "bullets": ["optional", "list of", "short strings or omit"]
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

                key_stat = scene_data.get("key_stat")
                bullets_raw = scene_data.get("bullets")
                bullets = bullets_raw if isinstance(bullets_raw, list) else None

                scenes.append(
                    Scene(
                        text=scene_data["text"],
                        visual_type=scene_data["visual_type"],
                        visual_content=scene_data["visual_content"],
                        key_stat=key_stat if key_stat else None,
                        bullets=bullets,
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

        known = {f.name for f in fields(Scene)}
        scenes = [
            Scene(**{k: v for k, v in scene_data.items() if k in known})
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
