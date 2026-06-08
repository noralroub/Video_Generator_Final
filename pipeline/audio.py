"""Audio generation module for paper video narration."""

import json
import logging
import os
import threading
import time
import wave
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import List

from google import genai
from google.genai import types

from scenes import Scene, load_scenes

logger = logging.getLogger(__name__)

# TTS configuration
TTS_MODEL = "gemini-2.5-flash-preview-tts"
SAMPLE_RATE = 24000  # Hz
BYTES_PER_SAMPLE = 2  # 16-bit audio
MAX_RETRIES = 1
TTS_RPM_LIMIT = 5  # TTS API rate limit: 5 requests per minute
TTS_RATE_LIMIT_WINDOW = 60.0  # seconds

# Rate limiter state
_rate_limiter_lock = threading.Lock()
_request_timestamps = []


def _wait_for_rate_limit():
    """
    Enforce rate limiting for TTS API (5 requests per minute).

    This function blocks until it's safe to make another request,
    ensuring we don't exceed the API rate limit.
    """
    global _request_timestamps

    with _rate_limiter_lock:
        current_time = time.time()

        # Remove timestamps older than the window
        _request_timestamps = [
            ts
            for ts in _request_timestamps
            if current_time - ts < TTS_RATE_LIMIT_WINDOW
        ]

        # If we've hit the limit, wait until the oldest request expires
        if len(_request_timestamps) >= TTS_RPM_LIMIT:
            oldest_request = _request_timestamps[0]
            wait_time = TTS_RATE_LIMIT_WINDOW - (current_time - oldest_request)

            if wait_time > 0:
                logger.info(
                    f"TTS rate limit reached ({TTS_RPM_LIMIT} requests per minute). "
                    f"Waiting {wait_time:.1f}s..."
                )
                time.sleep(wait_time)

                # Recalculate current time after sleeping
                current_time = time.time()

                # Clean up again after waiting
                _request_timestamps = [
                    ts
                    for ts in _request_timestamps
                    if current_time - ts < TTS_RATE_LIMIT_WINDOW
                ]

        # Record this request
        _request_timestamps.append(current_time)
        logger.debug(
            f"TTS rate limit state: {len(_request_timestamps)}/{TTS_RPM_LIMIT} "
            f"requests in last {TTS_RATE_LIMIT_WINDOW}s"
        )


@dataclass(frozen=True)
class SceneAudio:
    """Audio timing information for a single scene."""

    scene_index: int
    text: str
    visual_type: str  # "generated"
    visual_content: str  # video generation prompt
    start_time: float  # seconds
    end_time: float  # seconds
    duration: float  # seconds
    clip_duration: float  # same as duration, for clarity in output


@dataclass(frozen=True)
class AudioResult:
    """Complete audio generation result."""

    full_audio_path: str  # WAV file path with continuous audio
    scene_boundaries: List[SceneAudio]  # Timing for each scene
    total_duration: float  # Total audio duration in seconds
    voice: str  # Voice used for generation


def _ensure_punctuation(text: str) -> str:
    """Ensure text ends with proper punctuation."""
    text = text.strip()
    if not text:
        return text

    # If already ends with punctuation, return as-is
    if text[-1] in ".!?":
        return text

    # Add period by default
    return text + "."


def _calculate_duration(audio_data: bytes) -> float:
    """Calculate duration of PCM audio data in seconds."""
    num_samples = len(audio_data) // BYTES_PER_SAMPLE
    return num_samples / SAMPLE_RATE


def _generate_tts(
    client: genai.Client, text: str, voice: str, retry_count: int = 0
) -> bytes:
    """
    Generate TTS audio for given text.

    Args:
        client: Gemini API client
        text: Text to convert to speech
        voice: Voice name to use
        retry_count: Current retry attempt

    Returns:
        Raw PCM audio data (24kHz, 16-bit, mono)

    Raises:
        Exception: If TTS generation fails after retries
    """
    try:
        # Enforce rate limiting before making API call
        _wait_for_rate_limit()

        response = client.models.generate_content(
            model=TTS_MODEL,
            contents=text,
            config=types.GenerateContentConfig(
                response_modalities=["AUDIO"],
                speech_config=types.SpeechConfig(
                    voice_config=types.VoiceConfig(
                        prebuilt_voice_config=types.PrebuiltVoiceConfig(
                            voice_name=voice
                        )
                    )
                ),
            ),
        )

        audio_data = response.candidates[0].content.parts[0].inline_data.data
        return audio_data

    except Exception as e:
        if retry_count < MAX_RETRIES:
            logger.warning(
                f"TTS failed, retrying ({retry_count + 1}/{MAX_RETRIES}): {e}"
            )
            return _generate_tts(client, text, voice, retry_count + 1)
        else:
            logger.error(f"TTS failed after {MAX_RETRIES + 1} attempts: {e}")
            raise


def _save_wav(audio_data: bytes, output_path: Path) -> None:
    """Save PCM audio data as WAV file."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with wave.open(str(output_path), "wb") as wav_file:
        wav_file.setnchannels(1)  # mono
        wav_file.setsampwidth(BYTES_PER_SAMPLE)
        wav_file.setframerate(SAMPLE_RATE)
        wav_file.writeframes(audio_data)

    logger.info(f"Saved audio to {output_path}")


def _estimate_scene_weights(scene_texts: List[str]) -> List[float]:
    """Estimate narration timing weights without extra TTS calls.

    This keeps the single natural full-audio track as the source of truth and
    allocates scene boundaries based on text complexity. It avoids one TTS API
    call per scene, which is important for Gemini TTS free-tier quotas.
    """
    weights = []
    for text in scene_texts:
        words = text.split()
        word_weight = len(words)
        comma_pauses = text.count(",") * 0.35
        sentence_pauses = sum(text.count(mark) for mark in ".!?") * 0.75
        long_word_weight = sum(0.15 for word in words if len(word) > 9)
        weights.append(
            max(1.0, word_weight + comma_pauses + sentence_pauses + long_word_weight)
        )
    return weights


def generate_audio(
    scenes: List[Scene],
    output_dir: Path,
    voice: str = "Kore",
    api_key: str | None = None,
) -> AudioResult:
    """
    Generate audio narration for scenes with boundary timing.

    This function implements a quota-efficient proportional splitting approach:
    1. Generate full continuous TTS (natural flow - this is the keeper)
    2. Estimate each scene's timing weight from narration text
    3. Apply proportions to split full audio at scene boundaries

    Args:
        scenes: List of Scene objects to narrate
        output_dir: Directory to save audio files
        voice: Voice name for TTS (default: 'Kore')
        api_key: Gemini API key (defaults to GEMINI_API_KEY env var)

    Returns:
        AudioResult with full audio path and scene boundary timings

    Raises:
        ValueError: If API key is missing or scenes list is empty
        Exception: If TTS generation fails
    """
    if api_key is None:
        api_key = os.getenv("GEMINI_API_KEY")

    if not api_key:
        raise ValueError("GEMINI_API_KEY environment variable not set")

    if not scenes:
        raise ValueError("scenes list cannot be empty")

    output_dir = Path(output_dir)
    client = genai.Client(api_key=api_key)

    # Ensure all scene texts have punctuation
    scene_texts = [_ensure_punctuation(scene.text) for scene in scenes]

    logger.info(f"Generating audio for {len(scenes)} scenes with voice '{voice}'")

    # Step 1: Generate full continuous audio
    logger.info("Generating full continuous audio...")
    full_text = " ".join(scene_texts)
    full_audio_data = _generate_tts(client, full_text, voice)
    full_duration = _calculate_duration(full_audio_data)
    logger.info(f"Full audio duration: {full_duration:.2f}s")

    # Step 2: Estimate scene timing proportions without extra TTS calls.
    logger.info("Estimating scene boundaries from narration text (single TTS call mode)")
    timing_weights = _estimate_scene_weights(scene_texts)
    total_weight = sum(timing_weights)
    proportions = [weight / total_weight for weight in timing_weights]

    # Create boundaries: [0, prop1*full_dur, (prop1+prop2)*full_dur, ..., full_dur]
    boundaries = [0.0]
    cumulative = 0.0
    for prop in proportions:
        cumulative += prop
        boundaries.append(cumulative * full_duration)

    # Ensure last boundary is exactly full_duration (avoid floating point errors)
    boundaries[-1] = full_duration

    # Step 3: Create SceneAudio objects with boundary info
    scene_boundaries = []
    for i, scene in enumerate(scenes):
        duration = boundaries[i + 1] - boundaries[i]
        scene_audio = SceneAudio(
            scene_index=i,
            text=scene_texts[i],
            visual_type=scene.visual_type,
            visual_content=scene.visual_content,
            start_time=boundaries[i],
            end_time=boundaries[i + 1],
            duration=duration,
            clip_duration=duration,
        )
        scene_boundaries.append(scene_audio)
        logger.info(
            f"Scene {i}: {scene_audio.start_time:.2f}s - {scene_audio.end_time:.2f}s "
            f"({scene_audio.duration:.2f}s) - {scene.visual_type}"
        )

    # Step 4: Save full audio as WAV
    audio_path = output_dir / "audio.wav"
    _save_wav(full_audio_data, audio_path)

    return AudioResult(
        full_audio_path=str(audio_path),
        scene_boundaries=scene_boundaries,
        total_duration=full_duration,
        voice=voice,
    )


def save_audio_metadata(result: AudioResult, output_path: Path) -> None:
    """
    Save audio metadata to JSON file.

    Args:
        result: AudioResult object
        output_path: Path to output JSON file

    Raises:
        IOError: If file cannot be written
    """
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Convert to dict with scene boundaries as list of dicts
        metadata = {
            "full_audio_path": result.full_audio_path,
            "total_duration": result.total_duration,
            "voice": result.voice,
            "scene_boundaries": [asdict(sb) for sb in result.scene_boundaries],
        }

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)

        logger.info(f"Saved audio metadata to {output_path}")

    except Exception as e:
        logger.error(f"Failed to save audio metadata: {e}")
        raise IOError(f"Could not write metadata to {output_path}: {e}")


def load_audio_metadata(metadata_path: Path) -> AudioResult:
    """
    Load audio metadata from JSON file.

    Args:
        metadata_path: Path to metadata JSON file

    Returns:
        AudioResult object

    Raises:
        FileNotFoundError: If file doesn't exist
        ValueError: If JSON is invalid
    """
    if not metadata_path.exists():
        raise FileNotFoundError(f"Metadata file not found: {metadata_path}")

    try:
        with open(metadata_path, "r", encoding="utf-8") as f:
            metadata = json.load(f)

        # Reconstruct SceneAudio objects
        scene_boundaries = [SceneAudio(**sb) for sb in metadata["scene_boundaries"]]

        result = AudioResult(
            full_audio_path=metadata["full_audio_path"],
            scene_boundaries=scene_boundaries,
            total_duration=metadata["total_duration"],
            voice=metadata["voice"],
        )

        logger.info(f"Loaded audio metadata from {metadata_path}")
        return result

    except (json.JSONDecodeError, KeyError, TypeError) as e:
        logger.error(f"Invalid metadata in {metadata_path}: {e}")
        raise ValueError(f"Could not parse metadata file: {e}")
