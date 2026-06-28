"""Server-side MP4 export — records presentation.html with headless Chromium."""
from __future__ import annotations

import json
import logging
import re
import shutil
import subprocess
import threading
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

logger = logging.getLogger(__name__)

WIDTH, HEIGHT = 1080, 1920
EXPORT_STYLE = f"""
#stage, .stage {{
    width: {WIDTH}px !important;
    height: {HEIGHT}px !important;
    max-width: none !important;
    max-height: none !important;
    border-radius: 0 !important;
    box-shadow: none !important;
}}
body {{
    margin: 0 !important;
    min-height: {HEIGHT}px !important;
    background: #050810 !important;
    overflow: hidden !important;
}}
#controls, .controls {{ display: none !important; }}
"""


def _load_total_duration(output_dir: Path) -> float:
    metadata_path = output_dir / "audio_metadata.json"
    if metadata_path.exists():
        with open(metadata_path, "r", encoding="utf-8") as handle:
            metadata = json.load(handle)
        boundaries = metadata.get("scene_boundaries", [])
        if boundaries:
            return sum(
                float(item.get("duration", item.get("clip_duration", 6.0)))
                for item in boundaries
            )

    presentation_path = output_dir / "presentation.html"
    if presentation_path.exists():
        html = presentation_path.read_text(encoding="utf-8")
        match = re.search(r"var totalDuration = (\d+(?:\.\d+)?);", html)
        if match:
            return float(match.group(1))

    audio_path = output_dir / "audio.wav"
    if audio_path.exists():
        try:
            from pydub import AudioSegment

            return len(AudioSegment.from_wav(str(audio_path))) / 1000.0
        except Exception:
            pass

    return 60.0


def _start_static_server(directory: Path) -> tuple[ThreadingHTTPServer, int]:
    class QuietHandler(SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=str(directory), **kwargs)

        def log_message(self, format, *args):
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), QuietHandler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, port


def _record_presentation_html(output_dir: Path, export_dir: Path) -> Path:
    presentation_path = output_dir / "presentation.html"
    if not presentation_path.exists():
        raise FileNotFoundError("presentation.html not found")

    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise RuntimeError(
            "Playwright is required for MP4 export. "
            "Install dependencies and run: playwright install chromium"
        ) from exc

    if not shutil.which("ffmpeg"):
        raise RuntimeError("ffmpeg is required for MP4 export and was not found on PATH.")

    export_dir.mkdir(parents=True, exist_ok=True)
    total_duration = _load_total_duration(output_dir)
    wait_ms = int((total_duration + 2.0) * 1000)

    server, port = _start_static_server(output_dir)
    url = f"http://127.0.0.1:{port}/presentation.html"
    saved_path: Path | None = None

    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            context = browser.new_context(
                viewport={"width": WIDTH, "height": HEIGHT},
                record_video_dir=str(export_dir),
                record_video_size={"width": WIDTH, "height": HEIGHT},
            )
            page = context.new_page()
            page.goto(url, wait_until="load", timeout=60_000)
            page.add_style_tag(content=EXPORT_STYLE)
            page.wait_for_timeout(500)

            page.evaluate(
                """async () => {
                    const audio = document.getElementById('narration');
                    if (!audio) return;
                    audio.currentTime = 0;
                    try { await audio.play(); } catch (err) {}
                }"""
            )

            try:
                page.wait_for_function(
                    "() => { const audio = document.getElementById('narration'); return audio && audio.ended; }",
                    timeout=wait_ms + 30_000,
                )
            except Exception:
                logger.warning("Timed out waiting for narration to finish; using fixed duration fallback")
                page.wait_for_timeout(wait_ms)

            page.wait_for_timeout(500)
            video = page.video
            page.close()
            if video:
                saved_path = Path(video.path())
            context.close()
            browser.close()
    finally:
        server.shutdown()

    if not saved_path or not saved_path.exists():
        raise RuntimeError("Playwright did not produce a video recording.")

    capture_path = export_dir / "capture.webm"
    if saved_path != capture_path:
        shutil.move(str(saved_path), str(capture_path))
    return capture_path


def _mux_audio(video_path: Path, audio_path: Path, output_path: Path) -> None:
    cmd = [
        "ffmpeg",
        "-y",
        "-i",
        str(video_path),
        "-i",
        str(audio_path),
        "-c:v",
        "libx264",
        "-pix_fmt",
        "yuv420p",
        "-c:a",
        "aac",
        "-shortest",
        str(output_path),
    ]
    subprocess.run(cmd, check=True, capture_output=True, text=True)


def _transcode_video(video_path: Path, output_path: Path) -> None:
    cmd = [
        "ffmpeg",
        "-y",
        "-i",
        str(video_path),
        "-c:v",
        "libx264",
        "-pix_fmt",
        "yuv420p",
        str(output_path),
    ]
    subprocess.run(cmd, check=True, capture_output=True, text=True)


def export_mp4(output_dir: Path, output_path: Path | None = None) -> Path:
    output_path = output_path or (output_dir / "presentation.mp4")
    export_dir = output_dir / "mp4_export"

    if not (output_dir / "presentation.html").exists():
        raise FileNotFoundError("presentation.html not found — run the pipeline first.")

    raw_video = _record_presentation_html(output_dir, export_dir)
    audio_path = output_dir / "audio.wav"
    if audio_path.exists():
        _mux_audio(raw_video, audio_path, output_path)
    else:
        _transcode_video(raw_video, output_path)
    return output_path
