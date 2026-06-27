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


def _export_mp4_pillow(output_dir: Path, output_path: Path) -> Path:
    """Legacy fallback when presentation.html is unavailable."""
    try:
        import PIL  # noqa: F401
    except ImportError as exc:
        raise RuntimeError("Pillow is required for MP4 export fallback. Install dependencies from requirements.txt.") from exc

    from frames import Frame, load_frames

    def _font(size: int, bold: bool = False):
        from PIL import ImageFont

        candidates = [
            "/System/Library/Fonts/Supplemental/Arial Bold.ttf" if bold else "/System/Library/Fonts/Supplemental/Arial.ttf",
            "/Library/Fonts/Arial.ttf",
        ]
        for candidate in candidates:
            try:
                return ImageFont.truetype(candidate, size)
            except Exception:
                pass
        return ImageFont.load_default()

    def _wrap(draw, text: str, font, max_width: int) -> list[str]:
        lines = []
        for paragraph in (text or "").splitlines() or [""]:
            current = ""
            for word in paragraph.split():
                test = f"{current} {word}".strip()
                if draw.textbbox((0, 0), test, font=font)[2] <= max_width:
                    current = test
                else:
                    if current:
                        lines.append(current)
                    current = word
            if current:
                lines.append(current)
        return lines

    def _draw_wrapped(draw, text, xy, font, fill, max_width, line_gap=10, max_lines=None):
        x, y = xy
        lines = _wrap(draw, text, font, max_width)
        if max_lines:
            lines = lines[:max_lines]
        for line in lines:
            draw.text((x, y), line, font=font, fill=fill)
            y += getattr(font, "size", 36) + line_gap
        return y

    def _render_png(frame: Frame, png_path: Path) -> None:
        from PIL import Image, ImageDraw

        dark = frame.theme != "light"
        bg = (5, 5, 5) if dark else (248, 250, 252)
        fg = (255, 255, 255) if dark else (17, 24, 39)
        muted = (190, 198, 210) if dark else (82, 82, 82)
        image = Image.new("RGB", (WIDTH, HEIGHT), bg)
        draw = ImageDraw.Draw(image)
        for i in range(0, HEIGHT, 72):
            draw.line((0, i, WIDTH, i), fill=(24, 24, 24) if dark else (232, 232, 232), width=1)
        draw.rounded_rectangle((72, 82, 470, 138), radius=28, outline=muted, width=2)
        draw.text((104, 100), frame.layout.replace("_", " ").upper(), font=_font(28, True), fill=muted)
        y = _draw_wrapped(draw, frame.headline, (72, 230), _font(76, True), fg, 936, 16, 5) + 34
        if frame.evidence_figure and frame.evidence_figure.get("url"):
            draw.rounded_rectangle((72, y, 1008, y + 560), radius=24, outline=muted, width=3)
            y += 596
        elif frame.evidence_table:
            draw.rounded_rectangle((72, y, 1008, y + 420), radius=24, outline=muted, width=3)
            _draw_wrapped(
                draw,
                frame.evidence_table.get("label") or frame.evidence_table.get("id") or "Source table",
                (108, y + 36),
                _font(38, True),
                fg,
                860,
                max_lines=1,
            )
            _draw_wrapped(draw, frame.evidence_table.get("caption") or "", (108, y + 96), _font(30), muted, 860, max_lines=5)
            y += 456
        elif frame.body:
            y = _draw_wrapped(draw, frame.body, (72, y), _font(42), muted, 936, 14, 8) + 24
        for point in (frame.key_points or [])[:3]:
            draw.ellipse((78, y + 16, 96, y + 34), fill=fg)
            y = _draw_wrapped(draw, point, (120, y), _font(36), fg, 850, 10, 2) + 12
        draw.text((72, HEIGHT - 120), "Infodemica", font=_font(34, True), fill=muted)
        png_path.parent.mkdir(parents=True, exist_ok=True)
        image.save(png_path)

    def _load_durations(count: int) -> list[float]:
        metadata_path = output_dir / "audio_metadata.json"
        if not metadata_path.exists():
            return [6.0] * count
        with open(metadata_path, "r", encoding="utf-8") as handle:
            metadata = json.load(handle)
        durations = [
            float(item.get("duration", item.get("clip_duration", 6.0)))
            for item in metadata.get("scene_boundaries", [])
        ]
        if len(durations) < count:
            durations.extend([6.0] * (count - len(durations)))
        return durations[:count]

    frames = sorted(load_frames(output_dir / "frames.json"), key=lambda item: item.order)
    if not frames:
        raise RuntimeError("No editable frames found to export.")

    export_dir = output_dir / "mp4_export"
    export_dir.mkdir(parents=True, exist_ok=True)
    durations = _load_durations(len(frames))
    concat_path = export_dir / "frames.txt"
    lines = []
    for idx, frame in enumerate(frames):
        png_path = export_dir / f"frame_{idx:03d}.png"
        _render_png(frame, png_path)
        lines += [f"file '{png_path}'", f"duration {max(1.0, durations[idx]):.3f}"]
    lines.append(f"file '{export_dir / f'frame_{len(frames) - 1:03d}.png'}'")
    concat_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    audio_path = output_dir / "audio.wav"
    cmd = ["ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", str(concat_path)]
    if audio_path.exists():
        cmd += ["-i", str(audio_path), "-shortest"]
    cmd += ["-vf", "fps=30,format=yuv420p", "-c:v", "libx264", "-pix_fmt", "yuv420p"]
    if audio_path.exists():
        cmd += ["-c:a", "aac"]
    cmd.append(str(output_path))
    subprocess.run(cmd, check=True, capture_output=True, text=True)
    return output_path


def export_mp4(output_dir: Path, output_path: Path | None = None) -> Path:
    output_path = output_path or (output_dir / "presentation.mp4")
    export_dir = output_dir / "mp4_export"

    if (output_dir / "presentation.html").exists():
        raw_video = _record_presentation_html(output_dir, export_dir)
        audio_path = output_dir / "audio.wav"
        if audio_path.exists():
            _mux_audio(raw_video, audio_path, output_path)
        else:
            _transcode_video(raw_video, output_path)
        return output_path

    return _export_mp4_pillow(output_dir, output_path)
