"""Server-side MP4 export for structured Infodemica frames."""
from __future__ import annotations
import json, shutil, subprocess
from pathlib import Path
from frames import Frame, load_frames
WIDTH, HEIGHT = 1080, 1920

def _load_durations(output_dir: Path, count: int) -> list[float]:
    metadata_path = output_dir / "audio_metadata.json"
    if not metadata_path.exists(): return [6.0] * count
    with open(metadata_path, "r", encoding="utf-8") as f: metadata = json.load(f)
    durations = [float(item.get("duration", item.get("clip_duration", 6.0))) for item in metadata.get("scene_boundaries", [])]
    if len(durations) < count: durations.extend([6.0] * (count - len(durations)))
    return durations[:count]

def _font(size: int, bold: bool = False):
    from PIL import ImageFont
    candidates = ["/System/Library/Fonts/Supplemental/Arial Bold.ttf" if bold else "/System/Library/Fonts/Supplemental/Arial.ttf", "/Library/Fonts/Arial.ttf"]
    for candidate in candidates:
        try: return ImageFont.truetype(candidate, size)
        except Exception: pass
    return ImageFont.load_default()

def _wrap(draw, text: str, font, max_width: int) -> list[str]:
    lines = []
    for paragraph in (text or "").splitlines() or [""]:
        current = ""
        for word in paragraph.split():
            test = f"{current} {word}".strip()
            if draw.textbbox((0, 0), test, font=font)[2] <= max_width: current = test
            else:
                if current: lines.append(current)
                current = word
        if current: lines.append(current)
    return lines

def _draw_wrapped(draw, text, xy, font, fill, max_width, line_gap=10, max_lines=None):
    x, y = xy
    lines = _wrap(draw, text, font, max_width)
    if max_lines: lines = lines[:max_lines]
    for line in lines:
        draw.text((x, y), line, font=font, fill=fill)
        y += getattr(font, "size", 36) + line_gap
    return y

def _render_png(frame: Frame, output_path: Path) -> None:
    from PIL import Image, ImageDraw
    dark = frame.theme != "light"; bg = (5,5,5) if dark else (248,250,252); fg = (255,255,255) if dark else (17,24,39); muted = (190,198,210) if dark else (82,82,82)
    image = Image.new("RGB", (WIDTH, HEIGHT), bg); draw = ImageDraw.Draw(image)
    for i in range(0, HEIGHT, 72): draw.line((0, i, WIDTH, i), fill=(24,24,24) if dark else (232,232,232), width=1)
    draw.rounded_rectangle((72,82,470,138), radius=28, outline=muted, width=2)
    draw.text((104,100), frame.layout.replace("_"," ").upper(), font=_font(28, True), fill=muted)
    y = _draw_wrapped(draw, frame.headline, (72,230), _font(76, True), fg, 936, 16, 5) + 34
    if frame.evidence_figure and frame.evidence_figure.get("url"):
        draw.rounded_rectangle((72,y,1008,y+560), radius=24, outline=muted, width=3); y += 596
    elif frame.evidence_table:
        draw.rounded_rectangle((72,y,1008,y+420), radius=24, outline=muted, width=3)
        _draw_wrapped(draw, frame.evidence_table.get("label") or frame.evidence_table.get("id") or "Source table", (108,y+36), _font(38, True), fg, 860, max_lines=1)
        _draw_wrapped(draw, frame.evidence_table.get("caption") or "", (108,y+96), _font(30), muted, 860, max_lines=5); y += 456
    elif frame.body:
        y = _draw_wrapped(draw, frame.body, (72,y), _font(42), muted, 936, 14, 8) + 24
    for point in (frame.key_points or [])[:3]:
        draw.ellipse((78,y+16,96,y+34), fill=fg); y = _draw_wrapped(draw, point, (120,y), _font(36), fg, 850, 10, 2) + 12
    draw.text((72, HEIGHT-120), "Infodemica", font=_font(34, True), fill=muted)
    output_path.parent.mkdir(parents=True, exist_ok=True); image.save(output_path)

def export_mp4(output_dir: Path, output_path: Path | None = None) -> Path:
    try: import PIL  # noqa: F401
    except ImportError as exc: raise RuntimeError("Pillow is required for MP4 export. Install dependencies from requirements.txt.") from exc
    if not shutil.which("ffmpeg"): raise RuntimeError("ffmpeg is required for MP4 export and was not found on PATH.")
    frames = sorted(load_frames(output_dir / "frames.json"), key=lambda item: item.order)
    if not frames: raise RuntimeError("No editable frames found to export.")
    durations = _load_durations(output_dir, len(frames)); export_dir = output_dir / "mp4_export"; export_dir.mkdir(parents=True, exist_ok=True)
    concat_path = export_dir / "frames.txt"; lines = []
    for idx, frame in enumerate(frames):
        png_path = export_dir / f"frame_{idx:03d}.png"; _render_png(frame, png_path); lines += [f"file '{png_path}'", f"duration {max(1.0, durations[idx]):.3f}"]
    lines.append(f"file '{export_dir / f'frame_{len(frames)-1:03d}.png'}'"); concat_path.write_text("\n".join(lines)+"\n", encoding="utf-8")
    output_path = output_path or (output_dir / "presentation.mp4"); audio_path = output_dir / "audio.wav"
    cmd = ["ffmpeg","-y","-f","concat","-safe","0","-i",str(concat_path)]
    if audio_path.exists(): cmd += ["-i",str(audio_path),"-shortest"]
    cmd += ["-vf","fps=30,format=yuv420p","-c:v","libx264","-pix_fmt","yuv420p"]
    if audio_path.exists(): cmd += ["-c:a","aac"]
    cmd.append(str(output_path)); subprocess.run(cmd, check=True, capture_output=True, text=True)
    return output_path
