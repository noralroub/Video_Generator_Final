"""Render Claude-generated HTML presentations with local audio timing."""

from __future__ import annotations

import json
import logging
from html import escape
from pathlib import Path

from claude_presentation import (
    PLACEHOLDER_AUDIO_SRC,
    PLACEHOLDER_SCENE_DURATIONS_JSON,
    PLACEHOLDER_TITLE,
    PLACEHOLDER_TOTAL_DURATION_JSON,
    generate_presentation_html,
)
from scenes import load_scenes

logger = logging.getLogger(__name__)

DEFAULT_SCENE_DURATION = 6.0


def _load_audio_durations(metadata_path: Path, scene_count: int) -> tuple[list[float], float]:
    if not metadata_path.exists():
        durations = [DEFAULT_SCENE_DURATION] * scene_count
        return durations, sum(durations)

    with open(metadata_path, "r", encoding="utf-8") as f:
        metadata = json.load(f)

    boundaries = metadata.get("scene_boundaries", [])
    durations = [
        float(item.get("duration", item.get("clip_duration", DEFAULT_SCENE_DURATION)))
        for item in boundaries
    ]
    if len(durations) < scene_count:
        durations.extend([DEFAULT_SCENE_DURATION] * (scene_count - len(durations)))
    else:
        durations = durations[:scene_count]
    return durations, sum(durations)


def _load_title(paper_path: Path | None, fallback: str) -> str:
    if not paper_path or not paper_path.exists():
        return fallback
    try:
        with open(paper_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return fallback
    return data.get("title") or fallback


def _fallback_html(title: str, scenes: list, scene_durations: list[float], total_duration: float, audio_src: str) -> str:
    """Create a deterministic HTML video when Claude output is incomplete."""
    scene_markup = []
    gradients = [
        ("#0f172a", "#2563eb", "#a855f7"),
        ("#111827", "#0891b2", "#22c55e"),
        ("#18181b", "#f97316", "#ef4444"),
        ("#0c0a09", "#f59e0b", "#14b8a6"),
        ("#020617", "#6366f1", "#ec4899"),
    ]
    for idx, scene in enumerate(scenes):
        bg, a, b = gradients[idx % len(gradients)]
        active = " active" if idx == 0 else ""
        text = escape(scene.text or "")
        source_table = getattr(scene, "source_table", None) or {}
        source_figure = getattr(scene, "source_figure", None) or {}
        evidence_markup = ""
        if source_table:
            label = escape(source_table.get("label") or source_table.get("id") or "Source table")
            caption = escape(source_table.get("caption") or "")
            rows = []
            for raw_row in (source_table.get("text") or "").splitlines()[:5]:
                cells = [escape(cell.strip()) for cell in raw_row.split("|") if cell.strip()]
                if cells:
                    rows.append("<tr>" + "".join(f"<td>{cell}</td>" for cell in cells[:4]) + "</tr>")
            evidence_markup = f"""
        <div class="source-evidence-card source-table-card">
          <strong>{label}</strong>
          <span>{caption}</span>
          <table>{"".join(rows)}</table>
        </div>"""
        elif source_figure:
            label = escape(source_figure.get("id") or "Source image")
            caption = escape(source_figure.get("caption") or "")
            image_url = escape(source_figure.get("url") or "")
            image_markup = f'<img src="{image_url}" alt="{label}">' if image_url else ""
            evidence_markup = f"""
        <div class="source-evidence-card source-figure-card">
          <strong>{label}</strong>
          {image_markup}
          <span>{caption}</span>
        </div>"""
        else:
            evidence_markup = """
        <div class="visual-card" aria-hidden="true">
          <div class="visual-path"></div>
          <div class="visual-figure"></div>
          <div class="visual-node node-a"></div>
          <div class="visual-node node-b"></div>
          <div class="visual-node node-c"></div>
          <div class="visual-wave wave-a"></div>
          <div class="visual-wave wave-b"></div>
        </div>"""
        scene_markup.append(
            f"""
      <section class="scene{active}" data-scene-index="{idx + 1}" style="--bg:{bg};--a:{a};--b:{b};">
        <div class="orb orb-one"></div>
        <div class="orb orb-two"></div>
        <div class="grid"></div>
        <div class="metric">{idx + 1:02d}</div>
        {evidence_markup}
        <p class="caption">{text}</p>
      </section>"""
        )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{escape(title)}</title>
<style>
* {{ box-sizing: border-box; }}
body {{
  margin: 0;
  min-height: 100vh;
  display: grid;
  place-items: center;
  background: #020617;
  font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
  color: white;
}}
.stage {{
  position: relative;
  width: min(100vw, 405px);
  aspect-ratio: 9 / 16;
  overflow: hidden;
  background: #020617;
  border-radius: 18px;
  box-shadow: 0 24px 80px rgba(0,0,0,.45);
}}
.scene {{
  position: absolute;
  inset: 0;
  padding: 34px 28px 108px;
  display: flex;
  flex-direction: column;
  justify-content: flex-end;
  gap: 18px;
  opacity: 0;
  transform: scale(1.04);
  transition: opacity .55s ease, transform .8s ease;
  background:
    radial-gradient(circle at 22% 18%, color-mix(in srgb, var(--a) 50%, transparent), transparent 28%),
    radial-gradient(circle at 78% 34%, color-mix(in srgb, var(--b) 45%, transparent), transparent 30%),
    linear-gradient(160deg, var(--bg), #020617 72%);
}}
.scene.active {{ opacity: 1; transform: scale(1); }}
.scene::after {{
  content: "";
  position: absolute;
  inset: 36% 0 0;
  background: linear-gradient(to top, rgba(2,6,23,.94), rgba(2,6,23,.46), transparent);
  pointer-events: none;
}}
.grid {{
  position: absolute;
  inset: 0;
  background-image:
    linear-gradient(rgba(255,255,255,.055) 1px, transparent 1px),
    linear-gradient(90deg, rgba(255,255,255,.055) 1px, transparent 1px);
  background-size: 34px 34px;
  mask-image: linear-gradient(to bottom, black, transparent 72%);
  animation: drift 9s linear infinite;
}}
.orb {{
  position: absolute;
  border-radius: 999px;
  filter: blur(2px);
  opacity: .72;
  animation: float 5s ease-in-out infinite alternate;
}}
.orb-one {{ width: 150px; height: 150px; top: 70px; left: 40px; background: var(--a); }}
.orb-two {{ width: 110px; height: 110px; top: 210px; right: 30px; background: var(--b); animation-delay: -1.8s; }}
.metric {{
  position: absolute;
  top: 24px;
  right: 24px;
  z-index: 2;
  font-size: 13px;
  letter-spacing: .18em;
  color: rgba(255,255,255,.58);
}}
.visual-card {{
  position: relative;
  z-index: 2;
  align-self: stretch;
  min-height: 150px;
  padding: 20px;
  border: 1px solid rgba(255,255,255,.18);
  border-radius: 18px;
  overflow: hidden;
  background:
    radial-gradient(circle at 20% 28%, color-mix(in srgb, var(--a) 34%, transparent), transparent 24%),
    radial-gradient(circle at 82% 66%, color-mix(in srgb, var(--b) 30%, transparent), transparent 22%),
    linear-gradient(145deg, rgba(255,255,255,.16), rgba(15,23,42,.42));
  backdrop-filter: blur(14px);
}}
.visual-card::before {{
  content: "";
  position: absolute;
  inset: 12px;
  border-radius: 14px;
  border: 1px solid rgba(255,255,255,.14);
  background-image:
    linear-gradient(rgba(255,255,255,.08) 1px, transparent 1px),
    linear-gradient(90deg, rgba(255,255,255,.08) 1px, transparent 1px);
  background-size: 22px 22px;
  opacity: .52;
}}
.visual-card::after {{
  content: "";
  position: absolute;
  width: 86px;
  height: 86px;
  right: 28px;
  top: 28px;
  border-radius: 28px;
  background:
    linear-gradient(135deg, rgba(255,255,255,.8), transparent),
    linear-gradient(135deg, var(--a), var(--b));
  box-shadow: 0 0 40px color-mix(in srgb, var(--a) 36%, transparent);
  animation: pulse 4s ease-in-out infinite;
}}
.visual-path {{
  position: absolute;
  left: 22px;
  right: 32px;
  bottom: 32px;
  height: 72px;
  border-bottom: 7px solid rgba(255,255,255,.74);
  border-radius: 0 0 80% 40%;
  transform: rotate(-8deg);
  filter: drop-shadow(0 0 18px color-mix(in srgb, var(--a) 55%, transparent));
}}
.visual-figure {{
  position: absolute;
  left: 54px;
  bottom: 78px;
  width: 28px;
  height: 44px;
  border-radius: 18px 18px 10px 10px;
  background: rgba(255,255,255,.88);
  box-shadow: 0 0 24px rgba(255,255,255,.26);
  animation: walk 3.8s ease-in-out infinite alternate;
}}
.visual-figure::before {{
  content: "";
  position: absolute;
  left: 5px;
  top: -22px;
  width: 18px;
  height: 18px;
  border-radius: 999px;
  background: rgba(255,255,255,.92);
}}
.visual-node {{
  position: absolute;
  width: 14px;
  height: 14px;
  border-radius: 999px;
  background: var(--b);
  box-shadow: 0 0 24px color-mix(in srgb, var(--b) 70%, transparent);
  animation: pulse 3s ease-in-out infinite;
}}
.node-a {{ left: 128px; top: 42px; }}
.node-b {{ left: 190px; bottom: 56px; animation-delay: -.8s; }}
.node-c {{ right: 108px; top: 86px; animation-delay: -1.4s; }}
.visual-wave {{
  position: absolute;
  left: 36px;
  right: 42px;
  height: 2px;
  border-radius: 999px;
  background: linear-gradient(90deg, transparent, rgba(255,255,255,.62), transparent);
  transform: rotate(-10deg);
  animation: sweep 4.5s linear infinite;
}}
.wave-a {{ top: 62px; }}
.wave-b {{ top: 104px; animation-delay: -2.2s; }}
.source-evidence-card {{
  position: relative;
  z-index: 2;
  padding: 14px;
  border: 1px solid rgba(255,255,255,.24);
  border-radius: 16px;
  background: rgba(2,6,23,.72);
  backdrop-filter: blur(10px);
  box-shadow: 0 18px 50px rgba(0,0,0,.24);
  max-height: 330px;
  overflow: hidden;
}}
.source-evidence-card strong {{ display: block; font-size: .85rem; margin-bottom: 5px; }}
.source-evidence-card span {{ display: block; font-size: .68rem; color: rgba(255,255,255,.72); line-height: 1.25; margin-bottom: 8px; }}
.source-table-card table {{ width: 100%; border-collapse: collapse; font-size: .62rem; color: rgba(255,255,255,.9); }}
.source-table-card td {{ border-top: 1px solid rgba(255,255,255,.14); padding: 4px 5px; }}
.source-figure-card img {{
  display: block;
  width: 100%;
  max-height: 230px;
  object-fit: contain;
  margin: 8px 0;
  border-radius: 10px;
  background: rgba(255,255,255,.92);
}}
@keyframes walk {{
  from {{ transform: translate3d(0, 0, 0) rotate(-4deg); }}
  to {{ transform: translate3d(54px, -18px, 0) rotate(6deg); }}
}}
@keyframes pulse {{
  0%, 100% {{ transform: scale(.92); opacity: .66; }}
  50% {{ transform: scale(1.08); opacity: 1; }}
}}
@keyframes sweep {{
  from {{ transform: translateX(-35%) rotate(-10deg); opacity: 0; }}
  20%, 72% {{ opacity: .72; }}
  to {{ transform: translateX(35%) rotate(-10deg); opacity: 0; }}
}}
.caption {{
  position: relative;
  z-index: 2;
  margin: 0;
  font-size: 25px;
  line-height: 1.12;
  font-weight: 780;
  letter-spacing: 0;
  text-wrap: balance;
  text-shadow: 0 4px 28px rgba(0,0,0,.58);
}}
.controls {{
  position: absolute;
  z-index: 10;
  left: 18px;
  right: 18px;
  bottom: 18px;
  display: flex;
  align-items: center;
  gap: 12px;
}}
.play {{
  width: 46px;
  height: 46px;
  border: 0;
  border-radius: 999px;
  color: #020617;
  background: white;
  font-size: 18px;
  cursor: pointer;
}}
.progress {{
  flex: 1;
  height: 6px;
  overflow: hidden;
  border-radius: 999px;
  background: rgba(255,255,255,.18);
}}
.progress-fill {{
  width: 0%;
  height: 100%;
  background: linear-gradient(90deg, #fff, #93c5fd, #f0abfc);
}}
.time {{
  width: 42px;
  font-size: 12px;
  color: rgba(255,255,255,.72);
  text-align: right;
}}
audio {{ display: none; }}
@keyframes float {{
  from {{ transform: translate3d(-12px, 8px, 0) scale(.95); }}
  to {{ transform: translate3d(14px, -16px, 0) scale(1.08); }}
}}
@keyframes drift {{
  from {{ transform: translateY(0); }}
  to {{ transform: translateY(34px); }}
}}
</style>
</head>
<body>
  <main class="stage">
    <audio id="narration" src="{escape(audio_src)}" preload="auto"></audio>
    {"".join(scene_markup)}
    <div class="controls">
      <button class="play" data-play-toggle aria-label="Play or pause">▶</button>
      <div class="progress"><div class="progress-fill" data-progress-bar></div></div>
      <div class="time" data-time>0:00</div>
    </div>
  </main>
<script>
(function () {{
  var audio = document.getElementById('narration');
  var scenes = Array.prototype.slice.call(document.querySelectorAll('.scene'));
  var play = document.querySelector('[data-play-toggle]');
  var progress = document.querySelector('[data-progress-bar]');
  var time = document.querySelector('[data-time]');
  var sceneDurations = {json.dumps(scene_durations)};
  var totalDuration = {json.dumps(total_duration)};

  function activeIndex(current) {{
    var elapsed = 0;
    for (var i = 0; i < sceneDurations.length; i++) {{
      elapsed += Number(sceneDurations[i]) || 0;
      if (current <= elapsed) return i;
    }}
    return scenes.length - 1;
  }}

  function format(seconds) {{
    seconds = Math.max(0, Math.floor(seconds || 0));
    return Math.floor(seconds / 60) + ':' + String(seconds % 60).padStart(2, '0');
  }}

  function update() {{
    var current = audio.currentTime || 0;
    var idx = activeIndex(current);
    scenes.forEach(function(scene, i) {{ scene.classList.toggle('active', i === idx); }});
    progress.style.width = Math.min(100, (current / totalDuration) * 100) + '%';
    time.textContent = format(current);
  }}

  play.addEventListener('click', function () {{
    if (audio.paused) {{ audio.play(); play.textContent = 'Ⅱ'; }}
    else {{ audio.pause(); play.textContent = '▶'; }}
  }});
  audio.addEventListener('timeupdate', update);
  audio.addEventListener('ended', function () {{ play.textContent = '▶'; update(); }});
  update();
}}());
</script>
</body>
</html>"""


def render_presentation_claude(
    script_path: Path,
    output_path: Path,
    audio_metadata_path: Path | None = None,
    audio_src: str = "audio.wav",
    paper_path: Path | None = None,
) -> None:
    """Generate and write a Claude HTML presentation."""
    scenes = load_scenes(script_path)
    if not scenes:
        raise ValueError("script.json has no scenes")

    html = generate_presentation_html(script_path, paper_path)
    scene_durations, total_duration = _load_audio_durations(
        audio_metadata_path or Path(""), len(scenes)
    )

    title = _load_title(paper_path, scenes[0].text)
    html = (
        html.replace(PLACEHOLDER_TITLE, title)
        .replace(PLACEHOLDER_AUDIO_SRC, audio_src)
        .replace(PLACEHOLDER_SCENE_DURATIONS_JSON, json.dumps(scene_durations))
        .replace(PLACEHOLDER_TOTAL_DURATION_JSON, json.dumps(total_duration))
    )

    if html.count('class="scene') < len(scenes) or "</html>" not in html or "</body>" not in html:
        logger.warning("Claude presentation was incomplete; using deterministic HTML video fallback")
        html = _fallback_html(title, scenes, scene_durations, total_duration, audio_src)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html, encoding="utf-8")
    logger.info(
        "Rendered Claude presentation to %s (%s scenes, %.1fs)",
        output_path,
        len(scenes),
        total_duration,
    )
