"""Journal brand overlay for HTML presentations (fixed logo on all slides)."""

from __future__ import annotations

import logging
import os
import re
import shutil
from pathlib import Path

logger = logging.getLogger(__name__)

_ASSETS_ROOT = Path(__file__).parent / "assets" / "brands"

BRAND_REGISTRY: dict[str, dict[str, str]] = {
    "jacc": {
        "logo_filename": "jacc-logo.svg",
        "alt": "Journal of the American College of Cardiology",
    },
}

BRAND_STYLE = """<style data-infodemica-journal-brand="true">
.journal-brand-lockup {
  position: absolute;
  top: 28px;
  right: 24px;
  z-index: 100;
  pointer-events: none;
  max-width: 26%;
  line-height: 0;
}
.journal-brand-lockup img {
  display: block;
  width: 100%;
  height: auto;
  max-height: 56px;
  object-fit: contain;
  filter: drop-shadow(0 2px 8px rgba(0, 0, 0, 0.45));
}
</style>"""

_LOCKUP_TEMPLATE = (
    '<div class="journal-brand-lockup" data-infodemica-journal-brand="{brand}">'
    '<img src="{logo_src}" alt="{alt}" decoding="async" />'
    "</div>"
)

_STAGE_PATTERN = re.compile(
    r"(<(div|main)\b[^>]*\b(?:id=[\"']stage[\"']|class=[\"'][^\"']*\bstage\b[^\"']*[\"'])[^>]*>)",
    re.IGNORECASE,
)


def get_presentation_journal_brand() -> str | None:
    """Brand key from environment; None disables overlay."""
    brand = os.getenv("PRESENTATION_JOURNAL_BRAND", "jacc").strip().lower()
    return brand or None


def _source_logo_path(brand: str) -> Path | None:
    config = BRAND_REGISTRY.get(brand)
    if not config:
        return None
    filename = config["logo_filename"]
    primary = _ASSETS_ROOT / brand / filename
    if primary.exists():
        return primary
    png_fallback = primary.with_suffix(".png")
    if png_fallback.exists():
        return png_fallback
    logger.warning("Journal brand asset missing for %s: %s", brand, primary)
    return None


def copy_brand_assets(output_dir: Path, brand: str | None = None) -> str | None:
    """Copy logo into paper media folder; returns filename used or None."""
    brand = brand or get_presentation_journal_brand()
    if not brand:
        return None
    config = BRAND_REGISTRY.get(brand)
    if not config:
        logger.warning("Unknown journal brand: %s", brand)
        return None
    source = _source_logo_path(brand)
    if not source:
        return None
    dest_name = source.name
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    dest = output_dir / dest_name
    if not dest.exists() or source.stat().st_mtime > dest.stat().st_mtime:
        shutil.copy2(source, dest)
        logger.info("Copied journal brand asset to %s", dest)
    return dest_name


def _lockup_markup(brand: str, logo_src: str) -> str:
    config = BRAND_REGISTRY[brand]
    return _LOCKUP_TEMPLATE.format(brand=brand, logo_src=logo_src, alt=config["alt"])


def _inject_style(html: str) -> str:
    if 'data-infodemica-journal-brand="true"' in html:
        return re.sub(
            r'<style\s+data-infodemica-journal-brand="true"\s*>.*?</style>\s*',
            BRAND_STYLE + "\n",
            html,
            count=1,
            flags=re.DOTALL | re.IGNORECASE,
        )
    if "</head>" in html:
        return html.replace("</head>", BRAND_STYLE + "\n</head>", 1)
    return BRAND_STYLE + html


def _remove_existing_lockup(html: str) -> str:
    return re.sub(
        r'<div\s+class="journal-brand-lockup"[^>]*>.*?</div>\s*',
        "",
        html,
        count=1,
        flags=re.DOTALL | re.IGNORECASE,
    )


def _inject_lockup_into_stage(html: str, lockup: str) -> str:
    match = _STAGE_PATTERN.search(html)
    if match:
        insert_at = match.end()
        return html[:insert_at] + lockup + html[insert_at:]
    logger.warning("No .stage element found for journal brand lockup")
    return html


def ensure_journal_brand(html: str, brand: str | None = None, logo_src: str | None = None) -> str:
    """Inject fixed top-right journal logo inside the presentation stage."""
    brand = brand or get_presentation_journal_brand()
    if not brand or brand not in BRAND_REGISTRY:
        return html

    config = BRAND_REGISTRY[brand]
    source = _source_logo_path(brand)
    if not logo_src:
        logo_src = source.name if source else config["logo_filename"]

    html = _inject_style(html)
    html = _remove_existing_lockup(html)
    lockup = _lockup_markup(brand, logo_src)
    html = _inject_lockup_into_stage(html, lockup)
    return html
