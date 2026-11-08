"""Download and resolve PMC figure images for presentations and MP4 export."""

from __future__ import annotations

import json
import logging
import mimetypes
import re
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_USER_AGENT = "Mozilla/5.0 Infodemica local source image loader"
_ALLOWED_SUFFIXES = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".svg"}
_PROXY_PATH_RE = re.compile(r"^/source-image/")
_PROXY_INDEX_RE = re.compile(r"^/source-image/[^/]+/(\d+)/?$")


def paper_figure_meta(output_dir: Path, figure_index: int) -> dict[str, Any] | None:
    """Load figure metadata from paper.json by list index."""
    paper_path = Path(output_dir) / "paper.json"
    if not paper_path.exists():
        return None
    try:
        with open(paper_path, "r", encoding="utf-8") as f:
            figures = json.load(f).get("figures") or []
    except (OSError, json.JSONDecodeError):
        return None
    if 0 <= figure_index < len(figures):
        return dict(figures[figure_index])
    return None


def _paper_pmcid(output_dir: Path) -> str:
    paper_path = Path(output_dir) / "paper.json"
    if not paper_path.exists():
        return ""
    try:
        with open(paper_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return ""
    return str(data.get("pmcid") or "").strip()


def load_pmc_cdn_images(pmcid: str) -> dict[str, str]:
    """Map PMC image file names to NCBI CDN blob URLs from the article page."""
    match = re.search(r"PMC\d+", pmcid or "", re.IGNORECASE)
    if not match:
        return {}
    article_url = f"https://pmc.ncbi.nlm.nih.gov/articles/{match.group(0).upper()}/"
    try:
        request_obj = urllib.request.Request(
            article_url,
            headers={"User-Agent": _USER_AGENT},
        )
        with urllib.request.urlopen(request_obj, timeout=15) as response:
            html = response.read().decode("utf-8", errors="ignore")
    except Exception as exc:
        logger.warning("Could not load PMC article page for images %s: %s", article_url, exc)
        return {}

    image_urls: dict[str, str] = {}
    for url in re.findall(r'https://cdn\.ncbi\.nlm\.nih\.gov/pmc/blobs/[^"\'>\s]+', html):
        image_urls[Path(urllib.parse.urlparse(url).path).name] = url
    return image_urls


def _figure_image_name(figure_meta: dict[str, Any]) -> str:
    for key in ("url", "source_url"):
        value = (figure_meta.get(key) or "").strip()
        if not value:
            continue
        if "tileshop" in value and "id=" in value:
            parsed = re.search(r"id=(\d+)_([^&\"']+)", value)
            if parsed:
                return parsed.group(2)
        path_name = Path(urllib.parse.urlparse(value).path).name
        if path_name and "." in path_name:
            return path_name
    return ""


def resolve_figure_download_url(
    output_dir: Path,
    figure_meta: dict[str, Any],
    figure_index: int | None = None,
) -> str:
    """Resolve a fetchable image URL (prefer PMC CDN blobs over legacy bin/tileshop links)."""
    meta = dict(figure_meta)
    if figure_index is not None:
        paper_fig = paper_figure_meta(output_dir, figure_index)
        if paper_fig:
            for key in ("id", "caption", "url", "source_url"):
                meta.setdefault(key, paper_fig.get(key))

    image_name = _figure_image_name(meta)
    pmcid = _paper_pmcid(output_dir)
    if pmcid and image_name:
        cdn_images = load_pmc_cdn_images(pmcid)
        if image_name in cdn_images:
            return cdn_images[image_name]

    for key in ("url", "source_url"):
        value = (meta.get(key) or "").strip()
        if not value.startswith(("http://", "https://")):
            continue
        if "tileshop" in value or value.lower().endswith(".html"):
            continue
        if "cdn.ncbi.nlm.nih.gov/pmc/blobs/" in value:
            return value
        if "/bin/" in value and pmcid and image_name:
            cdn_images = load_pmc_cdn_images(pmcid)
            if image_name in cdn_images:
                return cdn_images[image_name]
        if "/bin/" not in value:
            return value
    return ""


def _download_url(figure_meta: dict[str, Any]) -> str:
    """Pick the best HTTP URL to download raster content (legacy helper)."""
    for key in ("url", "source_url"):
        value = (figure_meta.get(key) or "").strip()
        if value.startswith(("http://", "https://")) and "tileshop" not in value:
            if not value.lower().endswith(".html"):
                return value
    return ""


def figure_cache_path(output_dir: Path, figure_index: int, source_url: str) -> Path:
    """Path for a cached figure file under source_figures/."""
    suffix = Path(source_url.split("?")[0]).suffix.lower()
    if suffix not in _ALLOWED_SUFFIXES:
        suffix = ".jpg"
    return Path(output_dir) / "source_figures" / f"figure_{figure_index}{suffix}"


def _suffix_from_content_type(content_type: str) -> str:
    guessed = mimetypes.guess_extension((content_type or "").split(";")[0].strip())
    if guessed and guessed.lower() in _ALLOWED_SUFFIXES:
        return guessed.lower()
    return ".jpg"


def download_figure(source_url: str, dest: Path) -> Path:
    """Download image bytes to dest; returns final path (may differ by extension)."""
    request_obj = urllib.request.Request(
        source_url,
        headers={
            "User-Agent": _USER_AGENT,
            "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
        },
    )
    with urllib.request.urlopen(request_obj, timeout=15) as response:
        image_bytes = response.read()
        content_type = response.headers.get_content_type() or "image/jpeg"

    if content_type.startswith("text/") or image_bytes[:15].lower().startswith(b"<!doctype"):
        raise ValueError(f"URL returned HTML, not an image ({content_type})")
    if len(image_bytes) < 200:
        raise ValueError("Downloaded figure is too small to be a valid image")

    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.suffix.lower() not in _ALLOWED_SUFFIXES:
        dest = dest.with_suffix(_suffix_from_content_type(content_type))
    dest.write_bytes(image_bytes)
    return dest


def relative_figure_url(cache_path: Path, output_dir: Path) -> str:
    try:
        rel = cache_path.relative_to(output_dir)
    except ValueError:
        rel = cache_path
    return str(rel).replace("\\", "/")


def ensure_figure_cached(
    output_dir: Path,
    figure_index: int,
    figure_meta: dict[str, Any],
) -> str | None:
    """Ensure figure is on disk; return relative url or None."""
    output_dir = Path(output_dir)
    figures_dir = output_dir / "source_figures"
    for path in sorted(figures_dir.glob(f"figure_{figure_index}.*")):
        if path.is_file():
            return relative_figure_url(path, output_dir)

    source_url = resolve_figure_download_url(output_dir, figure_meta, figure_index)
    if not source_url:
        source_url = _download_url(figure_meta)
    if not source_url:
        rel = (figure_meta.get("url") or "").strip()
        if rel.startswith("source_figures/") and (output_dir / rel).is_file():
            return rel
        logger.warning("No download URL for figure index %s", figure_index)
        return None

    cache_path = figure_cache_path(output_dir, figure_index, source_url)
    if cache_path.exists():
        return relative_figure_url(cache_path, output_dir)

    try:
        final_path = download_figure(source_url, cache_path)
        logger.info("Cached figure %s at %s", figure_index, final_path.name)
        return relative_figure_url(final_path, output_dir)
    except Exception as exc:
        logger.warning("Could not download figure %s from %s: %s", figure_index, source_url, exc)
        return None


def normalize_source_figure(
    output_dir: Path,
    source_figure: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """Ensure local relative url; drop Django proxy paths."""
    if not source_figure:
        return None
    meta = dict(source_figure)
    url = (meta.get("url") or "").strip()

    figure_index = meta.get("figure_index")
    if _PROXY_PATH_RE.match(url) or url.startswith("/source-image/"):
        proxy_match = _PROXY_INDEX_RE.match(url)
        if proxy_match and figure_index is None:
            figure_index = int(proxy_match.group(1))
            meta["figure_index"] = figure_index
        meta.pop("url", None)

    if figure_index is not None:
        try:
            figure_index = int(figure_index)
        except (TypeError, ValueError):
            figure_index = None
        if figure_index is not None and not _download_url(meta):
            from_paper = paper_figure_meta(output_dir, figure_index)
            if from_paper:
                meta.setdefault("id", from_paper.get("id"))
                meta.setdefault("caption", from_paper.get("caption"))
                if from_paper.get("url"):
                    meta.setdefault("url", from_paper["url"])

    if figure_index is None and url.startswith("source_figures/"):
        return meta

    if figure_index is not None:
        rel = ensure_figure_cached(output_dir, figure_index, meta)
        if rel:
            meta["url"] = rel
            meta["figure_index"] = figure_index
            if not meta.get("source_url"):
                meta["source_url"] = _download_url(meta)
            return meta

    if url.startswith("source_figures/"):
        path = output_dir / url
        if path.exists():
            return meta

    return meta if meta.get("url") else None


def prefetch_paper_figures(output_dir: Path) -> int:
    """Download all figures listed in paper.json; returns count successfully cached."""
    output_dir = Path(output_dir)
    paper_path = output_dir / "paper.json"
    if not paper_path.exists():
        return 0

    try:
        with open(paper_path, "r", encoding="utf-8") as f:
            paper_data = json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("Could not read paper.json for figure prefetch: %s", exc)
        return 0

    figures = paper_data.get("figures") or []
    cached = 0
    for index, figure in enumerate(figures):
        merged = dict(figure)
        merged.setdefault("figure_index", index)
        if ensure_figure_cached(output_dir, index, merged):
            cached += 1
    logger.info("Prefetched %s/%s paper figures", cached, len(figures))
    return cached


def normalize_script_figures(output_dir: Path) -> int:
    """Update script.json source_figure entries with local relative urls."""
    output_dir = Path(output_dir)
    script_path = output_dir / "script.json"
    if not script_path.exists():
        return 0

    try:
        with open(script_path, "r", encoding="utf-8") as f:
            scenes_data = json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("Could not read script.json for figure normalize: %s", exc)
        return 0

    if not isinstance(scenes_data, list):
        return 0

    updated = 0
    for scene in scenes_data:
        if not isinstance(scene, dict):
            continue
        raw = scene.get("source_figure")
        if not raw:
            continue
        normalized = normalize_source_figure(output_dir, raw)
        if normalized:
            scene["source_figure"] = normalized
            updated += 1
        else:
            scene["source_figure"] = raw

    if updated:
        with open(script_path, "w", encoding="utf-8") as f:
            json.dump(scenes_data, f, indent=2, ensure_ascii=False)
        logger.info("Normalized source_figure on %s scene(s) in script.json", updated)

    return updated


def fix_proxy_urls_in_html(html: str, output_dir: Path) -> str:
    """Replace Django /source-image/... img src with local source_figures paths."""

    def _repl(match: re.Match[str]) -> str:
        fig_idx = int(match.group(1))
        meta: dict[str, Any] = {"figure_index": fig_idx}
        paper = paper_figure_meta(output_dir, fig_idx)
        if paper:
            meta.update(paper)
        normalized = normalize_source_figure(output_dir, meta)
        if normalized and normalized.get("url"):
            return f'src="{normalized["url"]}"'
        return match.group(0)

    return re.sub(r'src="/source-image/[^/]+/(\d+)/"', _repl, html)


def apply_script_figures_to_html(html: str, output_dir: Path, script_path: Path) -> str:
    """Ensure presentation HTML uses cached figure paths from script.json."""
    from scenes import enrich_scene, load_scenes

    output_dir = Path(output_dir)
    html = fix_proxy_urls_in_html(html, output_dir)

    try:
        scenes = load_scenes(script_path)
    except Exception as exc:
        logger.warning("Could not load script for figure HTML fix: %s", exc)
        return html

    for idx, scene in enumerate(scenes):
        if not scene.source_figure:
            continue
        enriched = enrich_scene(scene)
        fig = normalize_source_figure(output_dir, enriched.source_figure)
        if not fig or not fig.get("url"):
            continue
        rel = fig["url"]
        if rel in html:
            continue
        alt = (fig.get("id") or "Study figure").replace('"', "")
        scene_num = idx + 1
        marker = f'data-scene-index="{scene_num}"'
        pos = html.find(marker)
        if pos == -1:
            continue
        insert_at = html.find(">", pos)
        if insert_at == -1:
            continue
        insert_at += 1
        panel = (
            f'<div class="figure-panel ken-burns">'
            f'<img src="{rel}" alt="{alt}" style="object-fit:contain" />'
            f'<div class="figure-label">{alt}</div></div>'
        )
        html = html[:insert_at] + panel + html[insert_at:]

    return html
