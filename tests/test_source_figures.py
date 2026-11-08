"""Tests for PMC figure download and script normalization."""

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

PIPELINE_DIR = Path(__file__).resolve().parent.parent / "pipeline"
if str(PIPELINE_DIR) not in sys.path:
    sys.path.insert(0, str(PIPELINE_DIR))

from claude_presentation import _resolve_source_figure  # noqa: E402
from source_figures import (  # noqa: E402
    ensure_figure_cached,
    fix_proxy_urls_in_html,
    normalize_source_figure,
    prefetch_paper_figures,
)


class SourceFiguresTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp(prefix="infodemica-figures-"))

    def _write_figure(self, index: int, suffix: str = "png") -> None:
        figures_dir = self.temp_dir / "source_figures"
        figures_dir.mkdir(parents=True, exist_ok=True)
        (figures_dir / f"figure_{index}.{suffix}").write_bytes(b"\x89PNG\r\n")

    def test_normalize_legacy_proxy_url_without_figure_index(self):
        self._write_figure(2)
        meta = {"url": "/source-image/38774006/2/"}
        with open(self.temp_dir / "paper.json", "w", encoding="utf-8") as f:
            json.dump(
                {
                    "figures": [
                        {"id": "a", "url": "https://example.com/a.jpg"},
                        {"id": "b", "url": "https://example.com/b.jpg"},
                        {"id": "c", "url": "https://example.com/c.jpg"},
                    ]
                },
                f,
            )
        result = normalize_source_figure(self.temp_dir, meta)
        self.assertEqual(result["figure_index"], 2)
        self.assertEqual(result["url"], "source_figures/figure_2.png")

    def test_fix_proxy_urls_in_html(self):
        self._write_figure(2)
        html = '<img src="/source-image/38774006/2/" alt="fig">'
        fixed = fix_proxy_urls_in_html(html, self.temp_dir)
        self.assertIn('src="source_figures/figure_2.png"', fixed)

    def test_normalize_source_figure_sets_relative_url(self):
        self._write_figure(1)
        meta = {
            "id": "fig2",
            "figure_index": 1,
            "source_url": "https://example.com/figure_1.png",
            "url": "/source-image/38774006/1/",
        }
        result = normalize_source_figure(self.temp_dir, meta)
        self.assertIsNotNone(result)
        self.assertEqual(result["url"], "source_figures/figure_1.png")
        self.assertEqual(result["figure_index"], 1)

    @patch("source_figures.download_figure")
    def test_prefetch_skips_failed_downloads(self, mock_download):
        mock_download.side_effect = OSError("network error")
        paper = {
            "title": "Test",
            "full_text": "Body",
            "figures": [
                {"id": "f0", "url": "https://example.com/a.jpg", "caption": "A"},
                {"id": "f1", "url": "https://example.com/b.jpg", "caption": "B"},
            ],
        }
        with open(self.temp_dir / "paper.json", "w", encoding="utf-8") as f:
            json.dump(paper, f)
        (self.temp_dir / "source_figures").mkdir(parents=True, exist_ok=True)
        (self.temp_dir / "source_figures" / "figure_0.jpg").write_bytes(b"ok")

        cached = prefetch_paper_figures(self.temp_dir)
        self.assertEqual(cached, 1)

    def test_resolve_source_figure_uses_figure_index_not_scene_index(self):
        """Scene 2 can reference paper figure 1 (figure_1.png), not figure_2."""
        self._write_figure(1)
        figure = {
            "id": "fig2",
            "figure_index": 1,
            "url": "source_figures/figure_1.png",
        }
        resolved = _resolve_source_figure(self.temp_dir, figure, scene_index=2)
        self.assertEqual(resolved["url"], "source_figures/figure_1.png")

    @patch("source_figures.download_figure")
    def test_ensure_figure_cached_downloads_when_missing(self, mock_download):
        dest = self.temp_dir / "source_figures" / "figure_0.jpg"

        def write_file(url, path):
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"jpeg")
            return path

        mock_download.side_effect = write_file
        rel = ensure_figure_cached(
            self.temp_dir,
            0,
            {"url": "https://example.com/fig.jpg"},
        )
        self.assertEqual(rel, "source_figures/figure_0.jpg")
        mock_download.assert_called_once()


if __name__ == "__main__":
    unittest.main()
