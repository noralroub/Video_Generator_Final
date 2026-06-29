"""Tests for scene enrichment helpers."""

import sys
import unittest
from pathlib import Path

PIPELINE_DIR = Path(__file__).resolve().parent.parent / "pipeline"
if str(PIPELINE_DIR) not in sys.path:
    sys.path.insert(0, str(PIPELINE_DIR))

from scenes import (  # noqa: E402
    Scene,
    compute_phrase_timings,
    enrich_scene,
    extract_emphasis_words,
    load_scenes,
    save_scenes,
    wrap_emphasis_html,
)


class SceneEnrichmentTests(unittest.TestCase):
    def test_enrich_scene_splits_phrases_and_infers_layout(self):
        scene = Scene(
            text="They found statins reduced heart attacks by 27 percent.",
            visual_type="generated",
            visual_content="Show key result.",
        )
        enriched = enrich_scene(scene)
        self.assertGreaterEqual(len(enriched.display_phrases or []), 2)
        self.assertEqual(enriched.visual_layout, "stat_counter")
        self.assertIsNotNone(enriched.chart_data)

    def test_extract_emphasis_words_finds_numbers(self):
        words = extract_emphasis_words("Risk dropped 27% in the trial.")
        self.assertTrue(any("27" in word for word in words))

    def test_compute_phrase_timings_respects_duration(self):
        phrases = [
            {"text": "First phrase here.", "emphasis": []},
            {"text": "Second phrase follows.", "emphasis": []},
        ]
        timed = compute_phrase_timings(phrases, 4.0)
        self.assertEqual(timed[0]["enter_ms"], 0)
        self.assertGreater(timed[1]["enter_ms"], 0)
        self.assertLess(timed[1]["enter_ms"], 4000)

    def test_wrap_emphasis_html(self):
        html = wrap_emphasis_html("Statins cut risk by 27%", ["Statins", "27%"])
        self.assertIn('<span class="emph">Statins</span>', html)
        self.assertIn('<span class="emph">27%</span>', html)

    def test_load_scenes_backward_compatible(self):
        script_path = Path(self._tmpdir()) / "script.json"
        save_scenes(
            [
                Scene(
                    text="Legacy narration.",
                    visual_type="generated",
                    visual_content="Legacy visual.",
                )
            ],
            script_path,
        )
        scenes = load_scenes(script_path)
        self.assertEqual(len(scenes), 1)
        self.assertEqual(scenes[0].text, "Legacy narration.")
        self.assertIsNotNone(scenes[0].display_phrases)

    def _tmpdir(self):
        import tempfile

        return tempfile.mkdtemp()


if __name__ == "__main__":
    unittest.main()
