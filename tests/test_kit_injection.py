"""Tests for presentation kit injection."""

import sys
import unittest
from pathlib import Path

PIPELINE_DIR = Path(__file__).resolve().parent.parent / "pipeline"
if str(PIPELINE_DIR) not in sys.path:
    sys.path.insert(0, str(PIPELINE_DIR))

from claude_presentation import (  # noqa: E402
    PLACEHOLDER_AUDIO_SRC,
    PLACEHOLDER_SCENE_DURATIONS_JSON,
    _ensure_contract,
    sanitize_presentation_html,
)


class KitInjectionTests(unittest.TestCase):
    def test_sanitize_removes_duplicate_kit_when_claude_has_sync(self):
        html = _ensure_contract(
            """<!DOCTYPE html><html><head></head><body>
            <audio id="narration" src="audio.wav"></audio>
            <section class="scene active"></section>
            <script>
            var sceneDurations = [1,2];
            var audio = document.getElementById('narration');
            audio.addEventListener('timeupdate', function(){});
            document.querySelectorAll('.scene');
            </script>
            </body></html>"""
        )
        self.assertNotIn("data-infodemica-reveals", html)

    def test_sanitize_keeps_kit_for_minimal_html(self):
        html = _ensure_contract(
            "<!DOCTYPE html><html><head></head><body><section class='scene active'></section></body></html>"
        )
        self.assertIn("data-infodemica-reveals", html)

    def test_ensure_contract_injects_motion_kit(self):
        html = _ensure_contract(
            "<!DOCTYPE html><html><head></head><body><section class='scene active'></section></body></html>"
        )
        self.assertIn(PLACEHOLDER_AUDIO_SRC, html)
        self.assertIn(PLACEHOLDER_SCENE_DURATIONS_JSON, html)
        self.assertIn("timeupdate", html)
        self.assertIn("data-infodemica-motion", html)
        self.assertIn("orbDrift", html)
        self.assertIn("wordPop", html)
        self.assertIn(".headline", html)
        self.assertIn(".stat-counter", html)
        self.assertIn("data-animate", html)
        self.assertIn("shiftOrbPalette", html)
        self.assertIn("updateReveals", html)


if __name__ == "__main__":
    unittest.main()
