"""Tests for presentation payload timing."""

import json
import sys
import tempfile
import unittest
from pathlib import Path

PIPELINE_DIR = Path(__file__).resolve().parent.parent / "pipeline"
if str(PIPELINE_DIR) not in sys.path:
    sys.path.insert(0, str(PIPELINE_DIR))

from claude_presentation import _build_scenes_payload  # noqa: E402
from scenes import Scene, save_scenes  # noqa: E402


class PresentationPayloadTests(unittest.TestCase):
    def test_build_scenes_payload_adds_enter_ms(self):
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            script_path = output_dir / "script.json"
            save_scenes(
                [
                    Scene(
                        text="First part. Second part with 42 percent.",
                        visual_type="generated",
                        visual_content="Show result.",
                        display_phrases=[
                            {"text": "First part.", "emphasis": []},
                            {"text": "Second part with 42 percent.", "emphasis": ["42 percent"]},
                        ],
                        visual_layout="stat_counter",
                        chart_data={"value": 42},
                    )
                ],
                script_path,
            )
            (output_dir / "audio_metadata.json").write_text(
                json.dumps(
                    {
                        "scene_boundaries": [
                            {"duration": 5.0, "clip_duration": 5.0},
                        ]
                    }
                ),
                encoding="utf-8",
            )

            payload = _build_scenes_payload(output_dir, script_path, [5.0])
            self.assertEqual(len(payload), 1)
            phrases = payload[0]["display_phrases"]
            self.assertEqual(phrases[0]["enter_ms"], 0)
            self.assertGreater(phrases[1]["enter_ms"], 0)
            self.assertEqual(payload[0]["visual_layout"], "stat_counter")


if __name__ == "__main__":
    unittest.main()
