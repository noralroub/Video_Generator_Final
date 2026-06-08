import json
import shutil
import tempfile
from pathlib import Path
from unittest.mock import patch

from django.contrib.auth.models import User
from django.test import TestCase, override_settings
from django.urls import reverse

from web.models import VideoGenerationJob


class ShippingControlsTests(TestCase):
    def setUp(self):
        self.media_root = Path(tempfile.mkdtemp(prefix="infodemica-test-media-"))
        self.user = User.objects.create_user(
            username="shipper",
            email="shipper@example.com",
            password="complex-pass-123",
        )

    def tearDown(self):
        shutil.rmtree(self.media_root, ignore_errors=True)

    def login(self):
        self.client.login(username="shipper", password="complex-pass-123")

    def make_job(self, paper_id="PMC123", status="failed", task_id="task-1"):
        return VideoGenerationJob.objects.create(
            user=self.user,
            paper_id=paper_id,
            status=status,
            progress_percent=0,
            task_id=task_id,
        )

    def test_registration_requires_email(self):
        response = self.client.post(
            reverse("register"),
            {
                "username": "new-user",
                "password1": "complex-pass-123",
                "password2": "complex-pass-123",
            },
        )

        self.assertContains(response, "This field is required")
        self.assertFalse(User.objects.filter(username="new-user").exists())

    @override_settings(REQUIRE_VIDEO_ACCESS_CODE=False, MEDIA_ROOT=tempfile.gettempdir())
    def test_upload_can_start_without_access_code_when_disabled(self):
        self.login()

        with patch("web.views._start_pipeline_async") as start_pipeline:
            response = self.client.post(
                reverse("upload_paper"),
                {"paper_id": "PMC10979640", "access_code": ""},
            )

        self.assertEqual(response.status_code, 302)
        self.assertIn("/status/PMC10979640/", response["Location"])
        start_pipeline.assert_called_once()

    @override_settings(REQUIRE_VIDEO_ACCESS_CODE=False, GENERATION_DAILY_LIMIT=1)
    def test_upload_blocks_when_daily_limit_reached(self):
        self.login()
        self.make_job(paper_id="PMC111", status="completed", task_id="limit-task")

        with patch("web.views._start_pipeline_async") as start_pipeline:
            response = self.client.post(
                reverse("upload_paper"),
                {"paper_id": "PMC222", "access_code": ""},
            )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "reached today")
        start_pipeline.assert_not_called()

    @override_settings(MEDIA_ROOT=tempfile.gettempdir(), USE_CLOUD_STORAGE=False)
    def test_delete_generation_removes_owned_job_and_local_artifacts(self):
        self.login()
        self.make_job(paper_id="PMCDELETE", status="completed", task_id="delete-task")
        output_dir = Path(tempfile.gettempdir()) / "PMCDELETE"
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "presentation.html").write_text("<html></html>", encoding="utf-8")

        response = self.client.post(reverse("delete_generation", args=["PMCDELETE"]))

        self.assertEqual(response.status_code, 302)
        self.assertFalse(VideoGenerationJob.objects.filter(paper_id="PMCDELETE").exists())
        self.assertFalse(output_dir.exists())

    @override_settings(MEDIA_ROOT=tempfile.gettempdir())
    def test_retry_generation_requeues_owned_failed_job(self):
        self.login()
        self.make_job(paper_id="PMCRETRY", status="failed", task_id="retry-task")

        with patch("web.views._start_pipeline_async") as start_pipeline:
            response = self.client.post(reverse("retry_generation", args=["PMCRETRY"]))

        self.assertEqual(response.status_code, 302)
        self.assertIn("/status/PMCRETRY/", response["Location"])
        start_pipeline.assert_called_once()

    def test_legal_pages_render(self):
        self.assertEqual(self.client.get(reverse("privacy_policy")).status_code, 200)
        self.assertEqual(self.client.get(reverse("terms_of_service")).status_code, 200)

    @override_settings(MEDIA_ROOT=tempfile.gettempdir())
    def test_review_script_page_renders_for_owner(self):
        self.login()
        self.make_job(paper_id="PMCREVIEW", status="pending", task_id="review-task")
        output_dir = Path(tempfile.gettempdir()) / "PMCREVIEW"
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "script.json").write_text(
            json.dumps([
                {
                    "text": "Original narration.",
                    "visual_type": "generated",
                    "visual_content": "Original visual.",
                }
            ]),
            encoding="utf-8",
        )
        (output_dir / "paper.json").write_text(
            json.dumps(
                {
                    "figures": [
                        {
                            "id": "fig1",
                            "caption": "A source figure caption.",
                            "url": "https://example.com/fig1.png",
                        }
                    ],
                    "tables": [
                        {
                            "label": "Table 1",
                            "caption": "A source table caption.",
                            "text": "Column A | Column B\n1 | 2",
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )

        response = self.client.get(reverse("review_script", args=["PMCREVIEW"]))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Original narration.")
        self.assertContains(response, "Source snippets")
        self.assertContains(response, "A source table caption.")
        self.assertContains(response, "A source figure caption.")
        self.assertContains(response, "Use source table in this scene")
        self.assertContains(response, "Use source image in this scene")
        shutil.rmtree(output_dir, ignore_errors=True)

    @override_settings(MEDIA_ROOT=tempfile.gettempdir())
    def test_review_script_extracts_table_caption_from_xml_when_json_lacks_tables(self):
        self.login()
        self.make_job(paper_id="PMCXMLTABLE", status="pending", task_id="xml-table-task")
        output_dir = Path(tempfile.gettempdir()) / "PMCXMLTABLE"
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "script.json").write_text(
            json.dumps([
                {
                    "text": "Narration.",
                    "visual_type": "generated",
                    "visual_content": "Visual.",
                }
            ]),
            encoding="utf-8",
        )
        (output_dir / "paper.json").write_text(json.dumps({"figures": [], "tables": []}), encoding="utf-8")
        (output_dir / "paper.xml").write_text(
            """
            <article>
              <table-wrap id="t1">
                <label>Table 1</label>
                <caption><p>Important extracted table caption.</p></caption>
              </table-wrap>
            </article>
            """,
            encoding="utf-8",
        )

        response = self.client.get(reverse("review_script", args=["PMCXMLTABLE"]))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Important extracted table caption.")
        shutil.rmtree(output_dir, ignore_errors=True)

    @override_settings(MEDIA_ROOT=tempfile.gettempdir())
    def test_review_script_saves_edits_and_resumes_pipeline(self):
        self.login()
        self.make_job(paper_id="PMCEDIT", status="pending", task_id="edit-task")
        output_dir = Path(tempfile.gettempdir()) / "PMCEDIT"
        frames_dir = output_dir / "frames"
        frames_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "script.json").write_text(
            json.dumps([
                {
                    "text": "Draft narration.",
                    "visual_type": "generated",
                    "visual_content": "Draft visual.",
                }
            ]),
            encoding="utf-8",
        )
        (output_dir / "audio.wav").write_bytes(b"stale audio")
        (output_dir / "audio_metadata.json").write_text("{}", encoding="utf-8")
        (output_dir / "presentation.html").write_text("<html></html>", encoding="utf-8")
        (frames_dir / "scene_00.html").write_text("<section></section>", encoding="utf-8")

        with patch("web.views._start_pipeline_async") as start_pipeline:
            response = self.client.post(
                reverse("review_script", args=["PMCEDIT"]),
                {
                    "scene_0_text": "Edited narration.",
                    "scene_0_visual": "Edited visual direction.",
                },
            )

        self.assertEqual(response.status_code, 302)
        self.assertIn("/status/PMCEDIT/", response["Location"])
        start_pipeline.assert_called_once()
        self.assertFalse(start_pipeline.call_args.kwargs["review_required"])
        saved = json.loads((output_dir / "script.json").read_text(encoding="utf-8"))
        self.assertEqual(saved[0]["text"], "Edited narration.")
        self.assertEqual(saved[0]["visual_content"], "Edited visual direction.")
        self.assertIsNone(saved[0]["source_table"])
        self.assertIsNone(saved[0]["source_figure"])
        self.assertFalse((output_dir / "audio.wav").exists())
        self.assertFalse((output_dir / "presentation.html").exists())
        self.assertFalse(frames_dir.exists())
        shutil.rmtree(output_dir, ignore_errors=True)

    @override_settings(MEDIA_ROOT=tempfile.gettempdir())
    def test_review_script_saves_selected_source_table_for_scene(self):
        self.login()
        self.make_job(paper_id="PMCTABLESELECT", status="pending", task_id="table-select-task")
        output_dir = Path(tempfile.gettempdir()) / "PMCTABLESELECT"
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "script.json").write_text(
            json.dumps([
                {
                    "text": "Draft narration.",
                    "visual_type": "generated",
                    "visual_content": "Draft visual.",
                }
            ]),
            encoding="utf-8",
        )
        (output_dir / "paper.json").write_text(
            json.dumps(
                {
                    "figures": [],
                    "tables": [
                        {
                            "id": "t1",
                            "label": "Table 1",
                            "caption": "Important table.",
                            "text": "A | B\n1 | 2",
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )

        with patch("web.views._start_pipeline_async"):
            response = self.client.post(
                reverse("review_script", args=["PMCTABLESELECT"]),
                {
                    "scene_0_text": "Edited narration.",
                    "scene_0_visual": "Render the key results.",
                    "scene_0_source_table": "0",
                },
            )

        self.assertEqual(response.status_code, 302)
        saved = json.loads((output_dir / "script.json").read_text(encoding="utf-8"))
        self.assertEqual(saved[0]["source_table"]["label"], "Table 1")
        self.assertIn("Use source table Table 1", saved[0]["visual_content"])
        shutil.rmtree(output_dir, ignore_errors=True)

    @override_settings(MEDIA_ROOT=tempfile.gettempdir())
    def test_review_script_saves_selected_source_figure_for_scene(self):
        self.login()
        self.make_job(paper_id="PMCFIGURESELECT", status="pending", task_id="figure-select-task")
        output_dir = Path(tempfile.gettempdir()) / "PMCFIGURESELECT"
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "script.json").write_text(
            json.dumps([
                {
                    "text": "Draft narration.",
                    "visual_type": "generated",
                    "visual_content": "Draft visual.",
                }
            ]),
            encoding="utf-8",
        )
        (output_dir / "paper.json").write_text(
            json.dumps(
                {
                    "figures": [
                        {
                            "id": "F1",
                            "caption": "Important source image.",
                            "url": "https://example.com/figure.png",
                        }
                    ],
                    "tables": [],
                }
            ),
            encoding="utf-8",
        )

        with patch("web.views._start_pipeline_async"):
            response = self.client.post(
                reverse("review_script", args=["PMCFIGURESELECT"]),
                {
                    "scene_0_text": "Edited narration.",
                    "scene_0_visual": "Render the source network image.",
                    "scene_0_source_figure": "0",
                },
            )

        self.assertEqual(response.status_code, 302)
        saved = json.loads((output_dir / "script.json").read_text(encoding="utf-8"))
        self.assertEqual(saved[0]["source_figure"]["id"], "F1")
        self.assertIn("Use source image F1", saved[0]["visual_content"])
        shutil.rmtree(output_dir, ignore_errors=True)
