import json
import os
import shutil
import tempfile
from pathlib import Path
from unittest.mock import patch

from django.contrib.auth.models import User
from django.core.files.uploadedfile import SimpleUploadedFile
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

    @override_settings(MEDIA_ROOT=tempfile.gettempdir())
    def test_pipeline_progress_tracks_four_steps(self):
        output_dir = Path(tempfile.gettempdir()) / "PMC4STEP"
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "paper.json").write_text(json.dumps({"title": "T", "full_text": "x"}), encoding="utf-8")
        (output_dir / "script.json").write_text(json.dumps([{"text": "n", "visual_type": "generated", "visual_content": "v"}]), encoding="utf-8")

        from web.views import _get_pipeline_progress

        progress = _get_pipeline_progress(output_dir)
        self.assertEqual(progress["completed_steps"], ["fetch-paper", "generate-script"])
        self.assertEqual(progress["current_step"], "generate-audio")
        self.assertEqual(progress["total_steps"], 4)

        (output_dir / "audio.wav").write_bytes(b"audio")
        (output_dir / "audio_metadata.json").write_text(json.dumps({"scene_boundaries": [{"duration": 5.0}]}), encoding="utf-8")
        progress = _get_pipeline_progress(output_dir)
        self.assertIn("generate-audio", progress["completed_steps"])
        self.assertEqual(progress["current_step"], "generate-presentation")

        (output_dir / "presentation.html").write_text("<html></html>", encoding="utf-8")
        (output_dir / "presentation.json").write_text(json.dumps({"schema_version": 3}), encoding="utf-8")
        progress = _get_pipeline_progress(output_dir)
        self.assertEqual(progress["status"], "completed")
        self.assertEqual(len(progress["completed_steps"]), 4)
        shutil.rmtree(output_dir, ignore_errors=True)

    @override_settings(MEDIA_ROOT=tempfile.gettempdir())
    def test_pipeline_result_renders_presentation_iframe(self):
        self.login()
        self.make_job(paper_id="PMCRESULT", status="completed", task_id="result-task")
        output_dir = Path(tempfile.gettempdir()) / "PMCRESULT"
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "presentation.html").write_text(
            "<!DOCTYPE html><html><head></head><body><section class='scene active'></section></body></html>",
            encoding="utf-8",
        )

        response = self.client.get(reverse("pipeline_result", args=["PMCRESULT"]))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "html-video-frame")
        self.assertContains(response, "Edit Script")
        self.assertNotContains(response, "Edit Frames")
        shutil.rmtree(output_dir, ignore_errors=True)

    def test_ensure_contract_injects_audio_sync(self):
        import sys

        pipeline_dir = str(Path(__file__).resolve().parent.parent / "pipeline")
        if pipeline_dir not in sys.path:
            sys.path.insert(0, pipeline_dir)
        from claude_presentation import PLACEHOLDER_AUDIO_SRC, PLACEHOLDER_SCENE_DURATIONS_JSON, _ensure_contract

        html = _ensure_contract("<!DOCTYPE html><html><head></head><body><section class='scene active'></section></body></html>")
        self.assertIn(PLACEHOLDER_AUDIO_SRC, html)
        self.assertIn(PLACEHOLDER_SCENE_DURATIONS_JSON, html)
        self.assertIn("timeupdate", html)
        self.assertIn("data-infodemica-motion", html)
        self.assertIn("orbDrift", html)
        self.assertIn("[data-reveal]", html)
        self.assertIn("wordPop", html)
        self.assertIn(".headline", html)
        self.assertIn(".stat-counter", html)
        self.assertIn("data-animate", html)
        self.assertIn("shiftOrbPalette", html)
        self.assertIn("updateReveals", html)
        self.assertIn("resetReveals", html)

    @override_settings(MEDIA_ROOT=tempfile.gettempdir())
    def test_export_mp4_downloads_existing_file(self):
        self.login()
        self.make_job(paper_id="PMCMP4", status="completed", task_id="mp4-task")
        output_dir = Path(tempfile.gettempdir()) / "PMCMP4"
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "presentation.mp4").write_bytes(b"fake mp4")

        response = self.client.get(reverse("export_mp4", args=["PMCMP4"]))

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response["Content-Type"], "video/mp4")
        self.assertIn("PMCMP4.mp4", response["Content-Disposition"])
        shutil.rmtree(output_dir, ignore_errors=True)

    @override_settings(MEDIA_ROOT=tempfile.gettempdir())
    def test_review_script_adds_custom_source_table_and_video(self):
        self.login()
        self.make_job(paper_id="PMCSOURCEUPLOAD", status="pending", task_id="source-upload-task")
        output_dir = Path(tempfile.gettempdir()) / "PMCSOURCEUPLOAD"
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "script.json").write_text(
            json.dumps([
                {"text": "Narration.", "visual_type": "generated", "visual_content": "Visual."}
            ]),
            encoding="utf-8",
        )
        (output_dir / "paper.json").write_text(json.dumps({"figures": [], "tables": []}), encoding="utf-8")
        upload = SimpleUploadedFile("clip.mp4", b"fake video", content_type="video/mp4")

        response = self.client.post(
            reverse("review_script", args=["PMCSOURCEUPLOAD"]),
            {
                "action": "add_source",
                "custom_table_label": "Custom Table A",
                "custom_table_caption": "Uploaded table caption.",
                "custom_table_text": "A | B\n1 | 2",
                "custom_video_caption": "Uploaded video caption.",
                "custom_videos": upload,
            },
        )

        self.assertEqual(response.status_code, 302)
        uploads = json.loads((output_dir / "source_uploads.json").read_text(encoding="utf-8"))
        self.assertEqual(uploads["tables"][0]["label"], "Custom Table A")
        self.assertEqual(uploads["videos"][0]["name"], "clip.mp4")

        response = self.client.get(reverse("review_script", args=["PMCSOURCEUPLOAD"]))
        self.assertContains(response, "Custom Table A")
        self.assertContains(response, "Uploaded video caption.")
        self.assertContains(response, "Use source video in this scene")
        shutil.rmtree(output_dir, ignore_errors=True)
