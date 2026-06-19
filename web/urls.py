from django.contrib.auth.views import (
    LoginView,
    LogoutView,
    PasswordResetView,
    PasswordResetDoneView,
    PasswordResetConfirmView,
    PasswordResetCompleteView,
)
from django.urls import path
from .views import (
    home, health, static_debug, upload_paper, pipeline_status, pipeline_result, register,
    api_start_generation, api_status, api_result, serve_video, my_videos, debug_video_files,
    test_r2_storage, analytics_endpoint, analytics_track_click, retry_generation,
    delete_generation, privacy_policy, terms_of_service, review_script, edit_frames, export_mp4, source_figure_image
)

urlpatterns = [
    path("", home, name="home"),
    path("health", health, name="health"),
    path("static-debug/", static_debug, name="static_debug"),
    path("test-r2-storage/", test_r2_storage, name="test_r2_storage"),
    path("debug-video-files/<str:pmid>/", debug_video_files, name="debug_video_files"),
    path("upload/", upload_paper, name="upload_paper"),
    path("my-videos/", my_videos, name="my_videos"),
    path("status/<str:pmid>/", pipeline_status, name="pipeline_status"),
    path("review/<str:pmid>/", review_script, name="review_script"),
    path("frames/<str:pmid>/", edit_frames, name="edit_frames"),
    path("export-mp4/<str:pmid>/", export_mp4, name="export_mp4"),
    path("source-image/<str:pmid>/<int:index>/", source_figure_image, name="source_figure_image"),
    path("result/<str:pmid>/", pipeline_result, name="pipeline_result"),
    path("retry/<str:pmid>/", retry_generation, name="retry_generation"),
    path("delete/<str:pmid>/", delete_generation, name="delete_generation"),
    path("video/<str:pmid>/", serve_video, name="serve_video"),
    path("privacy/", privacy_policy, name="privacy_policy"),
    path("terms/", terms_of_service, name="terms_of_service"),
    path("login/", LoginView.as_view(template_name="registration/login.html"), name="login"),
    path("logout/", LogoutView.as_view(next_page="/"), name="logout"),
    path("register/", register, name="register"),
    path(
        "password-reset/",
        PasswordResetView.as_view(
            template_name="registration/password_reset_form.html",
            email_template_name="registration/password_reset_email.html",
            subject_template_name="registration/password_reset_subject.txt",
            success_url="/password-reset/done/",
        ),
        name="password_reset",
    ),
    path(
        "password-reset/done/",
        PasswordResetDoneView.as_view(template_name="registration/password_reset_done.html"),
        name="password_reset_done",
    ),
    path(
        "password-reset/confirm/<uidb64>/<token>/",
        PasswordResetConfirmView.as_view(
            template_name="registration/password_reset_confirm.html",
            success_url="/password-reset/complete/",
        ),
        name="password_reset_confirm",
    ),
    path(
        "password-reset/complete/",
        PasswordResetCompleteView.as_view(template_name="registration/password_reset_complete.html"),
        name="password_reset_complete",
    ),
    # API endpoints
    path("api/generate/", api_start_generation, name="api_start_generation"),
    path("api/status/<str:paper_id>/", api_status, name="api_status"),
    path("api/result/<str:paper_id>/", api_result, name="api_result"),
    # Analytics endpoint (SHA1 hash of "hidden-hill" = e9ec8bb)
    path("e9ec8bb/", analytics_endpoint, name="analytics_endpoint"),
    path("analytics/track-click/", analytics_track_click, name="analytics_track_click"),
]
