from pathlib import Path
import os
import sys
import dj_database_url

# ============================================================================
# Environment Variables
# ============================================================================
# 
# This application requires the following environment variables:
#
# REQUIRED:
#   - GEMINI_API_KEY: Google Gemini API key (for TTS)
#   - ANTHROPIC_API_KEY: Anthropic API key (for script + motion HTML generation)
#   - RUNWAYML_API_SECRET: RunwayML API key (optional; not used by HTML pipeline)
#   - VIDEO_ACCESS_CODE: Access code required to generate videos (security)
#   - SECRET_KEY: Django secret key (generate with get_random_secret_key())
#
# OPTIONAL (with defaults):
#   - DEBUG: Set to "True" for development, "False" for production
#   - DATABASE_URL: PostgreSQL connection string (defaults to SQLite locally)
#   - DATABASE_SSL: Set to "True" if database requires SSL
#   - ALLOWED_HOSTS: Comma-separated list of allowed hostnames
#   - CSRF_TRUSTED_ORIGINS: Comma-separated list of trusted origins
#   - CELERY_BROKER_URL: Redis connection URL (required for Celery task queue)
#     Example: redis://localhost:6379/0 (local) or redis://user:pass@host:port/db (production)
#
# For local development, create a .env file (see .env.example)
# For production, set these in your deployment platform (Railway, etc.)
#
# ============================================================================

def _csv(name, default=""):
    return [v.strip() for v in os.getenv(name, default).split(",") if v.strip()]

ALLOWED_HOSTS = _csv("ALLOWED_HOSTS", ".up.railway.app,localhost,127.0.0.1")
CSRF_TRUSTED_ORIGINS = _csv("CSRF_TRUSTED_ORIGINS", "https://*.up.railway.app")
SECURE_PROXY_SSL_HEADER = ("HTTP_X_FORWARDED_PROTO", "https")

BASE_DIR = Path(__file__).resolve().parent.parent

# Load environment variables from .env file for local development
# Do this AFTER BASE_DIR is defined so we can use it
try:
    from dotenv import load_dotenv
    # Explicitly load .env from project root
    env_path = BASE_DIR / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
except ImportError:
    pass  # python-dotenv not installed, skip (not needed on Railway)

# ============================================================================
# Security & Secrets
# ============================================================================

# Debug Mode
# Default to True for local development, False for production
# Can be overridden with DEBUG environment variable
DEBUG = os.getenv("DEBUG", "True") == "True"

# Django Secret Key
# Generate with: python -c "from django.core.management.utils import get_random_secret_key; print(get_random_secret_key())"
# IMPORTANT: Use a strong, unique key in production!
SECRET_KEY = os.getenv("SECRET_KEY", "dev-secret-not-for-production")

# Warn if using default secret key in production
if not DEBUG and SECRET_KEY == "dev-secret-not-for-production":
    import warnings
    warnings.warn(
        "SECRET_KEY is not set! Using default dev secret. "
        "This is INSECURE for production. Set SECRET_KEY environment variable.",
        UserWarning
    )

# Video Generation Access Code
# Required code that users must provide to generate videos via web UI or API
# Prevents unauthorized usage of expensive API calls (Gemini, RunwayML)
# Set via VIDEO_ACCESS_CODE environment variable in .env file or environment
# REQUIRED: Must be set for security. If not set, video generation will fail with error.
# Generate a strong code: python -c "import secrets; print(secrets.token_urlsafe(32))"
VIDEO_ACCESS_CODE = os.getenv("VIDEO_ACCESS_CODE", None)

# Simulation Mode
# When enabled, video generation tasks will simulate progress instead of running the actual pipeline
# This is useful for testing the status update system without incurring API costs
# Set SIMULATION_MODE=True in your .env file or environment to enable
SIMULATION_MODE = os.getenv("SIMULATION_MODE", "False") == "True"

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "web",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

# Only use WhiteNoise in production (DEBUG=False)
# In development, Django's staticfiles app handles static files automatically
if not DEBUG:
    MIDDLEWARE.insert(1, "whitenoise.middleware.WhiteNoiseMiddleware")

ROOT_URLCONF = "config.urls"
TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [BASE_DIR / "web" / "templates"],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]
WSGI_APPLICATION = "config.wsgi.application"

DATABASES = {
    "default": dj_database_url.parse(
        os.getenv("DATABASE_URL", f"sqlite:///{BASE_DIR / 'db.sqlite3'}"),
        conn_max_age=600,
        ssl_require=os.getenv("DATABASE_SSL", "False") == "True",
    )
}

STATIC_URL = "/static/"
STATIC_ROOT = BASE_DIR / "staticfiles"
# Use CompressedStaticFilesStorage (not Manifest) for simpler deployment
# Only use WhiteNoise storage in production; in development, use default storage
if not DEBUG:
    STATICFILES_STORAGE = "whitenoise.storage.CompressedStaticFilesStorage"

# Media files for generated outputs (videos, audio, metadata)
# Cloud Storage Configuration (Cloudflare R2)
USE_CLOUD_STORAGE = os.getenv("USE_CLOUD_STORAGE", "False") == "True"

if USE_CLOUD_STORAGE:
    # Cloudflare R2 (S3-compatible)
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    AWS_STORAGE_BUCKET_NAME = os.getenv("AWS_STORAGE_BUCKET_NAME")
    AWS_S3_ENDPOINT_URL = os.getenv("AWS_S3_ENDPOINT_URL")  # R2 endpoint
    AWS_S3_REGION_NAME = "auto"  # R2 uses "auto"
    
    # Security & performance
    AWS_S3_FILE_OVERWRITE = False
    AWS_DEFAULT_ACL = "public-read"  # Videos are public (or use "private" for signed URLs)
    AWS_S3_OBJECT_PARAMETERS = {
        "CacheControl": "max-age=86400",  # 1 day cache
    }
    
    # Storage settings - Use STORAGES dict for Django 4.2+ (also works with DEFAULT_FILE_STORAGE for backward compat)
    STORAGES = {
        "default": {
            "BACKEND": "storages.backends.s3boto3.S3Boto3Storage",
        },
        "staticfiles": {
            "BACKEND": "django.contrib.staticfiles.storage.StaticFilesStorage",
        },
    }
    # Also set DEFAULT_FILE_STORAGE for backward compatibility
    DEFAULT_FILE_STORAGE = "storages.backends.s3boto3.S3Boto3Storage"
    
    # Media files will be stored in R2
    # django-storages will handle URL construction automatically based on AWS_S3_ENDPOINT_URL
    # For public access, R2 uses: https://<bucket-name>.<account-id>.r2.cloudflarestorage.com
    # But django-storages constructs URLs automatically, so we don't need to set MEDIA_URL manually
    
    # MEDIA_ROOT is not used when using cloud storage, but keep it for compatibility
    MEDIA_ROOT = BASE_DIR / "media"
else:
    # Fallback to local storage (for development)
    MEDIA_URL = "/media/"
    MEDIA_ROOT = BASE_DIR / "media"

# Authentication settings
LOGIN_URL = "/login/"
LOGIN_REDIRECT_URL = "/"
LOGOUT_REDIRECT_URL = "/"

# Password validation
AUTH_PASSWORD_VALIDATORS = [
    {
        "NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.MinimumLengthValidator",
        "OPTIONS": {
            "min_length": 8,
        },
    },
    {
        "NAME": "django.contrib.auth.password_validation.CommonPasswordValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.NumericPasswordValidator",
    },
]

# ============================================================================
# Celery Configuration
# ============================================================================

# Celery broker URL (Redis or RabbitMQ)
# Railway provides Redis connection via REDIS_URL or REDISCLOUD_URL
# For local development: redis://localhost:6379/0
# Priority: CELERY_BROKER_URL > REDIS_URL > REDISCLOUD_URL > default
CELERY_BROKER_URL = (
    os.getenv("CELERY_BROKER_URL") or
    os.getenv("REDIS_URL") or
    os.getenv("REDISCLOUD_URL") or
    "redis://localhost:6379/0"
)

# Celery result backend (optional, for storing task results)
CELERY_RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND", CELERY_BROKER_URL)

# Celery task serialization
CELERY_ACCEPT_CONTENT = ["json"]
CELERY_TASK_SERIALIZER = "json"
CELERY_RESULT_SERIALIZER = "json"

# Celery timezone
CELERY_TIMEZONE = "UTC"

# Celery task settings
CELERY_TASK_TRACK_STARTED = True
CELERY_TASK_TIME_LIMIT = 30 * 60  # 30 minutes hard limit
CELERY_TASK_SOFT_TIME_LIMIT = 25 * 60  # 25 minutes soft limit (raises exception)
CELERY_WORKER_SEND_TASK_EVENTS = True
CELERY_TASK_SEND_SENT_EVENT = True

# Celery worker pool (use 'solo' for Windows, 'prefork' for Linux)
# Windows doesn't support prefork pool properly, so use solo pool
if sys.platform == "win32":
    CELERY_WORKER_POOL = "solo"
else:
    CELERY_WORKER_POOL = "prefork"

# Celery worker concurrency (number of worker processes)
# Limit to 2 for Railway to avoid resource exhaustion
# Can be overridden with --concurrency flag in Procfile
CELERY_WORKER_CONCURRENCY = int(os.getenv("CELERY_WORKER_CONCURRENCY", "2"))

# Max tasks per child worker (helps prevent memory leaks)
CELERY_WORKER_MAX_TASKS_PER_CHILD = int(os.getenv("CELERY_WORKER_MAX_TASKS_PER_CHILD", "50"))

# ============================================================================
# Security Settings (Production)
# ============================================================================

# Security headers (only in production)
if not DEBUG:
    SECURE_SSL_REDIRECT = False  # Railway handles SSL termination
    SESSION_COOKIE_SECURE = True
    CSRF_COOKIE_SECURE = True
    SECURE_BROWSER_XSS_FILTER = True
    SECURE_CONTENT_TYPE_NOSNIFF = True
    X_FRAME_OPTIONS = "DENY"
    SECURE_HSTS_SECONDS = 31536000  # 1 year
    SECURE_HSTS_INCLUDE_SUBDOMAINS = True
    SECURE_HSTS_PRELOAD = True

# Internationalization
LANGUAGE_CODE = "en-us"
TIME_ZONE = "UTC"
USE_I18N = True
USE_TZ = True

# Default primary key field type
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"