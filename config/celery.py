"""
Celery configuration for Infodemica video generation tasks.

This module sets up Celery for asynchronous task processing.
Tasks are stored in Redis (or RabbitMQ) broker and survive server restarts.
"""

import os
import warnings
from celery import Celery

# Set the default Django settings module for the 'celery' program.
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

# Create Celery app instance
app = Celery("hidden_hill")

# Load configuration from Django settings
# Using a string here means the worker doesn't have to serialize
# the configuration object to child processes.
app.config_from_object("django.conf:settings", namespace="CELERY")

# Auto-discover tasks from all registered Django apps
app.autodiscover_tasks()

# Suppress the superuser warning in containerized environments (Railway, Docker)
# In containerized environments, processes are already isolated, so running as root
# is less of a security concern. The --uid/--gid flags in Procfile will handle
# dropping privileges when possible.
warnings.filterwarnings("ignore", category=UserWarning, module="celery.platforms")


@app.task(bind=True, ignore_result=True)
def debug_task(self):
    """Debug task to test Celery setup."""
    print(f"Request: {self.request!r}")

