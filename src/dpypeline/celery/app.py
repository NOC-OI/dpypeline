"""Celery app creation."""
import celery

from . import config

app = celery.Celery()
app.config_from_object(config)
