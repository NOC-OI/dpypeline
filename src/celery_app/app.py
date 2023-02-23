"""Celery app creation."""
import celery

from . import celery_config

app = celery.Celery("ETLPipeline")
app.config_from_object(celery_config)
