#!/bin/bash
python -m celery -A main flower --port=5555
