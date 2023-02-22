#!/bin/bash

N_POOL_PROCESSES=2

# Enable the service to start at boot time and start the RabbitMQ server:
python -m celery -A main worker --concurrency=${N_POOL_PROCESSES} --loglevel=INFO -n ETLPipeline@%h
