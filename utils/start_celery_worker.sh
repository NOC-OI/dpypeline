#!/bin/bash

N_POOL_PROCESSES=1
LOGLEVEL=INFO
NODE_NAME=ETLPipeline

# Enable the service to start at boot time and start the RabbitMQ server:
python -m celery -A main worker --concurrency=${N_POOL_PROCESSES} --loglevel=${LOGLEVEL} -n ${NODE_NAME}@%h
