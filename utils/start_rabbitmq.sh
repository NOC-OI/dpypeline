#!/bin/bash

# Enable the service to start at boot time and start the RabbitMQ server:
systemctl enable rabbitmq-server
systemctl start rabbitmq-server
