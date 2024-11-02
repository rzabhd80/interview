#!/bin/bash

# Default settings
ENVIRONMENT=${1:-dev} # Accepts 'dev' or 'prod', defaults to 'dev' if not provided
COMPOSE_FILE="docker-compose.yaml"
PROJECT_NAME="my_project"

# Define paths for each environment
case $ENVIRONMENT in
  dev)
    COMPOSE_FILE="deploy/docker-compose.dev.yaml"
    ;;
  prod)
    COMPOSE_FILE="deploy/docker-compose.prod.yaml"
    ;;
  *)
    echo "Unknown environment. Use 'dev' or 'prod'."
    exit 1
    ;;
esac

# Helper functions for docker-compose commands
start_services() {
  echo "Starting services for ${ENVIRONMENT}..."
  docker-compose -f $COMPOSE_FILE -p $PROJECT_NAME up -d
}

stop_services() {
  echo "Stopping services for ${ENVIRONMENT}..."
  docker-compose -f $COMPOSE_FILE -p $PROJECT_NAME down
}

show_logs() {
  echo "Showing logs for ${ENVIRONMENT}..."
  docker-compose -f $COMPOSE_FILE -p $PROJECT_NAME logs -f
}

cleanup_volumes() {
  echo "Removing all volumes for ${ENVIRONMENT}..."
  docker-compose -f $COMPOSE_FILE -p $PROJECT_NAME down -v
}

# Command dispatcher
case $2 in
  start)
    start_services
    ;;
  stop)
    stop_services
    ;;
  logs)
    show_logs
    ;;
  cleanup)
    cleanup_volumes
    ;;
  *)
    echo "Usage: $0 {dev|prod} {start|stop|logs|cleanup}"
    exit 1
    ;;
esac

