"""Constants and default configurations for the event queue."""

import logging
from typing import Dict, Any

# Default serialization format
DEFAULT_SERIALIZATION_FORMAT = "json"

# Default hash flag
DEFAULT_HASH_FLAG = False

# Default producer configuration
DEFAULT_PRODUCER_CONFIG: Dict[str, Any] = {
    "bootstrap_servers": "localhost:9092",
    "enable_idempotence": True,
    "acks": "all",
}

# Default consumer configuration
DEFAULT_CONSUMER_CONFIG: Dict[str, Any] = {
    "bootstrap_servers": "localhost:9092",
    "session_timeout_ms": 20000,
    "auto_offset_reset": "latest",
    "enable_auto_commit": True,
    "auto_commit_interval_ms": 5000,
}

# Logging configuration
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
LOG_LEVEL = logging.INFO

# Health check configuration
HEALTH_CHECK_INTERVAL = 30  # seconds
READINESS_CHECK_INTERVAL = 30  # seconds

# Metrics configuration
METRICS_PORT = 9090
METRICS_PREFIX = "event_queue_"

# Error messages
ERROR_MESSAGES = {
    "producer_not_initialized": "Kafka producer not initialized",
    "consumer_not_initialized": "Kafka consumer not initialized",
    "invalid_configuration": "Invalid configuration provided",
    "serialization_error": "Error serializing message",
    "deserialization_error": "Error deserializing message",
    "producer_error": "Error producing message to Kafka",
    "consumer_error": "Error consuming message from Kafka",
} 