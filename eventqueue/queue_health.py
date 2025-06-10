"""Health check implementation for the event queue."""

import asyncio
import logging
from typing import Optional

from prometheus_client import Counter, Gauge, start_http_server

from .queue_constants import (
    HEALTH_CHECK_INTERVAL,
    METRICS_PORT,
    METRICS_PREFIX,
    READINESS_CHECK_INTERVAL,
)

logger = logging.getLogger(__name__)

# Metrics
messages_produced = Counter(
    f"{METRICS_PREFIX}messages_produced_total",
    "Total number of messages produced",
)
messages_consumed = Counter(
    f"{METRICS_PREFIX}messages_consumed_total",
    "Total number of messages consumed",
)
producer_lag = Gauge(
    f"{METRICS_PREFIX}producer_lag",
    "Number of messages waiting to be produced",
)
consumer_lag = Gauge(
    f"{METRICS_PREFIX}consumer_lag",
    "Number of messages waiting to be consumed",
)

class HealthCheck:
    """Health check implementation for the event queue."""

    def __init__(self):
        """Initialize health check."""
        self._is_healthy = True
        self._is_ready = False
        self._health_check_task: Optional[asyncio.Task] = None
        self._ready_check_task: Optional[asyncio.Task] = None
        self._metrics_server_task: Optional[asyncio.Task] = None

    async def start(self):
        """Start health check tasks."""
        self._health_check_task = asyncio.create_task(self._health_check())
        self._ready_check_task = asyncio.create_task(self._ready_check())
        self._metrics_server_task = asyncio.create_task(self._start_metrics_server())

    async def stop(self):
        """Stop health check tasks."""
        if self._health_check_task:
            self._health_check_task.cancel()
        if self._ready_check_task:
            self._ready_check_task.cancel()
        if self._metrics_server_task:
            self._metrics_server_task.cancel()

    async def _health_check(self):
        """Run health check periodically."""
        while True:
            try:
                # Add your health check logic here
                self._is_healthy = True
                await asyncio.sleep(HEALTH_CHECK_INTERVAL)
            except Exception as e:
                logger.error(f"Health check failed: {e}")
                self._is_healthy = False

    async def _ready_check(self):
        """Run readiness check periodically."""
        while True:
            try:
                # Add your readiness check logic here
                self._is_ready = True
                await asyncio.sleep(READINESS_CHECK_INTERVAL)
            except Exception as e:
                logger.error(f"Readiness check failed: {e}")
                self._is_ready = False

    async def _start_metrics_server(self):
        """Start Prometheus metrics server."""
        try:
            start_http_server(METRICS_PORT)
            logger.info(f"Metrics server started on port {METRICS_PORT}")
        except Exception as e:
            logger.error(f"Failed to start metrics server: {e}")

    @property
    def is_healthy(self) -> bool:
        """Check if the service is healthy."""
        return self._is_healthy

    @property
    def is_ready(self) -> bool:
        """Check if the service is ready."""
        return self._is_ready

# Global health check instance
health_check = HealthCheck()

async def _healthz():
    """Health check endpoint for Kubernetes."""
    while True:
        if not health_check.is_healthy:
            logger.error("Health check failed")
        await asyncio.sleep(HEALTH_CHECK_INTERVAL)

async def _readyz():
    """Readiness check endpoint for Kubernetes."""
    while True:
        if not health_check.is_ready:
            logger.error("Readiness check failed")
        await asyncio.sleep(READINESS_CHECK_INTERVAL) 