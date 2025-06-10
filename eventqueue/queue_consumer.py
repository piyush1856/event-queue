"""Async Kafka consumer implementation for the event queue."""

import asyncio
import json
import logging
from typing import Any, Callable, Dict, List, Optional, Set, Union

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError

from .queue_constants import (
    DEFAULT_CONSUMER_CONFIG,
    DEFAULT_SERIALIZATION_FORMAT,
    ERROR_MESSAGES,
)
from .queue_health import consumer_lag, messages_consumed

logger = logging.getLogger(__name__)

class QueueConsumer:
    """Async Kafka consumer implementation."""

    def __init__(
        self,
        topics: Union[str, List[str]],
        config: Optional[Dict[str, Any]] = None,
        tasks: Optional[List[Callable]] = None,
    ):
        """Initialize the consumer.

        Args:
            topics: Kafka topic(s) to consume from
            config: Kafka consumer configuration
            tasks: List of task functions to process messages
        """
        self.topics = [topics] if isinstance(topics, str) else topics
        self.config = config or DEFAULT_CONSUMER_CONFIG
        self.tasks = tasks or []
        self.consumer: Optional[AIOKafkaConsumer] = None
        self._is_initialized = False
        self._is_running = False
        self._consumer_task: Optional[asyncio.Task] = None

    async def initialize(self):
        """Initialize the Kafka consumer."""
        try:
            self.consumer = AIOKafkaConsumer(
                *self.topics,
                **self.config,
            )
            await self.consumer.start()
            self._is_initialized = True
            logger.info(f"Kafka consumer initialized for topics {self.topics}")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka consumer: {e}")
            raise

    async def stop(self):
        """Stop the Kafka consumer."""
        self._is_running = False
        if self._consumer_task:
            self._consumer_task.cancel()
            try:
                await self._consumer_task
            except asyncio.CancelledError:
                pass

        if self.consumer:
            try:
                await self.consumer.stop()
                self._is_initialized = False
                logger.info("Kafka consumer stopped successfully")
            except Exception as e:
                logger.error(f"Failed to stop Kafka consumer: {e}")
                raise

    async def start(self):
        """Start consuming messages."""
        if not self._is_initialized:
            raise RuntimeError(ERROR_MESSAGES["consumer_not_initialized"])

        self._is_running = True
        self._consumer_task = asyncio.create_task(self._consume_messages())

    async def _consume_messages(self):
        """Consume messages from Kafka topics."""
        try:
            async for message in self.consumer:
                if not self._is_running:
                    break

                try:
                    await self._process_message(message)
                except Exception as e:
                    logger.error(f"Failed to process message: {e}")
                    # Continue with next message
        except Exception as e:
            logger.error(f"Error consuming messages: {e}")
            self._is_running = False
            raise

    async def _process_message(self, message: Any):
        """Process a consumed message.

        Args:
            message: Kafka message
        """
        try:
            value = message.value
            if isinstance(value, bytes):
                value = json.loads(value.decode("utf-8"))

            for task in self.tasks:
                try:
                    if asyncio.iscoroutinefunction(task):
                        await task(value)
                    else:
                        task(value)
                except Exception as e:
                    logger.error(f"Task failed to process message: {e}")
                    # Continue with next task

            messages_consumed.inc()
            logger.debug(f"Message processed from topic {message.topic}")
        except Exception as e:
            logger.error(f"Failed to process message: {e}")
            raise

async def setup_and_start_consumer(config: Dict[str, Any]) -> QueueConsumer:
    """Set up and start a Kafka consumer.

    Args:
        config: Consumer configuration

    Returns:
        QueueConsumer: Initialized and started consumer
    """
    try:
        topics_config = config.get("topics_configurations", {})
        topics = list(topics_config.keys())
        tasks = []
        for topic_config in topics_config.values():
            tasks.extend(topic_config.get("tasks", []))

        consumer = QueueConsumer(
            topics=topics,
            config=config.get("consumer_config"),
            tasks=tasks,
        )
        await consumer.initialize()
        await consumer.start()
        return consumer
    except Exception as e:
        logger.error(f"Failed to set up consumer: {e}")
        raise 