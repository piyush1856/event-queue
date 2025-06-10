"""Async Kafka producer implementation for the event queue."""

import asyncio
import json
import logging
from typing import Any, Dict, List, Optional, Union

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError

from .queue_constants import (
    DEFAULT_HASH_FLAG,
    DEFAULT_PRODUCER_CONFIG,
    DEFAULT_SERIALIZATION_FORMAT,
    ERROR_MESSAGES,
)
from .queue_health import messages_produced, producer_lag

logger = logging.getLogger(__name__)

class QueueProducer:
    """Async Kafka producer implementation."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize the producer.

        Args:
            config: Kafka producer configuration
        """
        self.config = config or DEFAULT_PRODUCER_CONFIG
        self.producer: Optional[AIOKafkaProducer] = None
        self._is_initialized = False

    async def initialize(self):
        """Initialize the Kafka producer."""
        try:
            self.producer = AIOKafkaProducer(**self.config)
            await self.producer.start()
            self._is_initialized = True
            logger.info("Kafka producer initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {e}")
            raise

    async def stop(self):
        """Stop the Kafka producer."""
        if self.producer:
            try:
                await self.producer.stop()
                self._is_initialized = False
                logger.info("Kafka producer stopped successfully")
            except Exception as e:
                logger.error(f"Failed to stop Kafka producer: {e}")
                raise

    async def send(
        self,
        topic: str,
        value: Any,
        key: Optional[bytes] = None,
        partition: Optional[int] = None,
        timestamp_ms: Optional[int] = None,
        headers: Optional[List[tuple]] = None,
    ) -> None:
        """Send a message to Kafka.

        Args:
            topic: Kafka topic
            value: Message value
            key: Message key
            partition: Topic partition
            timestamp_ms: Message timestamp
            headers: Message headers
        """
        if not self._is_initialized:
            raise RuntimeError(ERROR_MESSAGES["producer_not_initialized"])

        try:
            if isinstance(value, (dict, list)):
                value = json.dumps(value).encode("utf-8")
            elif isinstance(value, str):
                value = value.encode("utf-8")
            elif not isinstance(value, bytes):
                value = str(value).encode("utf-8")

            await self.producer.send_and_wait(
                topic=topic,
                value=value,
                key=key,
                partition=partition,
                timestamp_ms=timestamp_ms,
                headers=headers,
            )
            messages_produced.inc()
            logger.debug(f"Message sent to topic {topic}")
        except Exception as e:
            logger.error(f"Failed to send message to topic {topic}: {e}")
            raise

class QueueEmitter:
    """Event emitter implementation."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize the event emitter.

        Args:
            config: Kafka producer configuration
        """
        self.kafka_producer = QueueProducer(config)
        self._is_initialized = False

    async def initialize(self):
        """Initialize the event emitter."""
        await self.kafka_producer.initialize()
        self._is_initialized = True

    async def stop(self):
        """Stop the event emitter."""
        await self.kafka_producer.stop()
        self._is_initialized = False

    async def emit(
        self,
        topics: Union[str, List[str]],
        event: Any,
        partition_value: Optional[str] = None,
        event_meta: Optional[Dict[str, Any]] = None,
        serialization_format: str = DEFAULT_SERIALIZATION_FORMAT,
        hash_flag: bool = DEFAULT_HASH_FLAG,
        callback: bool = False,
        headers: Optional[List[tuple]] = None,
    ) -> None:
        """Emit an event to Kafka.

        Args:
            topics: Kafka topic(s)
            event: Event data
            partition_value: Partition value for routing
            event_meta: Event metadata
            serialization_format: Message serialization format
            hash_flag: Whether to use hash-based partitioning
            callback: Whether to use callback
            headers: Message headers
        """
        if not self._is_initialized:
            raise RuntimeError(ERROR_MESSAGES["producer_not_initialized"])

        if isinstance(topics, str):
            topics = [topics]

        event_meta = event_meta or {}
        message = {
            "event": event,
            "meta": event_meta,
        }

        try:
            for topic in topics:
                await self.kafka_producer.send(
                    topic=topic,
                    value=message,
                    headers=headers,
                )
            logger.info(f"Event emitted to topics {topics}")
        except Exception as e:
            logger.error(f"Failed to emit event to topics {topics}: {e}")
            raise

class QueueEmitterWrapper:
    """Wrapper for the event emitter with queue support."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize the event emitter wrapper.

        Args:
            config: Kafka producer configuration
        """
        self.event_emitter = QueueEmitter(config)
        self.event_queue: List[Dict[str, Any]] = []
        producer_lag.set(0)

    async def initialize(self):
        """Initialize the event emitter wrapper."""
        await self.event_emitter.initialize()

    async def stop(self):
        """Stop the event emitter wrapper."""
        await self.event_emitter.stop()

    async def emit(
        self,
        topics: Union[str, List[str]],
        event: Any,
        partition_value: Optional[str] = None,
        event_meta: Optional[Dict[str, Any]] = None,
        serialization_format: str = DEFAULT_SERIALIZATION_FORMAT,
        hash_flag: bool = DEFAULT_HASH_FLAG,
        callback: bool = False,
        headers: Optional[List[tuple]] = None,
    ) -> None:
        """Emit an event to Kafka.

        Args:
            topics: Kafka topic(s)
            event: Event data
            partition_value: Partition value for routing
            event_meta: Event metadata
            serialization_format: Message serialization format
            hash_flag: Whether to use hash-based partitioning
            callback: Whether to use callback
            headers: Message headers
        """
        await self.event_emitter.emit(
            topics=topics,
            event=event,
            partition_value=partition_value,
            event_meta=event_meta,
            serialization_format=serialization_format,
            hash_flag=hash_flag,
            callback=callback,
            headers=headers,
        )

    def add_event_to_queue(
        self,
        *,
        topics: Union[str, List[str]],
        event: Any,
        partition_value: Optional[str] = None,
        event_meta: Optional[Dict[str, Any]] = None,
        serialization_format: str = DEFAULT_SERIALIZATION_FORMAT,
        hash_flag: bool = DEFAULT_HASH_FLAG,
        callback: bool = False,
        headers: Optional[List[tuple]] = None,
    ) -> None:
        """Add an event to the queue for batch processing."""
        self.event_queue.append({
            'topics': topics,
            'event': event,
            'partition_value': partition_value,
            'event_meta': event_meta,
            'serialization_format': serialization_format,
            'hash_flag': hash_flag,
            'callback': callback,
            'headers': headers,
        })
        producer_lag.inc()

    async def emit_events(self) -> None:
        """Emit all events in the queue."""
        for event_data in self.event_queue:
            await self.emit(**event_data)
        self.clear_queue()

    def clear_queue(self) -> None:
        """Clear the event queue."""
        self.event_queue.clear()
        producer_lag.set(0) 