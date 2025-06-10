"""Example producer implementation using event queue."""

import asyncio
import logging
from typing import Dict, Any

from eventqueue.queue_emitter import QueueEmitterWrapper
from eventqueue.queue_constants import DEFAULT_PRODUCER_CONFIG

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Example configuration
PRODUCER_CONFIG = {
    **DEFAULT_PRODUCER_CONFIG,
    "bootstrap_servers": "localhost:9092",
}

# Example topics
TOPICS = ["test-topic-1", "test-topic-2"]

async def produce_messages(emitter: QueueEmitterWrapper, num_messages: int = 5):
    """Produce example messages.

    Args:
        emitter: Queue emitter instance
        num_messages: Number of messages to produce
    """
    for i in range(num_messages):
        # Example event data
        event_data = {
            "message_id": i,
            "content": f"Test message {i}",
            "timestamp": asyncio.get_event_loop().time(),
        }

        # Example metadata
        event_meta = {
            "source": "example-producer",
            "version": "1.0",
        }

        try:
            # Emit event directly
            await emitter.emit(
                topics=TOPICS,
                event=event_data,
                event_meta=event_meta,
                partition_value=str(i % 3),  # Example partition key
            )
            logger.info(f"Emitted message {i} directly")

            # Add event to queue
            emitter.add_event_to_queue(
                topics=TOPICS,
                event=event_data,
                event_meta=event_meta,
                partition_value=str(i % 3),
            )
            logger.info(f"Added message {i} to queue")
        except Exception as e:
            logger.error(f"Failed to emit message {i}: {e}")

    # Emit queued events
    try:
        await emitter.emit_events()
        logger.info("Emitted all queued events")
    except Exception as e:
        logger.error(f"Failed to emit queued events: {e}")

async def main():
    """Main function."""
    try:
        # Initialize emitter
        emitter = QueueEmitterWrapper(PRODUCER_CONFIG)
        await emitter.initialize()
        logger.info("Emitter initialized")

        # Produce messages
        await produce_messages(emitter)

        # Stop emitter
        await emitter.stop()
        logger.info("Emitter stopped")
    except Exception as e:
        logger.error(f"Error in main: {e}")

if __name__ == "__main__":
    asyncio.run(main())
