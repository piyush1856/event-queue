"""Example consumer implementation using event queue."""

import asyncio
import logging
from typing import Dict, Any

from eventqueue.queue_consumer import setup_and_start_consumer
from eventqueue.queue_constants import DEFAULT_CONSUMER_CONFIG

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Example message processing function
async def process_message(message: Dict[str, Any]):
    """Process received message.

    Args:
        message: Received message
    """
    try:
        logger.info(f"Processing message: {message}")
        # Add your message processing logic here
        await asyncio.sleep(0.1)  # Simulate processing time
    except Exception as e:
        logger.error(f"Error processing message: {e}")

# Consumer configuration
CONSUMER_CONFIG = {
    "consumer_config": {
        **DEFAULT_CONSUMER_CONFIG,
        "bootstrap_servers": "localhost:9092",
        "group_id": "example-group",
    },
    "topics_configurations": {
        "test-topic-1": {
            "tasks": [process_message],
        },
        "test-topic-2": {
            "tasks": [process_message],
        },
    },
}

async def main():
    """Main function."""
    try:
        # Set up and start consumer
        consumer = await setup_and_start_consumer(CONSUMER_CONFIG)
        logger.info("Consumer started")

        # Keep the consumer running
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info("Stopping consumer...")
        await consumer.stop()
        logger.info("Consumer stopped")
    except Exception as e:
        logger.error(f"Error in main: {e}")

if __name__ == "__main__":
    asyncio.run(main())
