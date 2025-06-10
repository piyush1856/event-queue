# Event Queue

An async event queue implementation using Kafka for Python applications.

## Features

- Async Kafka producer and consumer implementation using aiokafka
- Queue-based event emission with batching support
- Health checks and metrics for Kubernetes integration
- Prometheus metrics for monitoring
- Comprehensive error handling and logging
- Type hints and documentation

## Installation

```bash
pip install event-queue
```

## Usage

### Producer

```python
from eventqueue.queue_emitter import QueueEmitterWrapper

# Initialize the emitter
emitter = QueueEmitterWrapper({
    "bootstrap_servers": "localhost:9092",
    "enable_idempotence": True,
    "acks": "all",
})

# Initialize the emitter
await emitter.initialize()

# Emit an event
await emitter.emit(
    topics=["my-topic"],
    event={"key": "value"},
    partition_value="partition-key",
)

# Queue events for batch emission
emitter.add_event_to_queue(
    topics=["my-topic"],
    event={"key": "value"},
    partition_value="partition-key",
)

# Emit queued events
await emitter.emit_events()

# Stop the emitter
await emitter.stop()
```

### Consumer

```python
from eventqueue.queue_consumer import setup_and_start_consumer

# Define message processing tasks
async def process_message(message):
    print(f"Processing message: {message}")

# Consumer configuration
config = {
    "consumer_config": {
        "bootstrap_servers": "localhost:9092",
        "group_id": "my-group",
    },
    "topics_configurations": {
        "my-topic": {
            "tasks": [process_message],
        },
    },
}

# Set up and start the consumer
consumer = await setup_and_start_consumer(config)

# Stop the consumer
await consumer.stop()
```

### Health Checks

The library provides health check endpoints for Kubernetes integration:

```python
from eventqueue.queue_health import _healthz, _readyz

# Start health check tasks
await _healthz()
await _readyz()
```

## Configuration

### Producer Configuration

```python
{
    "bootstrap_servers": "localhost:9092",
    "enable_idempotence": True,
    "acks": "all",
    "retries": 3,
    "max_in_flight_requests_per_connection": 1,
}
```

### Consumer Configuration

```python
{
    "bootstrap_servers": "localhost:9092",
    "session_timeout_ms": 20000,
    "auto_offset_reset": "latest",
    "enable_auto_commit": True,
    "auto_commit_interval_ms": 5000,
}
```

## Metrics

The library exposes the following Prometheus metrics:

- `event_queue_messages_produced_total`: Total number of messages produced
- `event_queue_messages_consumed_total`: Total number of messages consumed
- `event_queue_producer_lag`: Number of messages waiting to be produced
- `event_queue_consumer_lag`: Number of messages waiting to be consumed

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details. 