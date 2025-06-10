# Event Queue Examples

This directory contains example implementations of the event queue library.

## Running the Examples

1. Start a local Kafka broker (e.g., using Docker):
```bash
docker run -d --name kafka -p 9092:9092 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092 -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 wurstmeister/kafka
```

2. Run the producer example:
```bash
python producer_example.py
```

3. Run the consumer example in a separate terminal:
```bash
python consumer_example.py
```

## Example Features

- Producer example demonstrates:
  - Direct event emission
  - Queue-based event emission
  - Batch processing
  - Error handling
  - Logging

- Consumer example demonstrates:
  - Topic subscription
  - Message processing
  - Error handling
  - Graceful shutdown
  - Logging

## Configuration

Both examples use default configurations that can be modified in the respective files:
- `producer_example.py`: Modify `PRODUCER_CONFIG`
- `consumer_example.py`: Modify `CONSUMER_CONFIG`
