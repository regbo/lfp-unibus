# Unibus

A Spring Boot application providing a WebSocket interface to Apache Kafka for bidirectional message streaming.

## Why Unibus?

* Bridges WebSocket clients directly to Kafka topics for real time browser or device messaging
* Supports both producing and consuming over a single durable socket
* Exposes Kafka producer and consumer tuning knobs through simple query parameters
* Accepts JSON, plain text, or binary payloads without extra tooling
* Uses Spring WebFlux plus Reactor Kafka for highly concurrent workloads

## Get Started

1. Install JDK 21 and Gradle 8.14.3 or newer.
2. Ensure access to a Kafka cluster reachable from the application.
3. Clone the repository and build:
   ```bash
   ./gradlew build
   ```
4. Run the WebSocket bridge:
   ```bash
   ./gradlew bootRun
   ```
5. Connect a WebSocket client to `ws://localhost:8888/{topic}` to produce and/or consume messages.

## Overview

Bridges WebSocket connections to Kafka topics, enabling real time bidirectional communication. Topic names are derived from WebSocket URL paths with flexible configuration via query parameters.

## Requirements

- JDK 21 or higher
- Gradle 8.14.3 or higher
- Access to a Kafka cluster

## Building

```bash
./gradlew build
```

## Running

```bash
./gradlew bootRun
```

The application will start on port 8888 by default (configurable via `application.yaml`).

## Configuration

### Application Configuration

Configuration is managed through `application.yaml`:

```yaml
server:
  port: 8888

kafka:
  bootstrap:
    servers: kafka.lfpconnect.io:443
  security:
    protocol: SSL
```

Any property prefixed with `kafka.` will be automatically extracted and used as Kafka configuration options.

### WebSocket URL Format

Connect to Kafka topics via WebSocket using the following URL format:

```
ws://host:port/{topic-segments}?producer={true|false}&consumer={true|false}&{kafka-config}
```

All query parameters map directly to the Java Kafka producer and consumer configuration options.
Use the official documentation to look up every available setting:
[`Producer Configs`](https://kafka.apache.org/documentation/#producerconfigs) |
[`Consumer Configs`](https://kafka.apache.org/documentation/#consumerconfigs)

#### URL Components

- **Path Segments**: The topic name is derived from URL path segments, joined with underscores
  - Example: `ws://localhost:8888/my/topic` → topic name: `my_topic`
  
- **Query Parameters**:
  - `producer`: Enable producer functionality (default: `true`)
  - `producer.result`: Enable producer result messages (default: `true`). When enabled, sends confirmation messages back through the WebSocket after messages are successfully produced to Kafka.
  - `consumer`: Enable consumer functionality (default: `true`)
  - `producer.{kafka-property}`: Producer-specific Kafka configuration
  - `consumer.{kafka-property}`: Consumer-specific Kafka configuration

#### Examples

**Consume only from a topic:**
```
ws://localhost:8888/my-topic?consumer=true&producer=false
```

**Produce only to a topic:**
```
ws://localhost:8888/my-topic?producer=true&consumer=false
```

**Custom consumer group:**
```
ws://localhost:8888/my-topic?consumer.group.id=my-custom-group
```

**Custom producer client ID:**
```
ws://localhost:8888/my-topic?producer.client.id=my-producer
```

**Disable producer result messages:**
```
ws://localhost:8888/my-topic?producer.result=false
```

## Message Formats

### Producing Messages

JSON payloads mirror the Java [`ProducerRecord`](https://kafka.apache.org/26/javadoc/org/apache/kafka/clients/producer/ProducerRecord.html)
shape. Any field accepted by `ProducerRecord` can be supplied, and the `key`/`value` encodings follow
Kafka producer behavior.

Send JSON messages through the WebSocket connection. The message format is flexible and supports multiple input formats. The `key` and `value` fields accept various formats that are automatically converted to binary data:

#### Single Message Object

Send a single message with full control over all fields:

```json
{
  "partition": 0,
  "timestamp": 1234567890000,
  "key": "message-key",
  "value": {"field": "value"},
  "headers": [
    {"key": "header-key", "value": "header-value"}
  ]
}
```

**Fields:**
- `partition`: Optional partition number (null for automatic assignment)
- `timestamp`: Optional timestamp in milliseconds (null for current time)
- `key`: Optional key. Accepts:
  - **Data URL**: `"data:text/plain;base64,SGVsbG8gV29ybGQ="` (base64-encoded binary)
  - **Plain string**: `"message-key"` (encoded as UTF-8 bytes)
  - **JSON object/array**: `{"field": "value"}` or `[1, 2, 3]` (serialized to JSON bytes)
- `value`: Optional value. Accepts the same formats as `key`:
  - **Data URL**: `"data:text/plain;base64,SGVsbG8gV29ybGQ="`
  - **Plain string**: `"simple-value"` (encoded as UTF-8 bytes)
  - **JSON object/array**: `{"field": "value"}` or `[1, 2, 3]` (serialized to JSON bytes)
- `headers`: Optional list of headers. Each header can be:
  - **Array format**: `["header-key", "header-value"]`
  - **Object format**: `{"key": "header-key", "value": "header-value"}`

**Note**: The `topic` field is automatically set from the WebSocket URL path and should not be included in the message.

#### Array of Messages

Send multiple messages at once by providing an array of message objects:

```json
[
  {
    "key": "key1",
    "value": {"field": "value1"},
    "headers": [["header1", "value1"]]
  },
  {
    "key": "key2",
    "value": {"field": "value2"},
    "headers": [["header2", "value2"]]
  }
]
```

Each object in the array will be sent as a separate Kafka message to the topic. Headers can use either array format `[["key", "value"]]` or object format `[{"key": "key", "value": "value"}]`.

#### Simple Value Format

For simple use cases, you can send just a value without wrapping it in a message object. The value will be automatically converted to binary:

```json
"simple-string-value"
```

or

```json
{"field": "value"}
```

or

```json
[1, 2, 3]
```

When a simple value is provided, it will be treated as the message `value` field, with all other fields (key, partition, timestamp, headers) set to their defaults (null/empty). Strings are encoded as UTF-8 bytes, while JSON objects and arrays are serialized to JSON bytes.

#### Array of Simple Values

You can also send an array of simple values, where each value becomes a separate message:

```json
["value1", "value2", "value3"]
```

This will produce three Kafka messages, each with the corresponding value. Each value is handled the same way as in the simple value format above.

### Consuming Messages

Received messages are JSON objects with a `type` field indicating the message type:
- `RECORD`: Kafka consumer record (when consuming from topics)
- `RESULT`: Producer result confirmation (when producer.result is enabled)

#### Consumer Records

Consumer records mirror the Java [`ConsumerRecord`](https://kafka.apache.org/26/javadoc/org/apache/kafka/clients/consumer/ConsumerRecord.html)
structure to keep deserialization predictable for clients that already speak Kafka.

Consumer records have the following structure:

```json
{
  "type": "RECORD",
  "partition": 0,
  "offset": 12345,
  "timestamp": 1234567890000,
  "timestampType": "CREATE_TIME",
  "serializedKeySize": 10,
  "serializedValueSize": 100,
  "headers": [
    {"key": "header-key", "value": "header-value"}
  ],
  "key": "message-key",
  "value": {"field": "value"},
  "leaderEpoch": 0,
  "deliveryCount": 1
}
```

**Fields:**
- `partition`: Partition number
- `offset`: Message offset
- `timestamp`: Timestamp in milliseconds
- `timestampType`: Type of timestamp (`CREATE_TIME`, `LOG_APPEND_TIME`, or `NO_TIMESTAMP_TYPE`)
- `serializedKeySize`: Size of serialized key in bytes
- `serializedValueSize`: Size of serialized value in bytes
- `headers`: List of headers (always object format: `{"key": "...", "value": ...}`)
- `key`: Message key. Serialized as:
  - **String**: If the bytes are valid UTF-8 text
  - **JSON object/array**: If the bytes are valid JSON
  - **Data URL**: Otherwise (e.g., `"data:application/octet-stream;base64,..."`)
- `value`: Message value. Serialized using the same logic as `key`
- `leaderEpoch`: Optional leader epoch
- `deliveryCount`: Optional delivery count

**Note**: The `topic` field is not included in the JSON (it's available from the WebSocket URL path).

#### Producer Results

When `producer.result=true` (the default), producer result messages are sent back through the WebSocket after messages are successfully produced. These messages have the following structure:

```json
{
  "type": "RESULT",
  "recordMetadata": {
    "topic": "my-topic",
    "partition": 0,
    "offset": 12345,
    "timestamp": 1234567890000,
    "serializedKeySize": 10,
    "serializedValueSize": 100
  },
  "correlationMetadata": null
}
```

**Fields:**
- `type`: Always `"RESULT"` for producer result messages
- `recordMetadata`: Kafka RecordMetadata containing topic, partition, offset, timestamp, and size information
- `correlationMetadata`: Optional correlation metadata (typically null)

To disable producer result messages, set `producer.result=false` in the WebSocket URL query parameters.

## Architecture

### Components

- **app**: Main application module with WebSocket handlers
- **common**: Shared module with Kafka configuration and data models
- **KafkaWebSocketHandler**: WebSocket handler bridging connections to Kafka
- **KafkaService**: Service for creating Kafka producers and consumers
- **KafkaConfig**: Shared Kafka configuration from environment properties

### Technology Stack

- **Spring Boot**: Application framework
- **Spring WebFlux**: Reactive web framework
- **Reactor Kafka**: Reactive Kafka client
- **Kotlin**: Programming language

## Development

### Project Structure

```
lfp-unibus/
  app/                           # Main application module
    src/main/kotlin/com/lfp/unibus/
      App.kt                     # Application entry point
      Config.kt                  # Spring configuration
      service/
        ws/KafkaWebSocketHandler.kt  # WebSocket handler
        KafkaService.kt          # Kafka service
  common/                        # Shared module
    src/main/kotlin/com/lfp/unibus/common/
      KafkaConfig.kt             # Kafka configuration
      data/                      # Data models
      json/                      # JSON serializers/deserializers
```

### Building and Testing

```bash
# Build the project
./gradlew build

# Run tests
./gradlew test

# Run the application
./gradlew bootRun
```

## License

See LICENSE file for details.

