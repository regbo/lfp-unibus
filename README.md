# LFP Unibus

A Spring Boot application that provides a WebSocket-based interface to Apache Kafka, allowing clients to produce and consume Kafka messages through WebSocket connections.

## Overview

LFP Unibus bridges WebSocket connections and Apache Kafka, enabling real-time bidirectional communication with Kafka topics. The application extracts topic names from WebSocket URL paths and supports flexible configuration through query parameters.

## Features

- **WebSocket to Kafka Bridge**: Seamlessly connect WebSocket clients to Kafka topics
- **Bidirectional Communication**: Support for both producing and consuming messages
- **Flexible Configuration**: Configure Kafka producer and consumer settings via query parameters
- **JSON and Binary Support**: Handle both JSON and binary message formats
- **Reactive Architecture**: Built on Spring WebFlux and Reactor Kafka for high performance

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

#### URL Components

- **Path Segments**: The topic name is derived from URL path segments, joined with underscores
  - Example: `ws://localhost:8888/my/topic` → topic name: `my_topic`
  
- **Query Parameters**:
  - `producer`: Enable producer functionality (default: `true`)
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

## Message Formats

### Producing Messages

Send JSON messages through the WebSocket connection with the following structure:

```json
{
  "partition": 0,
  "timestamp": 1234567890000,
  "key": "message-key",
  "value": {"field": "value"},
  "keyBinary": null,
  "valueBinary": null,
  "headers": [
    {"key": "header-key", "value": "header-value"}
  ]
}
```

**Fields:**
- `partition`: Optional partition number (null for automatic assignment)
- `timestamp`: Optional timestamp in milliseconds (null for current time)
- `key`: Key as JSON (mutually exclusive with `keyBinary`)
- `keyBinary`: Key as Base64-encoded binary (mutually exclusive with `key`)
- `value`: Value as JSON (mutually exclusive with `valueBinary`)
- `valueBinary`: Value as Base64-encoded binary (mutually exclusive with `value`)
- `headers`: Optional list of headers

**Note**: For each key/value pair, only one format (JSON or binary) can be specified.

### Consuming Messages

Received messages are JSON objects with the following structure:

```json
{
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

## Architecture

### Components

- **App**: Main Spring Boot application entry point
- **Config**: Spring configuration for WebSocket handling and Kafka options
- **KafkaWebSocketHandler**: Core WebSocket handler that bridges connections to Kafka
- **ProducerData**: Data class for producer message payloads
- **ConsumerData**: Data class for consumer message payloads
- **ByteArrayDeserializer**: Custom Jackson deserializer for Base64-encoded byte arrays

### Technology Stack

- **Spring Boot**: Application framework
- **Spring WebFlux**: Reactive web framework for WebSocket support
- **Reactor Kafka**: Reactive Kafka client
- **Jackson**: JSON serialization/deserialization
- **Kotlin**: Programming language

## Development

### Project Structure

```
app/
  src/
    main/
      kotlin/com/lfp/unibus/
        App.kt                    # Main application class
        Config.kt                 # Spring configuration
        KafkaWebSocketHandler.kt  # WebSocket handler
        ProducerData.kt           # Producer data model
        ConsumerData.kt           # Consumer data model
        ByteArrayDeserializer.kt  # Custom deserializer
      resources/
        application.yaml          # Application configuration
    test/
      kotlin/com/lfp/unibus/     # Test files
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

