package com.lfp.unibus

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.MapperFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.TextNode
import java.lang.reflect.Modifier
import java.nio.ByteBuffer
import java.nio.CharBuffer
import java.nio.charset.CodingErrorAction
import java.nio.charset.StandardCharsets
import java.util.*
import kotlin.reflect.KClass
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.serialization.BytesDeserializer
import org.apache.kafka.common.serialization.BytesSerializer
import org.apache.kafka.common.utils.Bytes
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.core.convert.ConversionService
import org.springframework.stereotype.Component
import org.springframework.web.reactive.socket.CloseStatus
import org.springframework.web.reactive.socket.WebSocketHandler
import org.springframework.web.reactive.socket.WebSocketSession
import org.springframework.web.util.UriComponents
import org.springframework.web.util.UriComponentsBuilder
import reactor.core.publisher.Mono
import reactor.kafka.receiver.KafkaReceiver
import reactor.kafka.receiver.ReceiverOptions
import reactor.kafka.sender.KafkaSender
import reactor.kafka.sender.SenderOptions
import reactor.kafka.sender.SenderRecord

/**
 * WebSocket handler that bridges WebSocket connections to Apache Kafka.
 *
 * Handles WebSocket connections and translates them into Kafka producer/consumer operations. The
 * topic name is derived from the WebSocket URL path segments. Query parameters control
 * producer/consumer behavior and Kafka configuration options.
 *
 * URL Format:
 * ws://host:port/{topic-segments}?producer={true|false}&consumer={true|false}&{kafka-config}
 *
 * @param conversionService Spring conversion service for type conversion
 * @param objectMapper Jackson ObjectMapper for JSON serialization/deserialization
 * @param kafkaOptions Map of Kafka configuration options from environment properties
 */
@Component
class KafkaWebSocketHandler(
        var conversionService: ConversionService,
        var objectMapper: ObjectMapper,
        @param:Qualifier("kafkaOptions") val kafkaOptions: Map<String, Any?>,
) : WebSocketHandler {

  init {
    objectMapper = objectMapper.copy().apply { enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS) }
  }

  private val logger = LoggerFactory.getLogger(this::class.java)
  /**
   * Cache of valid Kafka configuration property names for ProducerConfig and ConsumerConfig. Used
   * to validate query parameter names.
   */
  private val configPropertyNames =
          listOf(ProducerConfig::class, ConsumerConfig::class)
                  .map { clazz ->
                    val fields = clazz.java.fields
                    val propertyNames =
                            fields
                                    .filter {
                                      Modifier.isPublic(it.modifiers) &&
                                              Modifier.isStatic(it.modifiers) &&
                                              Modifier.isFinal(it.modifiers)
                                    }
                                    .filter { it.type == String::class.java }
                                    .filter { it.name.endsWith("_CONFIG") }
                                    .map { it.get(null) as String }
                    return@map Pair(clazz, propertyNames)
                  }
                  .toMap()

  /**
   * Handles a WebSocket session by setting up Kafka producer and/or consumer flows.
   *
   * The topic name is extracted from URL path segments (joined with underscores). Query parameters:
   * - "producer": Enable producer (default: true)
   * - "consumer": Enable consumer (default: true)
   * - "producer.{kafka-property}": Producer-specific Kafka configuration
   * - "consumer.{kafka-property}": Consumer-specific Kafka configuration
   *
   * @param session The WebSocket session to handle
   * @return Mono that completes when the session closes
   */
  override fun handle(session: WebSocketSession): Mono<Void> {
    val uri = UriComponentsBuilder.fromUri(session.handshakeInfo.uri).build()

    val topic = uri.pathSegments.filter { !it.isEmpty() }.joinToString("_")
    if (topic.isEmpty()) {
      return session.close(CloseStatus.POLICY_VIOLATION.withReason("topic is required"))
    }

    fun queryParamBoolean(name: String): Boolean? {
      return uri.queryParams.getFirst(name)?.let {
        conversionService.convert(it, Boolean::class.java)
      }
    }

    val producer = queryParamBoolean("producer") ?: true
    val consumer = queryParamBoolean("consumer") ?: true
    if (!producer && !consumer) {
      return session.close(
              CloseStatus.POLICY_VIOLATION.withReason("producer and consumer cannot both be false")
      )
    }
    val consumeFlow =
            if (consumer) {
              val consumerProps = consumerProperties(uri)
              val receiverOptions =
                      ReceiverOptions.create<Bytes, Bytes>(consumerProps)
                              .subscription(listOf(topic))
              val kafkaFlux =
                      KafkaReceiver.create(receiverOptions)
                              .receive()
                              .takeUntilOther(session.closeStatus().then())
                              .flatMap { record ->
                                Mono.fromCallable { toConsumerPayload(record) }.onErrorResume { e ->
                                  logger.error("consumer mapping error", e)
                                  Mono.error(e)
                                }
                              }
                              .map { session.textMessage(it) }

              session.send(kafkaFlux)
            } else {
              Mono.empty()
            }

    val produceFlow =
            if (producer) {
              val kafkaSender =
                      KafkaSender.create(
                              SenderOptions.create<Bytes, Bytes>(producerProperties(uri))
                      )

              session.receive()
                      .takeUntilOther(session.closeStatus().then())
                      .flatMap { msg ->
                        Mono.fromCallable { toSenderRecord(topic, msg.payloadAsText) }
                                .onErrorResume { e ->
                                  logger.error("producer mapping error", e)
                                  Mono.error(e)
                                }
                      }
                      .flatMap { record -> kafkaSender.send(Mono.just(record)) }
                      .then()
                      .doFinally { _ -> kafkaSender.close() }
            } else {
              session.receive()
                      .doOnNext { println("Discarded WebSocket message for topic=$topic") }
                      .then()
            }

    return Mono.`when`(produceFlow, consumeFlow).doFinally {
      println("Closing WebSocket for topic=$topic (producer=$producer consumer=$consumer)")
    }
  }

  /**
   * Builds Kafka producer properties from URI query parameters and global options.
   *
   * Extracts producer-specific configuration from query parameters prefixed with "producer.",
   * merges with global kafkaOptions, and sets required serializer classes. Automatically generates
   * a unique client ID if not provided.
   *
   * @param uri The URI components containing query parameters
   * @return Properties configured for Kafka producer
   */
  private fun producerProperties(uri: UriComponents): Properties {
    val props = kafkaProperties(uri, "producer", ProducerConfig::class)
    props.computeIfAbsent(
            ProducerConfig.CLIENT_ID_CONFIG,
            { "anonymous." + UUID.randomUUID() },
    )
    val serializerClass = BytesSerializer::class.java.name
    props.putAll(kafkaOptions)
    props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = serializerClass
    props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = serializerClass
    return props
  }

  /**
   * Builds Kafka consumer properties from URI query parameters and global options.
   *
   * Extracts consumer-specific configuration from query parameters prefixed with "consumer.",
   * merges with global kafkaOptions, and sets required deserializer classes. Automatically
   * generates unique group ID and client ID if not provided. Sets auto.offset.reset to "latest" if
   * not specified.
   *
   * @param uri The URI components containing query parameters
   * @return Properties configured for Kafka consumer
   */
  private fun consumerProperties(uri: UriComponents): Properties {
    val props = kafkaProperties(uri, "consumer", ConsumerConfig::class)
    props.computeIfAbsent(
            ConsumerConfig.GROUP_ID_CONFIG,
            { "anonymous." + UUID.randomUUID() },
    )
    props.computeIfAbsent(
            ConsumerConfig.CLIENT_ID_CONFIG,
            { "anonymous." + UUID.randomUUID() },
    )
    props.computeIfAbsent(
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
            { "latest" },
    )
    val deserializerClass = BytesDeserializer::class.java.name
    props.putAll(kafkaOptions)
    props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = deserializerClass
    props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = deserializerClass
    return props
  }

  /**
   * Extracts Kafka configuration properties from URI query parameters.
   *
   * Filters query parameters by the given prefix (e.g., "producer." or "consumer."), validates
   * property names against known Kafka configuration constants, and converts them to a Properties
   * object.
   *
   * @param uri The URI components containing query parameters
   * @param prefix The prefix to filter properties by (e.g., "producer." or "consumer.")
   * @param configClass The Kafka config class to validate property names against
   * @return Properties object containing the extracted configuration
   * @throws IllegalArgumentException if an invalid Kafka property name is found
   */
  private fun kafkaProperties(
          uri: UriComponents,
          prefix: String,
          configClass: KClass<out AbstractConfig>,
  ): Properties {
    val configPropertyNames = this.configPropertyNames[configClass]
    return uri.queryParams
            .keys
            .mapNotNull { key ->
              val kafkaKey = key.substringAfter("$prefix.", "")
              if (!kafkaKey.isEmpty()) {
                if (configPropertyNames != null && !configPropertyNames.contains(kafkaKey)) {
                  throw IllegalArgumentException("Invalid kafka property name: $kafkaKey")
                }
                val consumerValue = uri.queryParams.getFirst(key)
                if (consumerValue != null && !consumerValue.isEmpty()) {
                  return@mapNotNull Pair(kafkaKey, consumerValue)
                }
              }
              null
            }
            .toMap(Properties())
  }

  /**
   * Converts a WebSocket message string to a Kafka SenderRecord.
   *
   * @param topic The Kafka topic name
   * @param msg The JSON message string from WebSocket
   * @return SenderRecord ready to be sent to Kafka
   */
  private fun toSenderRecord(
          topic: String,
          msg: String,
  ): SenderRecord<Bytes, Bytes, *> {
    return SenderRecord.create(toProducerRecord(topic, msg), null)
  }

  /**
   * Converts a WebSocket message to a Kafka ProducerRecord.
   *
   * Deserializes the JSON message into ProducerData, converts headers, key, and value to
   * appropriate formats, and creates a ProducerRecord.
   *
   * @param topic The Kafka topic name
   * @param msg The JSON message string from WebSocket
   * @return ProducerRecord ready to be sent to Kafka
   */
  private fun toProducerRecord(
          topic: String,
          msg: String,
  ): ProducerRecord<Bytes, Bytes> {
    val producerData = objectMapper.readValue(msg, ProducerData::class.java)
    val headers =
            producerData?.headers?.let {
              val recordHeaders = RecordHeaders()
              for (header in it) {
                recordHeaders.add(header.key, toByteArray(header.value, header.valueBinary))
              }
              recordHeaders
            }
    val key = toBytes(producerData.key, producerData.keyBinary)
    val value = toBytes(producerData.value, producerData.valueBinary)
    val producerRecord =
            ProducerRecord<Bytes, Bytes>(
                    topic,
                    producerData.partition,
                    producerData.timestamp,
                    key,
                    value,
                    headers,
            )
    return producerRecord
  }

  /**
   * Converts a JsonNode or ByteArray to Kafka Bytes format.
   *
   * @param node Optional JsonNode to convert
   * @param binary Optional ByteArray to use if node is null
   * @return Bytes instance or null if both inputs are null
   */
  private fun toBytes(node: JsonNode?, binary: ByteArray?): Bytes? {
    return toByteArray(node, binary)?.let { Bytes.wrap(it) }
  }

  /**
   * Converts a JsonNode or ByteArray to a ByteArray.
   *
   * If node is provided and is textual, returns its text as bytes. If node is provided and is not
   * textual, serializes it to JSON bytes. Otherwise, returns the binary parameter.
   *
   * @param node Optional JsonNode to convert
   * @param binary Optional ByteArray to use if node is null
   * @return ByteArray or null if both inputs are null
   */
  private fun toByteArray(node: JsonNode?, binary: ByteArray?): ByteArray? {
    if (node != null && !node.isNull) {
      if (node.isTextual) {
        return node.textValue().toByteArray()
      } else {
        return objectMapper.writeValueAsBytes(node)
      }
    } else {
      return binary
    }
  }

  /**
   * Converts a Kafka ConsumerRecord to a JSON string payload.
   *
   * Extracts all record metadata and content, converts headers, key, and value to JsonNode format,
   * and serializes to JSON string for WebSocket transmission.
   *
   * @param record The Kafka ConsumerRecord to convert
   * @return JSON string representation of the consumer record
   */
  private fun toConsumerPayload(record: ConsumerRecord<Bytes, Bytes>): String {
    val headers =
            record.headers()?.let {
              it.map { header -> ConsumerData.Header(header.key(), toJsonNode(header.value())) }
            }
    val key = toJsonNode(record.key())

    val value = toJsonNode(record.value())
    val producerData =
            ConsumerData(
                    partition = record.partition(),
                    offset = record.offset(),
                    timestamp = record.timestamp(),
                    timestampType = record.timestampType(),
                    serializedKeySize = record.serializedKeySize(),
                    serializedValueSize = record.serializedValueSize(),
                    headers = headers,
                    key = key,
                    value = value,
                    leaderEpoch = record.leaderEpoch().orElse(null),
                    deliveryCount = record.deliveryCount().orElse(null),
            )
    return objectMapper.writeValueAsString(producerData)
  }

  /**
   * Converts Kafka Bytes to a JsonNode.
   *
   * @param value Optional Bytes instance to convert
   * @return JsonNode representation or null if input is null
   */
  private fun toJsonNode(value: Bytes?): JsonNode? {
    return value?.let { toJsonNode(it.get()) }
  }

  /**
   * Converts a ByteArray to a JsonNode.
   *
   * Attempts to decode as UTF-8 text first. If that fails, tries to parse as JSON. If both fail,
   * returns a Base64-encoded text node.
   *
   * @param value Optional ByteArray to convert
   * @return JsonNode representation (TextNode for text/Base64, parsed JSON node, or null)
   */
  private fun toJsonNode(value: ByteArray?): JsonNode? {
    if (value == null) return null
    val decoder =
            StandardCharsets.UTF_8
                    .newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)

    val buf = CharBuffer.allocate(value.size * 2)

    val result = decoder.decode(ByteBuffer.wrap(value), buf, true)
    if (result.isError) {
      return TextNode.valueOf(Base64.getEncoder().encodeToString(value))
    }

    return runCatching { objectMapper.readTree(value) }.getOrElse {
      buf.flip()
      val text = buf.toString()
      TextNode.valueOf(text)
    }
  }
}
