package com.lfp.unibus.service.ws

import com.fasterxml.jackson.databind.MapperFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.lfp.unibus.common.KafkaConfig
import com.lfp.unibus.common.data.ConsumerData
import com.lfp.unibus.common.data.ProducerData
import com.lfp.unibus.service.KafkaService
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.utils.Bytes
import org.slf4j.LoggerFactory
import org.springframework.core.convert.ConversionService
import org.springframework.stereotype.Component
import org.springframework.web.reactive.socket.CloseStatus
import org.springframework.web.reactive.socket.WebSocketHandler
import org.springframework.web.reactive.socket.WebSocketSession
import org.springframework.web.util.UriComponentsBuilder
import reactor.core.publisher.Mono
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
    var kafkaService: KafkaService,
    var kafkaConfig: KafkaConfig,
) : WebSocketHandler {

  init {
    objectMapper = objectMapper.copy().apply { enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS) }
  }

  private val logger = LoggerFactory.getLogger(this::class.java)

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

    val producerEnabled = queryParamBoolean("producer") ?: true
    val consumerEnabled = queryParamBoolean("consumer") ?: true
    if (!producerEnabled && !consumerEnabled) {
      return session.close(
          CloseStatus.POLICY_VIOLATION.withReason("producer and consumer cannot both be false")
      )
    }
    val consumeFlow =
        if (consumerEnabled) {
          val consumerConfig = kafkaConfig.consumer(uri.queryParams.asSingleValueMap())
          val consumer = kafkaService.consumer(consumerConfig, topic)
          val kafkaFlux =
              consumer
                  .receive()
                  .takeUntilOther(session.closeStatus().then())
                  .map { record -> toConsumerPayload(record) }
                  .map { session.textMessage(it) }
                  .onErrorResume { e ->
                    logger.error("consumer error", e)
                    Mono.error(e)
                  }

          session.send(kafkaFlux)
        } else {
          Mono.empty()
        }

    val produceFlow =
        if (producerEnabled) {
          val producerConfig = kafkaConfig.producer(uri.queryParams.asSingleValueMap())
          val producer = kafkaService.producer(producerConfig)
          session
              .receive()
              .takeUntilOther(session.closeStatus().then())
              .flatMapIterable { msg -> toSenderRecords(topic, msg.payloadAsText) }
              .flatMap { record -> producer.send(Mono.just(record)) }
              .onErrorResume { e ->
                logger.error("producer error", e)
                Mono.error(e)
              }
              .then()
              .doFinally { _ -> producer.close() }
        } else {
          session
              .receive()
              .doOnNext { println("Discarded WebSocket message for topic=$topic") }
              .then()
        }

    return Mono.`when`(produceFlow, consumeFlow).doFinally {
      println(
          "Closing WebSocket for topic=$topic (producer=$producerEnabled consumer=$consumerEnabled)"
      )
    }
  }

  /**
   * Converts a WebSocket message string to a Kafka SenderRecord.
   *
   * @param topic The Kafka topic name
   * @param msg The JSON message string from WebSocket
   * @return SenderRecord ready to be sent to Kafka
   */
  private fun toSenderRecords(
      topic: String,
      msg: String,
  ): List<SenderRecord<Bytes, Bytes, *>> {
    return ProducerData.read(objectMapper, topic, msg).map { it.toSenderRecord<Any>() }
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
    val producerData = ConsumerData(record)
    return objectMapper.writeValueAsString(producerData)
  }
}
