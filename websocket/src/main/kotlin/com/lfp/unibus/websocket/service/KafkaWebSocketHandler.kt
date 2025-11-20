package com.lfp.unibus.websocket.service

import com.fasterxml.jackson.databind.MapperFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.lfp.unibus.common.KafkaService
import com.lfp.unibus.common.data.ProducerData
import com.lfp.unibus.websocket.data.PayloadType
import org.apache.kafka.common.utils.Bytes
import org.slf4j.LoggerFactory
import org.springframework.core.convert.ConversionService
import org.springframework.stereotype.Component
import org.springframework.web.reactive.socket.CloseStatus
import org.springframework.web.reactive.socket.WebSocketHandler
import org.springframework.web.reactive.socket.WebSocketSession
import org.springframework.web.util.UriComponents
import org.springframework.web.util.UriComponentsBuilder
import reactor.core.publisher.Mono
import reactor.kafka.sender.SenderRecord

/**
 * WebSocket handler bridging connections to Kafka.
 *
 * Validates requested topics via Kafka Admin, then translates socket frames into producer sends or
 * consumer streams. Topic name derived from URL path segments. Query parameters control
 * producer/consumer behavior and Kafka configuration.
 *
 * URL Format:
 * ws://host:port/{topic-segments}?producer={true|false}&consumer={true|false}&{kafka-config}
 *
 * @param conversionService Spring conversion service
 * @param objectMapper Jackson ObjectMapper for JSON serialization
 * @param kafkaService Kafka service for creating producers/consumers and describing topics
 */
@Component
class KafkaWebSocketHandler(
    var conversionService: ConversionService,
    var objectMapper: ObjectMapper,
    var kafkaService: KafkaService,
) : WebSocketHandler {

  init {
    objectMapper = objectMapper.copy().apply { enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS) }
  }

  private val log = LoggerFactory.getLogger(this::class.java)

  /**
   * Handles WebSocket session by setting up Kafka producer/consumer flows.
   *
   * Topic name extracted from URL path segments. Query parameters:
   * - producer: Enable producer (default: true)
   * - producer.result: Enable producer result messages (default: true)
   * - consumer: Enable consumer (default: true)
   * - producer.{kafka-property}: Producer-specific Kafka config
   * - consumer.{kafka-property}: Consumer-specific Kafka config
   *
   * @param session WebSocket session
   * @return Mono completing when session closes
   */
  override fun handle(session: WebSocketSession): Mono<Void> {

    val uri = UriComponentsBuilder.fromUri(session.handshakeInfo.uri).build()

    fun queryParamBoolean(name: String): Boolean? {
      return uri.queryParams.getFirst(name)?.let {
        conversionService.convert(it, Boolean::class.java)
      }
    }

    val consumerEnabled = queryParamBoolean("consumer") ?: true
    val producerEnabled = queryParamBoolean("producer") ?: true
    val producerResultEnabled = queryParamBoolean("producer.result") ?: true

    if (!producerEnabled && !consumerEnabled) {
      return session.close(
          CloseStatus.POLICY_VIOLATION.withReason("producer and consumer cannot both be false")
      )
    }

    val topic = uri.pathSegments.filter { !it.isEmpty() }.joinToString("_")
    if (topic.isEmpty()) {
      return session.close(CloseStatus.POLICY_VIOLATION.withReason("topic is required"))
    }
    return kafkaService
        .describeTopic(topic)
        .switchIfEmpty(
            session
                .close(CloseStatus.POLICY_VIOLATION.withReason("topic does not exist"))
                .then(Mono.empty())
        )
        .flatMap {
          val consumeFlow =
              if (consumerEnabled) {
                handleConsume(session, uri, topic)
              } else {
                Mono.empty()
              }
          val produceFlow =
              if (producerEnabled) {
                handleProduce(session, uri, topic, producerResultEnabled)
              } else {
                session
                    .receive()
                    .doOnNext { log.debug("Discarded WebSocket message for topic={}", topic) }
                    .then()
              }
          return@flatMap Mono.`when`(produceFlow, consumeFlow).doFinally {
            log.info(
                "Closing WebSocket for topic={} ( consumer={} producer={} producerResult={})",
                topic,
                consumerEnabled,
                producerEnabled,
                producerResultEnabled,
            )
          }
        }
  }

  /**
   * Streams Kafka consumer records back to the WebSocket client.
   *
   * Creates a consumer using URI query parameters and forwards each record as JSON payloads until
   * the WebSocket closes or an error occurs.
   *
   * @param session Active WebSocket session
   * @param uri Parsed URI for extracting Kafka overrides
   * @param topic Validated Kafka topic name
   * @return Mono completing when the consumer stream terminates
   */
  private fun handleConsume(
      session: WebSocketSession,
      uri: UriComponents,
      topic: String,
  ): Mono<Void> {
    val consumer = kafkaService.consumer(topic, uri.queryParams.asSingleValueMap())
    val consumerPayloads =
        consumer
            .receive()
            .takeUntilOther(session.closeStatus().then())
            .map { record -> toConsumerPayload(PayloadType.RECORD, record) }
            .map { session.textMessage(it) }
            .onErrorResume { e ->
              log.error("consumer error", e)
              Mono.error(e)
            }

    return session.send(consumerPayloads)
  }

  /**
   * Streams WebSocket messages to Kafka as producer records.
   *
   * Creates a Kafka producer per session, transforms incoming frames into SenderRecords, and
   * optionally echoes `RESULT` payloads back to the client when producer result reporting is
   * enabled.
   *
   * @param session Active WebSocket session
   * @param uri Parsed URI for extracting Kafka overrides
   * @param topic Kafka topic name derived from the request path
   * @param producerResultEnabled Flag controlling producer result echoing
   * @return Mono completing when producer flow terminates
   */
  private fun handleProduce(
      session: WebSocketSession,
      uri: UriComponents,
      topic: String,
      producerResultEnabled: Boolean,
  ): Mono<Void> {
    val producer = kafkaService.producer(uri.queryParams.asSingleValueMap())
    val resultPayloads =
        session
            .receive()
            .takeUntilOther(session.closeStatus().then())
            .flatMapIterable { msg -> toSenderRecords(topic, msg.payloadAsText) }
            .flatMap { record -> producer.send(Mono.just(record)) }
            .filter { producerResultEnabled }
            .mapNotNull { result -> toConsumerPayload(PayloadType.RESULT, result) }
            .map { session.textMessage(it) }
            .onErrorResume { e ->
              log.error("producer error", e)
              Mono.error(e)
            }

    return session.send(resultPayloads).doFinally { producer.close() }
  }

  /**
   * Converts WebSocket message to Kafka sender records.
   *
   * @param topic Kafka topic name
   * @param msg JSON message string from WebSocket
   * @return List of SenderRecords ready for Kafka
   */
  private fun toSenderRecords(
      topic: String,
      msg: String,
  ): List<SenderRecord<Bytes, Bytes, *>> {
    val requestId = KafkaService.uuid()
    return ProducerData.read(objectMapper, topic, msg).map {
      val correlationMetadata = mapOf("recordId" to KafkaService.uuid(), "requestId" to requestId)
      it.toSenderRecord(correlationMetadata)
    }
  }

  /**
   * Converts Kafka record or result to JSON string payload.
   *
   * @param payloadType Type of payload (RECORD or RESULT)
   * @param value Kafka ConsumerRecord or SenderResult to convert
   * @return JSON string representation with type field
   */
  private fun toConsumerPayload(payloadType: PayloadType, value: Any): String {
    val node = objectMapper.valueToTree<ObjectNode>(value)
    node.put("type", payloadType.name)
    return objectMapper.writeValueAsString(node)
  }
}
