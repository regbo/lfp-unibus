package com.lfp.unibus

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.MapperFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.TextNode
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
import java.lang.reflect.Modifier
import java.nio.ByteBuffer
import java.nio.CharBuffer
import java.nio.charset.CodingErrorAction
import java.nio.charset.StandardCharsets
import java.util.*
import kotlin.reflect.KClass

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

    val produce = queryParamBoolean("produce") ?: true
    val consume = queryParamBoolean("consumer") ?: true
    if (!produce && !consume) {
      return session.close(
          CloseStatus.POLICY_VIOLATION.withReason("produce and consume cannot both be false")
      )
    }
    val consumeFlow =
        if (consume) {
          val consumerProps = consumerProperties(uri)
          val receiverOptions =
              ReceiverOptions.create<Bytes, Bytes>(consumerProps).subscription(listOf(topic))
          val kafkaFlux =
              KafkaReceiver.create(receiverOptions)
                  .receive()
                  .doOnError { e ->
                    logger.error("Kafka error, disconnecting", e)
                    throw e // forces disconnect
                  }
                  .takeUntilOther(session.closeStatus())
                  .flatMap { record ->
                    Mono.fromCallable { toConsumerPayload(record) }
                        .onErrorResume { e ->
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
        if (produce) {
          val kafkaSender =
              KafkaSender.create(SenderOptions.create<Bytes, Bytes>(producerProperties(uri)))
          val outbound = kafkaSender.createOutbound()
          session
              .receive()
              .flatMap { msg ->
                Mono.fromCallable { toProducerRecord(topic, msg.payloadAsText) }
                    .onErrorResume { e ->
                      logger.error("producer mapping error", e)
                      Mono.error(e)
                    }
              }
              .flatMap { record -> outbound.send(Mono.just(record)) }
              .then()
        } else {
          session
              .receive()
              .doOnNext { println("Discarded WebSocket message for topic=$topic") }
              .then()
        }

    return Mono.`when`(produceFlow, consumeFlow).doFinally {
      println("Closing WebSocket for topic=$topic (produce=$produce consume=$consume)")
    }
  }

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

  private fun kafkaProperties(
      uri: UriComponents,
      prefix: String,
      configClass: KClass<out AbstractConfig>,
  ): Properties {
    val configPropertyNames = this.configPropertyNames[configClass]
    return uri.queryParams.keys
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

  private fun toBytes(node: JsonNode?, binary: ByteArray?): Bytes? {
    return toByteArray(node, binary)?.let { Bytes.wrap(it) }
  }

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

  private fun toJsonNode(value: Bytes?): JsonNode? {
    return value?.let { toJsonNode(it.get()) }
  }

  private fun toJsonNode(value: ByteArray?): JsonNode? {
    if (value == null) return null
    val decoder =
        StandardCharsets.UTF_8.newDecoder()
            .onMalformedInput(CodingErrorAction.REPORT)
            .onUnmappableCharacter(CodingErrorAction.REPORT)

    val buf = CharBuffer.allocate(value.size * 2)

    val result = decoder.decode(ByteBuffer.wrap(value), buf, true)
    if (result.isError) {
      return TextNode.valueOf(Base64.getEncoder().encodeToString(value))
    }

    return runCatching { objectMapper.readTree(value) }
        .getOrElse {
          buf.flip()
          val text = buf.toString()
          TextNode.valueOf(text)
        }
  }
}
