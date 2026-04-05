package com.lfp.unibus.websocket.service

import com.fasterxml.jackson.databind.ObjectMapper
import com.lfp.unibus.common.Extensions.queryProperties
import com.lfp.unibus.common.Extensions.topic
import com.lfp.unibus.common.KafkaService
import com.lfp.unibus.common.data.PayloadType
import com.lfp.unibus.common.data.ProducerData
import java.net.URI
import org.apache.kafka.common.utils.Bytes
import org.springframework.core.convert.ConversionService
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.kafka.sender.SenderRecord

@Component
class KafkaProducer(
    conversionService: ConversionService,
    objectMapper: ObjectMapper,
    kafkaService: KafkaService,
) : KafkaHandler(conversionService, objectMapper, kafkaService) {

//  fun produce(
//      transport: Transport,
//      properties: Map<String, Any?>? = null,
//      uri: URI? = null,
//  ): Mono<Void> {
//    val uriProperties = uri.queryProperties()
//    val topic = uri.topic() ?: return transport.sendError("topic required")
//    val producer = kafkaService.producer(properties, uriProperties)
//    val resultPayloads =
//        transport
//            .receive()
//            .takeUntilOther(transport.closed())
//            .flatMapIterable { msg -> toSenderRecords(topic, msg.payloadAsText) }
//            .flatMap { record -> producer.send(Mono.just(record)) }
//            .filter { producerResultEnabled }
//            .mapNotNull { result -> toConsumerPayload(PayloadType.RESULT, result) }
//            .map { session.textMessage(it) }
//            .onErrorResume { e ->
//              log.error("producer error", e)
//              Mono.error(e)
//            }
//
//    return session.send(resultPayloads).doFinally { producer.close() }
//  }

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

  interface Transport {

    fun receive(): Flux<ByteArray>

    fun send(payload: ByteArray): Mono<Void>

    fun sendError(message: String): Mono<Void>

    fun closed(): Mono<Void>
  }
}
