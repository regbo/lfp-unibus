package com.lfp.unibus.common.json.serializer

import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.Module
import com.fasterxml.jackson.databind.module.SimpleModule
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.utils.Bytes
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import reactor.kafka.sender.SenderResult

/**
 * Spring configuration for Jackson JSON serializers.
 *
 * Registers custom serializers for Kafka types including ByteArray, Bytes, Header, Headers,
 * ConsumerRecord, SenderResult, and RecordMetadata.
 */
@Configuration
class JsonSerializers {

  @Bean
  fun module(): Module {
    val m = SimpleModule()
    m.addSerializer(ByteArray::class.java, ByteArrayJsonSerializer())
    m.addSerializer(Bytes::class.java, BytesJsonSerializer())
    m.addSerializer(Header::class.java, HeaderJsonSerializer())
    m.addSerializer(Headers::class.java, HeadersJsonSerializer())
    for (cls in listOf(ConsumerRecord::class, SenderResult::class, RecordMetadata::class)) {
      @Suppress("UNCHECKED_CAST")
      m.addSerializer(cls.java, KafkaJsonSerializer(cls) as JsonSerializer<Any>)
    }
    return m
  }
}
