package com.lfp.unibus.common.json.serializer

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.SerializerProvider
import org.apache.kafka.common.header.Headers

/**
 * Jackson serializer for Kafka Headers.
 *
 * Serializes as array of Header objects, each with "key" and "value" fields.
 */
class HeadersJsonSerializer : JsonSerializer<Headers>() {
  /**
   * Serializes Kafka Headers to JSON array.
   *
   * Outputs an array of Header objects, each serialized as an object with "key" and "value" fields.
   *
   * @param value Headers collection to serialize
   * @param gen JSON generator
   * @param serializers Serializer provider
   */
  override fun serialize(value: Headers, gen: JsonGenerator, serializers: SerializerProvider) {
    gen.writeStartArray()
    for (header in value) {
      serializers.defaultSerializeValue(header, gen)
    }
    gen.writeEndArray()
  }
}
