package com.lfp.unibus.common.json.serializer

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.SerializerProvider
import org.apache.kafka.common.header.Header

/**
 * Jackson serializer for Kafka Header.
 *
 * Serializes as object with "key" and "value" fields.
 */
class HeaderJsonSerializer : JsonSerializer<Header>() {
  override fun serialize(value: Header, gen: JsonGenerator, serializers: SerializerProvider) {
    gen.writeStartObject()
    gen.writeFieldName("key")
    val key = value.key()
    if (key == null) {
      gen.writeNull()
    } else {
      gen.writeString(key)
    }
    gen.writeFieldName("value")
    serializers.defaultSerializeValue(value.value(), gen)
    gen.writeEndObject()
  }
}
