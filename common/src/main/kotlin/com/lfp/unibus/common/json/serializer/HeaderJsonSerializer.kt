package com.lfp.unibus.common.json.serializer

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.SerializerProvider
import org.apache.kafka.common.header.Header

class HeaderJsonSerializer : JsonSerializer<Header>() {
  override fun serialize(value: Header?, gen: JsonGenerator, serializers: SerializerProvider?) {
    if (value != null) {
      gen.writeStartObject()
      gen.writeFieldName("key")
      val key = value.key()
      if (key == null) {
        gen.writeNull()
      } else {
        gen.writeString(key)
      }
      gen.writeFieldName("value")
      ByteArrayJsonSerializer.serialize(value.value(), gen)
      gen.writeEndObject()
    }
  }
}
