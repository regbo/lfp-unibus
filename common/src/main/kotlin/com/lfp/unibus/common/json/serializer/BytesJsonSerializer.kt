package com.lfp.unibus.common.json.serializer

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.SerializerProvider
import org.apache.kafka.common.utils.Bytes

class BytesJsonSerializer : JsonSerializer<Bytes>() {

  override fun serialize(value: Bytes?, gen: JsonGenerator, serializers: SerializerProvider?) {
    val valueBarr = value?.get()
    ByteArrayJsonSerializer.serialize(valueBarr, gen)
  }
}
