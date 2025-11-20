package com.lfp.unibus.common.json.serializer

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.SerializerProvider
import org.apache.kafka.common.utils.Bytes

/**
 * Jackson serializer for Kafka Bytes.
 *
 * Serializes by extracting ByteArray and using ByteArrayJsonSerializer.
 */
class BytesJsonSerializer : JsonSerializer<Bytes>() {

  /**
   * Serializes Kafka Bytes to JSON.
   *
   * Extracts the underlying ByteArray and delegates to ByteArrayJsonSerializer.
   *
   * @param value Bytes instance to serialize
   * @param gen JSON generator
   * @param serializers Serializer provider
   */
  override fun serialize(value: Bytes, gen: JsonGenerator, serializers: SerializerProvider) {
    val valueBarr = value.get()
    serializers.defaultSerializeValue(valueBarr, gen)
  }
}
