package com.lfp.unibus.common.json.deserializer

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import org.apache.kafka.common.utils.Bytes

class BytesJsonDeserializer : JsonDeserializer<Bytes>() {

  override fun deserialize(p: JsonParser, ctxt: DeserializationContext): Bytes? {
    val barr = ByteArrayJsonDeserializer.deserialize(p)
    return barr?.let { Bytes.wrap(it) }
  }
}
