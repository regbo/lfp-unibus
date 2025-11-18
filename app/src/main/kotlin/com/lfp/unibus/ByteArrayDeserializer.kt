package com.lfp.unibus

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import java.util.*

class ByteArrayDeserializer : JsonDeserializer<ByteArray>() {

  override fun deserialize(p: JsonParser, ctxt: DeserializationContext): ByteArray {
    return deserialize(p.text)
  }

  companion object {

    private val regex = Regex("^data:(?:([^/;,]+)/([^;,]+))?(?:;([^,]+))?,(.*)$")

    @JvmStatic
    fun deserialize(text: String): ByteArray {
      val m = regex.matchEntire(text)
      val base64 = if (m != null) m.groupValues[4] else text

      return Base64.getDecoder().decode(base64)
    }
  }
}
