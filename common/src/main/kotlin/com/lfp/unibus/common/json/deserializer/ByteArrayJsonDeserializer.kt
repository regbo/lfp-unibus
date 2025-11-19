package com.lfp.unibus.common.json.deserializer

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.core.ObjectCodec
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.lfp.unibus.common.data.DataUrl
import java.io.ByteArrayOutputStream

class ByteArrayJsonDeserializer : JsonDeserializer<ByteArray>() {
  override fun deserialize(p: JsonParser, ctxt: DeserializationContext): ByteArray? {
    return deserialize(p)
  }

  companion object {
    @JvmStatic
    fun deserialize(p: JsonParser): ByteArray? {
      return when (p.currentToken()) {
        JsonToken.VALUE_NULL -> null
        JsonToken.VALUE_STRING -> {
          return deserialize(p.text)
        }
        else -> {
          val node = p.readValueAsTree<JsonNode>()
          return deserialize(p.codec, node)
        }
      }
    }

    @JvmStatic
    fun deserialize(codec: ObjectCodec, node: JsonNode?): ByteArray? {
      if (node != null && !node.isNull) {
        if (node.isTextual) {
          return deserialize(node.textValue())
        } else {
          if (codec is ObjectMapper) {
            return codec.writeValueAsBytes(node)
          } else {
            ByteArrayOutputStream().use { out ->
              codec.factory.createGenerator(out).use { generator ->
                codec.writeValue(generator, node)
              }
              return out.toByteArray()
            }
          }
        }
      }
      return null
    }

    fun deserialize(text: String?): ByteArray? {
      if (text != null && !text.isEmpty()) {
        val dataUrl = DataUrl.parse(text)
        if (dataUrl != null && dataUrl.base64) {
          return dataUrl.data
        }
        return text.encodeToByteArray()
      }
      return null
    }
  }
}
