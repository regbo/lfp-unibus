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

/**
 * Jackson deserializer for ByteArray.
 *
 * Supports deserializing from strings (including data URLs), JSON nodes, or null values.
 */
class ByteArrayJsonDeserializer : JsonDeserializer<ByteArray>() {
  /**
   * Deserializes ByteArray from JSON parser.
   *
   * Delegates to the static deserialize method for parsing logic.
   *
   * @param p JSON parser
   * @param ctxt Deserialization context
   * @return Deserialized ByteArray or null
   */
  override fun deserialize(p: JsonParser, ctxt: DeserializationContext): ByteArray? {
    return deserialize(p)
  }

  companion object {
    /**
     * Deserializes ByteArray from JSON parser.
     *
     * Handles null values, string values (including data URLs), and JSON nodes.
     *
     * @param p JSON parser
     * @return Deserialized ByteArray or null
     */
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

    /**
     * Deserializes ByteArray from JSON node.
     *
     * If node is textual, delegates to string deserializer. Otherwise, serializes the node
     * to JSON bytes.
     *
     * @param codec Object codec for JSON operations
     * @param node JSON node to deserialize
     * @return Deserialized ByteArray or null
     */
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

    /**
     * Deserializes ByteArray from string.
     *
     * If the string is a valid data URL with base64 encoding, extracts the binary data.
     * Otherwise, encodes the string as UTF-8 bytes.
     *
     * @param text String to deserialize (may be a data URL or plain text)
     * @return Deserialized ByteArray or null if input is null or empty
     */
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
