package com.lfp.unibus.common.json.deserializer

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.JsonNode
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.RecordHeader

/**
 * Jackson deserializer for Kafka Header.
 *
 * Supports array format [key, value] or object format {"key": "...", "value": ...}.
 */
class HeaderJsonDeserializer : JsonDeserializer<Header>() {
  /**
   * Deserializes Kafka Header from JSON parser.
   *
   * Supports array format [key, value] or object format {"key": "...", "value": ...}.
   * Also supports single-key object format {"key": "value"}.
   *
   * @param p JSON parser
   * @param ctxt Deserialization context
   * @return Deserialized Header instance or null
   */
  override fun deserialize(p: JsonParser, ctxt: DeserializationContext): Header? {
    return when (p.currentToken()) {
      JsonToken.VALUE_NULL -> null
      JsonToken.START_ARRAY,
      JsonToken.START_OBJECT -> {
        val node = p.readValueAsTree<JsonNode>()
        deserialize(p, ctxt, node)
      }
      else -> {
        val node = p.readValueAsTree<JsonNode>()
        reportInputMismatch(ctxt, node)
      }
    }
  }

  private fun deserialize(p: JsonParser, ctxt: DeserializationContext, node: JsonNode): Header {
    if (node.isArray) {
      if (node.size() <= 2) {
        val key = get(node, 0)
        if (key == null || key.isTextual) {
          val value = get(node, 1)
          val valueBarr = value?.let { ByteArrayJsonDeserializer.deserialize(p.codec, value) }
          return RecordHeader(key?.textValue(), valueBarr)
        }
      }
    } else if (node.isObject) {
      val key: String?
      val value: JsonNode?
      if (node.size() == 1 && !node.has("key")) {
        key = node.fieldNames().next()
        value = node.get(key)
      } else {
        key = node.get("key").takeIf { it.isTextual }?.textValue()
        value = node.get("value")
      }
      val valueBarr = value?.let { ByteArrayJsonDeserializer.deserialize(p.codec, value) }
      return RecordHeader(key, valueBarr)
    }
    return reportInputMismatch(ctxt, node)
  }

  companion object {
    @JvmStatic
    private fun get(node: JsonNode?, index: Int): JsonNode? {
      return node?.takeIf { it.size() > index }?.get(index)?.takeIf { !it.isNull }
    }

    @JvmStatic
    private fun reportInputMismatch(
        ctxt: DeserializationContext,
        node: JsonNode? = null,
    ): Header {
      return ctxt.reportInputMismatch<Header>(
          Header::class.java,
          buildString {
            append("Invalid header format")
            if (node != null) {
              append(": $node")
            }
          },
      )
    }
  }
}