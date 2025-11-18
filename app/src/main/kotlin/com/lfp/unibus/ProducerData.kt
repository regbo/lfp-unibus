package com.lfp.unibus

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ContainerNode
import kotlin.reflect.KProperty1

/**
 * Data class representing a Kafka producer record payload.
 *
 * Used to send messages to Kafka topics via WebSocket. Supports both JSON
 * and binary formats for keys and values. Only one format (JSON or binary)
 * can be specified for each key/value pair.
 *
 * @param partition Optional partition number to send the record to (null for automatic)
 * @param timestamp Optional timestamp in milliseconds (null for current time)
 * @param key The key as a JsonNode (mutually exclusive with keyBinary)
 * @param keyBinary The key as binary data (mutually exclusive with key)
 * @param value The value as a JsonNode (mutually exclusive with valueBinary)
 * @param valueBinary The value as binary data (mutually exclusive with value)
 * @param headers Optional list of record headers
 *
 * @throws IllegalArgumentException if both JSON and binary formats are specified for key or value
 */
@Suppress("ArrayInDataClass")
data class ProducerData(
    val partition: Int?,
    val timestamp: Long?,
    val key: JsonNode?,
    val keyBinary: ByteArray?,
    val value: JsonNode?,
    val valueBinary: ByteArray?,
    val headers: List<Header>?,
) {

  init {
    oneOfCheck(this, ProducerData::key, ProducerData::keyBinary)
    oneOfCheck(this, ProducerData::value, ProducerData::valueBinary)
  }

  companion object {

    /**
     * Validates that only one of two properties is set (mutual exclusivity check).
     *
     * Throws an IllegalArgumentException if both properties have non-null values.
     *
     * @param T The type of the data object
     * @param data The data object to validate
     * @param nodeProperty The property representing the JSON format
     * @param binaryProperty The property representing the binary format
     * @throws IllegalArgumentException if both properties are non-null
     */
    @JvmStatic
    fun <T> oneOfCheck(
        data: T,
        nodeProperty: KProperty1<T, JsonNode?>,
        binaryProperty: KProperty1<T, ByteArray?>,
    ) {
      if (nodeProperty.get(data) != null && binaryProperty.get(data) != null) {
        throw IllegalArgumentException(
            "${data!!::class.simpleName} Error: Only one of ${nodeProperty.name} or ${binaryProperty.name} may be set"
        )
      }
    }
  }

  /**
   * Represents a Kafka record header with flexible value format.
   *
   * Header values can be provided as either JSON (value) or binary (valueBinary),
   * but not both. Supports multiple JSON input formats for convenience.
   *
   * @param key The header key (null if not present)
   * @param value The header value as a JsonNode (mutually exclusive with valueBinary)
   * @param valueBinary The header value as binary data (mutually exclusive with value)
   *
   * @throws IllegalArgumentException if both value and valueBinary are specified
   */
  data class Header(val key: String?, val value: JsonNode?, val valueBinary: ByteArray?) {

    init {
      oneOfCheck(this, Header::value, Header::valueBinary)
    }

    companion object {

      /**
       * Creates a Header from various JSON node formats.
       *
       * Supports the following input formats:
       * - Array with 1-2 elements: [key, value] or [key]
       * - Object with single key-value pair: {"key": value}
       * - Object with explicit fields: {"key": "...", "value": ..., "valueBinary": "..."}
       *
       * @param node The ContainerNode (array or object) containing header data
       * @return Header instance created from the node
       * @throws IllegalArgumentException if the node format is invalid
       */
      @JvmStatic
      @JsonCreator()
      fun create(node: ContainerNode<*>): Header {
        if (node.isArray || (node.isObject && node.size() == 1)) {
          if (node.isArray && !node.isEmpty && node.size() <= 2) {
            val key = node.get(0)?.textValue()
            val value = if (node.size() > 1) node.get(1) else null
            return Header(key, value, null)
          } else if (node.isObject && node.size() == 1) {
            val key = node.fieldNames().next()
            val value = node.get(key)
            return Header(key, value, null)
          }
          throw IllegalArgumentException(
              "${Header::class.simpleName} Error: Invalid pair $node"
          )
        } else {
          return Header(
              node.get(Header::key.name).textValue(),
              node.get(Header::value.name),
              node.get(Header::valueBinary.name)?.let {
                ByteArrayDeserializer.deserialize(it.textValue())
              },
          )
        }
      }
    }
  }
}
