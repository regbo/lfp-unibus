package com.lfp.unibus.common.data

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.node.TextNode
import com.lfp.unibus.common.json.deserializer.ByteArrayJsonDeserializer
import com.lfp.unibus.common.json.deserializer.BytesJsonDeserializer
import com.lfp.unibus.common.json.deserializer.HeaderJsonDeserializer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.utils.Bytes
import org.slf4j.LoggerFactory
import reactor.kafka.sender.SenderRecord

/**
 * Kafka producer record data model.
 *
 * Extends ProducerRecord with JSON deserialization support. Can be created from JSON or used
 * directly as a ProducerRecord.
 *
 * @param topic Kafka topic name
 * @param partition Optional partition number (null for automatic assignment)
 * @param timestamp Optional timestamp in milliseconds (null for current time)
 * @param key Optional message key (deserialized from various formats)
 * @param value Optional message value (deserialized from various formats)
 * @param headers Optional collection of message headers
 */
class ProducerData
@JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
constructor(
    @JsonProperty("topic") topic: String,
    @JsonProperty("partition") partition: Int? = null,
    @JsonProperty("timestamp") timestamp: Long? = null,
    @JsonProperty("key") @JsonDeserialize(using = BytesJsonDeserializer::class) key: Bytes? = null,
    @JsonProperty("value")
    @JsonDeserialize(using = BytesJsonDeserializer::class)
    value: Bytes? = null,
    @JsonProperty("headers")
    @JsonDeserialize(contentUsing = HeaderJsonDeserializer::class)
    headers: Collection<Header?>? = null,
) : ProducerRecord<Bytes, Bytes>(topic, partition, timestamp, key, value, headers) {

  /**
   * Converts to Reactor Kafka SenderRecord.
   *
   * @param correlationMetadata Optional correlation metadata
   * @return SenderRecord ready for sending
   */
  fun <T> toSenderRecord(correlationMetadata: T? = null): SenderRecord<Bytes, Bytes, T> {
    return SenderRecord.create(this, correlationMetadata)
  }

  companion object {
    private val log = LoggerFactory.getLogger(ProducerData::class.java)
    @JvmStatic
    private val RECORD_FIELDS =
        listOf(
            ProducerData::key,
            ProducerData::value,
            ProducerData::headers,
        )

    /**
     * Reads ProducerData from JSON string.
     *
     * Supports single objects or arrays. If input is not a record structure, treats entire input as value.
     *
     * @param mapper ObjectMapper for JSON parsing
     * @param topic Kafka topic name
     * @param input JSON string input
     * @return List of ProducerData instances
     */
    @JvmStatic
    fun read(mapper: ObjectMapper, topic: String, input: String?): List<ProducerData> {
      return read(mapper, topic, readTree(mapper, input), true)
    }

    /**
     * Recursively parses the provided JSON node into ProducerData instances.
     *
     * Arrays are flattened when `flatten` is true, objects that resemble a record structure are
     * converted directly, and any other payload is treated as a raw value for the supplied topic.
     *
     * @param mapper ObjectMapper for JSON conversion
     * @param topic Kafka topic name
     * @param node Parsed JSON node (object, array, or primitive)
     * @param flatten Whether to flatten top level arrays
     * @return List of ProducerData instances derived from the node
     */
    @JvmStatic
    private fun read(
        mapper: ObjectMapper,
        topic: String,
        node: JsonNode?,
        flatten: Boolean = false,
    ): List<ProducerData> {
      if (node == null || node.isNull) return emptyList()
      else if (flatten && node.isArray) {
        return node.flatMap { read(mapper, topic, it) }
      } else if (node.isObject && RECORD_FIELDS.any { node.has(it.name) }) {
        (node as ObjectNode).put(ProducerData::topic.name, topic)
        val data =
            runCatching { mapper.convertValue(node, ProducerData::class.java) }
                .onFailure { error ->
                  // Log conversion failures to aid debugging without interrupting batch processing.
                  log.warn("Failed to convert JSON node into ProducerData for topic={}", topic, error)
                }
                .getOrNull()
        if (data != null) return listOf(data)
      }
      val value = ByteArrayJsonDeserializer.deserialize(mapper, node)?.let { Bytes.wrap(it) }
      return listOf(
          ProducerData(
              topic = topic,
              value = value,
              partition = null,
              timestamp = null,
              key = null,
              headers = null,
          )
      )
    }

    /**
     * Parses raw JSON text into a JsonNode.
     *
     * Attempts to read structured JSON, but if parsing fails returns a text node so callers can
     * treat the payload as a raw value.
     *
     * @param mapper ObjectMapper for parsing
     * @param input Raw JSON string
     * @return Parsed JsonNode or null when the input is blank
     */
    @JvmStatic
    private fun readTree(mapper: ObjectMapper, input: String?): JsonNode? {
      val json = input?.trim()?.takeIf { it.isNotEmpty() }
      if (json == null) return null
      val node = runCatching { mapper.readTree(json) }.getOrNull()
      return if (node == null) TextNode.valueOf(json) else node.takeIf { !it.isNull }
    }
  }
}
