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
import reactor.kafka.sender.SenderRecord

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

  fun <T> toSenderRecord(correlationMetadata: T? = null): SenderRecord<Bytes, Bytes, T> {
    return SenderRecord.create(this, correlationMetadata)
  }

  companion object {
    @JvmStatic
    private val RECORD_FIELDS =
        listOf(
            ProducerData::key,
            ProducerData::value,
            ProducerData::headers,
        )

    @JvmStatic
    fun read(mapper: ObjectMapper, topic: String, input: String?): List<ProducerData> {
      return read(mapper, topic, readTree(mapper, input), true)
    }

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
                .onFailure { it.printStackTrace() }
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

    @JvmStatic
    private fun readTree(mapper: ObjectMapper, input: String?): JsonNode? {
      val json = input?.trim()?.takeIf { it.isNotEmpty() }
      if (json == null) return null
      val node = runCatching { mapper.readTree(json) }.getOrNull()
      return if (node == null) TextNode.valueOf(json) else node.takeIf { !it.isNull }
    }
  }
}
