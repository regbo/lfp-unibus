package com.lfp.unibus

import com.fasterxml.jackson.databind.JsonNode
import org.apache.kafka.common.record.TimestampType

/**
 * Data class representing a Kafka consumer record payload.
 *
 * Contains all metadata and content from a consumed Kafka message,
 * including partition, offset, timestamps, headers, key, and value.
 *
 * @param partition The partition number from which the record was consumed
 * @param offset The offset of the record in the partition
 * @param timestamp The timestamp of the record (milliseconds since epoch)
 * @param timestampType The type of timestamp (CREATE_TIME or LOG_APPEND_TIME)
 * @param serializedKeySize The size of the serialized key in bytes
 * @param serializedValueSize The size of the serialized value in bytes
 * @param headers Optional list of record headers
 * @param key The deserialized key as a JsonNode (null if no key)
 * @param value The deserialized value as a JsonNode (null if no value)
 * @param leaderEpoch Optional leader epoch for the record
 * @param deliveryCount Optional delivery count (for idempotent consumers)
 */
data class ConsumerData(
    val partition: Int,
    val offset: Long,
    val timestamp: Long,
    val timestampType: TimestampType,
    val serializedKeySize: Int,
    val serializedValueSize: Int,
    val headers: List<Header>?,
    val key: JsonNode?,
    val value: JsonNode?,
    val leaderEpoch: Int?,
    val deliveryCount: Short?,
) {

  /**
   * Represents a Kafka record header.
   *
   * @param key The header key (null if not present)
   * @param value The header value as a JsonNode (null if not present)
   */
  data class Header(val key: String?, val value: JsonNode?) {}
}
