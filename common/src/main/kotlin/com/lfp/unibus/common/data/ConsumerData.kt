package com.lfp.unibus.common.data

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import com.lfp.unibus.common.json.serializer.BytesJsonSerializer
import com.lfp.unibus.common.json.serializer.HeaderJsonSerializer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.common.utils.Bytes
import java.util.*

class ConsumerData(
    topic: String,
    partition: Int,
    offset: Long,
    timestamp: Long,
    timestampType: TimestampType,
    serializedKeySize: Int,
    serializedValueSize: Int,
    key: Bytes? = null,
    value: Bytes? = null,
    headers: Iterable<Header>? = null,
    leaderEpoch: Int? = null,
    deliveryCount: Short? = null,
) :
    ConsumerRecord<Bytes, Bytes>(
        topic,
        partition,
        offset,
        timestamp,
        timestampType,
        serializedKeySize,
        serializedValueSize,
        key,
        value,
        headers?.let {
          if (it !is Headers) {
            val h = RecordHeaders()
            it.forEach { header -> h.add(header) }
            h
          } else it
        } ?: RecordHeaders(),
        Optional.ofNullable(leaderEpoch),
        Optional.ofNullable(deliveryCount),
    ) {
  constructor(
      record: ConsumerRecord<Bytes, Bytes>
  ) : this(
      topic = record.topic(),
      partition = record.partition(),
      offset = record.offset(),
      timestamp = record.timestamp(),
      timestampType = record.timestampType(),
      serializedKeySize = record.serializedKeySize(),
      serializedValueSize = record.serializedValueSize(),
      key = record.key(),
      value = record.value(),
      headers = record.headers(),
      leaderEpoch = record.leaderEpoch().orElse(null),
      deliveryCount = record.deliveryCount().orElse(null),
  )

  @JsonIgnore
  override fun topic(): String {
    return super.topic()
  }

  @JsonProperty
  override fun partition(): Int {
    return super.partition()
  }

  @JsonProperty
  override fun offset(): Long {
    return super.offset()
  }

  @JsonProperty
  override fun timestamp(): Long {
    return super.timestamp()
  }

  @JsonProperty
  override fun timestampType(): TimestampType {
    return super.timestampType()
  }

  @JsonProperty
  override fun serializedKeySize(): Int {
    return super.serializedKeySize()
  }

  @JsonProperty
  override fun serializedValueSize(): Int {
    return super.serializedValueSize()
  }

  @JsonProperty
  @JsonSerialize(using = BytesJsonSerializer::class)
  override fun key(): Bytes? {
    return super.key()
  }

  @JsonProperty
  @JsonSerialize(using = BytesJsonSerializer::class)
  override fun value(): Bytes? {
    return super.value()
  }

  @JsonProperty
  @JsonSerialize(`as` = Iterable::class, contentUsing = HeaderJsonSerializer::class)
  override fun headers(): Headers {
    return super.headers()
  }

  @JsonProperty
  override fun leaderEpoch(): Optional<Int> {
    return super.leaderEpoch()
  }

  @JsonProperty
  override fun deliveryCount(): Optional<Short> {
    return super.deliveryCount()
  }
}
