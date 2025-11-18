package com.lfp.unibus

import com.fasterxml.jackson.databind.JsonNode
import org.apache.kafka.common.record.TimestampType

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

  data class Header(val key: String?, val value: JsonNode?) {}
}
