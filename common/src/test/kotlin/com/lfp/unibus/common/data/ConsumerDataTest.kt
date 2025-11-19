package com.lfp.unibus.common.data

import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.common.utils.Bytes
import org.junit.jupiter.api.Test

class ConsumerDataTest {

  @Test
  fun write() {
    var consumerData =
        ConsumerData(
            topic = "topic_name",
            partition = 0,
            offset = 0,
            timestamp = System.currentTimeMillis(),
            timestampType = TimestampType.CREATE_TIME,
            serializedKeySize = 5,
            serializedValueSize = 5,
            key = Bytes.wrap("wow".encodeToByteArray()),
            value = Bytes.wrap("neat".encodeToByteArray()),
            listOf(RecordHeader("name", "value".encodeToByteArray())),
        )
    print(TestUtils.objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(consumerData))
  }
}
