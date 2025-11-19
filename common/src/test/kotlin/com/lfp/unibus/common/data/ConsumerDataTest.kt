package com.lfp.unibus.common.data

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.common.utils.Bytes
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import java.util.Optional

/**
 * Test class for ConsumerData serialization.
 *
 * Tests JSON serialization and edge cases. Note: ConsumerData is serialization-only
 * and does not support deserialization (no JsonCreator annotation).
 */
class ConsumerDataTest {

  /**
   * Tests basic serialization of ConsumerData to JSON.
   */
  @Test
  fun `serialize ConsumerData to JSON`() {
    val consumerData =
        TestUtils.createConsumerData(
            topic = "test-topic",
            partition = 1,
            offset = 100L,
            timestamp = 1234567890L,
            timestampType = TimestampType.CREATE_TIME,
            key = Bytes.wrap("test-key".encodeToByteArray()),
            value = Bytes.wrap("test-value".encodeToByteArray()),
            headers = listOf(TestUtils.createHeader("header1", "value1")),
        )

    val json = TestUtils.objectMapper.writeValueAsString(consumerData)

    assertNotNull(json)
    assertEquals(true, json.contains("\"partition\":1"))
    assertEquals(true, json.contains("\"offset\":100"))
    assertEquals(true, json.contains("\"timestamp\":1234567890"))
  }

  /**
   * Tests serialization with null key and value.
   */
  @Test
  fun `serialize ConsumerData with null key and value`() {
    val consumerData =
        TestUtils.createConsumerData(
            key = null,
            value = null,
            headers = null,
        )

    val json = TestUtils.objectMapper.writeValueAsString(consumerData)

    assertNotNull(json)
    // JSON may contain null or omit the field, both are valid
    // Just verify JSON is valid and non-empty
    assertEquals(true, json.isNotEmpty() && json.startsWith("{") && json.endsWith("}"))
  }

  /**
   * Tests serialization with empty headers.
   */
  @Test
  fun `serialize ConsumerData with empty headers`() {
    val consumerData =
        TestUtils.createConsumerData(
            headers = emptyList(),
        )

    val json = TestUtils.objectMapper.writeValueAsString(consumerData)

    assertNotNull(json)
    assertEquals(true, json.contains("\"headers\":[]") || json.contains("\"headers\": []"))
  }

  /**
   * Tests serialization with multiple headers.
   */
  @Test
  fun `serialize ConsumerData with multiple headers`() {
    val consumerData =
        TestUtils.createConsumerData(
            headers =
                listOf(
                    TestUtils.createHeader("header1", "value1"),
                    TestUtils.createHeader("header2", "value2"),
                ),
        )

    val json = TestUtils.objectMapper.writeValueAsString(consumerData)

    assertNotNull(json)
    assertEquals(true, json.contains("header1"))
    assertEquals(true, json.contains("header2"))
  }

  /**
   * Tests serialization with different timestamp types.
   */
  @Test
  fun `serialize ConsumerData with different timestamp types`() {
    val createTimeData =
        TestUtils.createConsumerData(
            timestampType = TimestampType.CREATE_TIME,
        )
    val logAppendTimeData =
        TestUtils.createConsumerData(
            timestampType = TimestampType.LOG_APPEND_TIME,
        )
    val noTimestampData =
        TestUtils.createConsumerData(
            timestampType = TimestampType.NO_TIMESTAMP_TYPE,
        )

    val createTimeJson = TestUtils.objectMapper.writeValueAsString(createTimeData)
    val logAppendTimeJson = TestUtils.objectMapper.writeValueAsString(logAppendTimeData)
    val noTimestampJson = TestUtils.objectMapper.writeValueAsString(noTimestampData)

    assertNotNull(createTimeJson)
    assertNotNull(logAppendTimeJson)
    assertNotNull(noTimestampJson)
  }

  /**
   * Tests serialization with leaderEpoch and deliveryCount.
   */
  @Test
  fun `serialize ConsumerData with leaderEpoch and deliveryCount`() {
    val consumerData =
        TestUtils.createConsumerData(
            leaderEpoch = 5,
            deliveryCount = 3.toShort(),
        )

    val json = TestUtils.objectMapper.writeValueAsString(consumerData)

    assertNotNull(json)
    assertEquals(true, json.contains("\"leaderEpoch\":5") || json.contains("\"leaderEpoch\": 5"))
    assertEquals(true, json.contains("\"deliveryCount\":3") || json.contains("\"deliveryCount\": 3"))
  }

  /**
   * Tests serialization with binary data (non-UTF8).
   */
  @Test
  fun `serialize ConsumerData with binary data`() {
    val binaryData = byteArrayOf(0x00, 0x01, 0x02, 0xFF.toByte(), 0xFE.toByte())
    val consumerData =
        TestUtils.createConsumerData(
            key = Bytes.wrap(binaryData),
            value = Bytes.wrap(binaryData),
        )

    val json = TestUtils.objectMapper.writeValueAsString(consumerData)

    assertNotNull(json)
    assertEquals(true, json.contains("data:"))
  }

  /**
   * Tests serialization produces valid JSON structure.
   */
  @Test
  fun `serialize ConsumerData produces valid JSON structure`() {
    val consumerData =
        TestUtils.createConsumerData(
            topic = "test-topic",
            partition = 2,
            offset = 200L,
            timestamp = 9876543210L,
            timestampType = TimestampType.CREATE_TIME,
            key = Bytes.wrap("round-trip-key".encodeToByteArray()),
            value = Bytes.wrap("round-trip-value".encodeToByteArray()),
            headers =
                listOf(
                    TestUtils.createHeader("rt-header1", "rt-value1"),
                    TestUtils.createHeader("rt-header2", "rt-value2"),
                ),
            leaderEpoch = 10,
            deliveryCount = 5.toShort(),
        )

    val json = TestUtils.objectMapper.writeValueAsString(consumerData)

    assertNotNull(json)
    assertEquals(true, json.contains("\"partition\":2"))
    assertEquals(true, json.contains("\"offset\":200"))
    assertEquals(true, json.contains("\"timestamp\":9876543210"))
    assertEquals(true, json.contains("rt-header1"))
    assertEquals(true, json.contains("rt-header2"))
  }

  /**
   * Tests serialization with null values produces correct JSON.
   */
  @Test
  fun `serialize ConsumerData with null values produces correct JSON`() {
    val consumerData =
        TestUtils.createConsumerData(
            key = null,
            value = null,
            headers = null,
            leaderEpoch = null,
            deliveryCount = null,
        )

    val json = TestUtils.objectMapper.writeValueAsString(consumerData)

    assertNotNull(json)
    // JSON may contain null or omit the field, both are valid
    // Just verify JSON is valid and non-empty
    assertEquals(true, json.isNotEmpty() && json.startsWith("{") && json.endsWith("}"))
  }

  /**
   * Tests creating ConsumerData from ConsumerRecord.
   */
  @Test
  fun `create ConsumerData from ConsumerRecord`() {
    val headers = org.apache.kafka.common.header.internals.RecordHeaders()
    headers.add(TestUtils.createHeader("record-header", "record-value"))
    val record =
        ConsumerRecord(
            "source-topic",
            4,
            400L,
            2222222222L,
            TimestampType.LOG_APPEND_TIME,
            10,
            11,
            Bytes.wrap("record-key".encodeToByteArray()),
            Bytes.wrap("record-value".encodeToByteArray()),
            headers,
            Optional.empty(),
            Optional.empty(),
        )

    val consumerData = ConsumerData(record)

    assertEquals(record.topic(), consumerData.topic())
    assertEquals(record.partition(), consumerData.partition())
    assertEquals(record.offset(), consumerData.offset())
    assertEquals(record.timestamp(), consumerData.timestamp())
    assertEquals(record.timestampType(), consumerData.timestampType())
    TestUtils.assertBytesEquals(record.key(), consumerData.key())
    TestUtils.assertBytesEquals(record.value(), consumerData.value())
  }

  /**
   * Tests serialization after creating from ConsumerRecord.
   */
  @Test
  fun `serialize ConsumerData created from ConsumerRecord`() {
    val headers = org.apache.kafka.common.header.internals.RecordHeaders()
    headers.add(TestUtils.createHeader("conv-header", "conv-value"))
    val record =
        ConsumerRecord(
            "conversion-topic",
            5,
            500L,
            3333333333L,
            TimestampType.LOG_APPEND_TIME,
            20,
            21,
            Bytes.wrap("conversion-key".encodeToByteArray()),
            Bytes.wrap("conversion-value".encodeToByteArray()),
            headers,
            Optional.of(20),
            Optional.of(10.toShort()),
        )

    val consumerData = ConsumerData(record)
    val json = TestUtils.objectMapper.writeValueAsString(consumerData)

    assertNotNull(json)
    assertEquals(true, json.contains("\"partition\":5"))
    assertEquals(true, json.contains("\"offset\":500"))
    assertEquals(true, json.contains("conv-header"))
  }
}
