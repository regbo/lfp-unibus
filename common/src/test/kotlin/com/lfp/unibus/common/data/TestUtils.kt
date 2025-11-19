package com.lfp.unibus.common.data

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.json.JsonMapper
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.common.utils.Bytes
import org.junit.jupiter.api.Assertions.assertEquals
import kotlin.io.encoding.Base64

/** Test utilities for common test data and helper functions. */
object TestUtils {
  val objectMapper: ObjectMapper = run {
    return@run JsonMapper.builder().findAndAddModules().build()
  }
  const val imageBase64 =
      "iVBORw0KGgoAAAANSUhEUgAAAAoAAAAKCAIAAAACUFjqAAAAEklEQVR4nGP8z4APMOGVHbHSAEEsAROxCnMTAAAAAElFTkSuQmCC"
  val imageDataUrl = DataUrl(data = Base64.decode(imageBase64))

  /**
   * Creates a test ConsumerData instance with default values.
   *
   * @param topic Topic name (default: "test-topic")
   * @param partition Partition number (default: 0)
   * @param offset Offset value (default: 0L)
   * @param timestamp Timestamp value (default: System.currentTimeMillis())
   * @param timestampType Timestamp type (default: TimestampType.CREATE_TIME)
   * @param key Optional key bytes
   * @param value Optional value bytes
   * @param headers Optional headers list
   * @param leaderEpoch Optional leader epoch
   * @param deliveryCount Optional delivery count
   * @return ConsumerData instance
   */
  @JvmStatic
  fun createConsumerData(
      topic: String = "test-topic",
      partition: Int = 0,
      offset: Long = 0L,
      timestamp: Long = System.currentTimeMillis(),
      timestampType: TimestampType = TimestampType.CREATE_TIME,
      key: Bytes? = null,
      value: Bytes? = null,
      headers: List<Header>? = null,
      leaderEpoch: Int? = null,
      deliveryCount: Short? = null,
  ): ConsumerData {
    val keyBytes = key ?: Bytes.wrap("test-key".encodeToByteArray())
    val valueBytes = value ?: Bytes.wrap("test-value".encodeToByteArray())
    val keySize = keyBytes.get().size
    val valueSize = valueBytes.get().size
    return ConsumerData(
        topic = topic,
        partition = partition,
        offset = offset,
        timestamp = timestamp,
        timestampType = timestampType,
        serializedKeySize = keySize,
        serializedValueSize = valueSize,
        key = keyBytes,
        value = valueBytes,
        headers = headers,
        leaderEpoch = leaderEpoch,
        deliveryCount = deliveryCount,
    )
  }

  /**
   * Creates a test ProducerData instance with default values.
   *
   * @param topic Topic name (default: "test-topic")
   * @param partition Optional partition number
   * @param timestamp Optional timestamp value
   * @param key Optional key bytes
   * @param value Optional value bytes
   * @param headers Optional headers list
   * @return ProducerData instance
   */
  @JvmStatic
  fun createProducerData(
      topic: String = "test-topic",
      partition: Int? = null,
      timestamp: Long? = null,
      key: Bytes? = null,
      value: Bytes? = null,
      headers: Collection<Header?>? = null,
  ): ProducerData {
    return ProducerData(
        topic = topic,
        partition = partition,
        timestamp = timestamp,
        key = key,
        value = value,
        headers = headers,
    )
  }

  /**
   * Creates a test RecordHeader instance from string value.
   *
   * @param key Header key
   * @param value Header value as string
   * @return RecordHeader instance
   */
  @JvmStatic
  fun createHeader(key: String, value: String): RecordHeader {
    return RecordHeader(key, value.encodeToByteArray())
  }

  /**
   * Asserts that two Bytes instances are equal.
   *
   * @param expected Expected Bytes
   * @param actual Actual Bytes
   * @param message Error message prefix
   */
  @JvmStatic
  fun assertBytesEquals(expected: Bytes?, actual: Bytes?, message: String = "") {
    if (expected == null && actual == null) return
    if (expected == null || actual == null) {
      assertEquals(expected, actual, message)
      return
    }
    val expectedBytes = expected.get()
    val actualBytes = actual.get()
    if (expectedBytes == null && actualBytes == null) return
    if (expectedBytes == null || actualBytes == null) {
      assertEquals(expectedBytes?.contentToString(), actualBytes?.contentToString(), message)
      return
    }
    assertEquals(
        expectedBytes.contentToString(),
        actualBytes.contentToString(),
        message,
    )
  }

  /**
   * Asserts that two Headers collections are equal.
   *
   * @param expected Expected Headers
   * @param actual Actual Headers
   * @param message Error message prefix
   */
  @JvmStatic
  fun assertHeadersEquals(
      expected: org.apache.kafka.common.header.Headers?,
      actual: org.apache.kafka.common.header.Headers?,
      message: String = "",
  ) {
    if (expected == null && actual == null) return
    if (expected == null || actual == null) {
      assertEquals(expected, actual, message)
      return
    }
    val expectedList = expected.toList()
    val actualList = actual.toList()
    assertEquals(expectedList.size, actualList.size, "$message: Header count mismatch")
    expectedList.forEachIndexed { index, expectedHeader ->
      val actualHeader = actualList[index]
      assertEquals(
          expectedHeader.key(),
          actualHeader.key(),
          "$message: Header[$index] key mismatch",
      )
      assertEquals(
          expectedHeader.value()?.contentToString(),
          actualHeader.value()?.contentToString(),
          "$message: Header[$index] value mismatch",
      )
    }
  }
}
