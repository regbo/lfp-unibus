package com.lfp.unibus.common.data

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.json.JsonMapper
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.RecordHeader
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
}
