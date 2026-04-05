package com.lfp.unibus.common

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.json.JsonMapper
import com.lfp.unibus.common.data.DataUrl
import com.lfp.unibus.common.data.ProducerData
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.utils.Bytes
import org.junit.jupiter.api.Assertions
import kotlin.io.encoding.Base64

/** Test utilities for common test data and helper functions. */
object TestUtils {
  val objectMapper: ObjectMapper = run {
    return@run JsonMapper.builder().findAndAddModules().build()
  }
  const val imageBase64 =
      "iVBORw0KGgoAAAANSUhEUgAAAAoAAAAKCAIAAAACUFjqAAAAEklEQVR4nGP8z4APMOGVHbHSAEEsAROxCnMTAAAAAElFTkSuQmCC"
  val imageDataUrl = DataUrl(data = Base64.Default.decode(imageBase64))



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
        Assertions.assertEquals(expected, actual, message)
      return
    }
    val expectedBytes = expected.get()
    val actualBytes = actual.get()
    if (expectedBytes == null && actualBytes == null) return
    if (expectedBytes == null || actualBytes == null) {
        Assertions.assertEquals(expectedBytes?.contentToString(), actualBytes?.contentToString(), message)
      return
    }
      Assertions.assertEquals(
          expectedBytes.contentToString(),
          actualBytes.contentToString(),
          message,
      )
  }
}