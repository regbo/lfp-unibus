package com.lfp.unibus.common.data

import org.junit.jupiter.api.Test

class ProducerDataTest {

  @Test
  fun read() {
    val json =
        """
        [{"headers":[["name", "value"]], "value":"${TestUtils.imageDataUrl}"},"${TestUtils.imageDataUrl}" ]
        """
    val data = ProducerData.read(TestUtils.objectMapper, "topic_name", json)
    println(data)
  }

  @Test
  fun readValue() {
    val json =
        """
        ["${TestUtils.imageDataUrl}" ]
        """
    val data = ProducerData.read(TestUtils.objectMapper, "topic_name", json)
    println(data)
  }
}
