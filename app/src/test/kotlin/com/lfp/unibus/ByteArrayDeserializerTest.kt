package com.lfp.unibus

import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.util.*

/**
 * Test class for ByteArrayDeserializer.
 *
 * Tests Base64 decoding and Data URL format parsing.
 */
class ByteArrayDeserializerTest {

  private val objectMapper = ObjectMapper()

  @Test
  fun `deserialize plain Base64 string`() {
    val input = "SGVsbG8gV29ybGQ="
    val result = ByteArrayDeserializer.deserialize(input)
    val expected = "Hello World".toByteArray()
    assertArrayEquals(expected, result)
  }

  @Test
  fun `deserialize Data URL with base64 encoding`() {
    val base64Data = "SGVsbG8gV29ybGQ="
    val input = "data:application/octet-stream;base64,$base64Data"
    val result = ByteArrayDeserializer.deserialize(input)
    val expected = "Hello World".toByteArray()
    assertArrayEquals(expected, result)
  }

  @Test
  fun `deserialize Data URL with media type`() {
    val base64Data = "dGVzdA=="
    val input = "data:text/plain;base64,$base64Data"
    val result = ByteArrayDeserializer.deserialize(input)
    val expected = "test".toByteArray()
    assertArrayEquals(expected, result)
  }

  @Test
  fun `deserialize Data URL without encoding`() {
    val base64Data = "dGVzdA=="
    val input = "data:application/octet-stream,$base64Data"
    val result = ByteArrayDeserializer.deserialize(input)
    val expected = "test".toByteArray()
    assertArrayEquals(expected, result)
  }

  @Test
  fun `deserialize empty Base64 string`() {
    val input = ""
    val result = ByteArrayDeserializer.deserialize(input)
    assertArrayEquals(ByteArray(0), result)
  }

  @Test
  fun `deserialize through JsonDeserializer interface`() {
    val module = com.fasterxml.jackson.databind.module.SimpleModule()
    module.addDeserializer(ByteArray::class.java, ByteArrayDeserializer())
    val mapper = ObjectMapper().registerModule(module)

    val json = "\"SGVsbG8gV29ybGQ=\""
    val result: ByteArray = mapper.readValue(json, ByteArray::class.java)
    val expected = "Hello World".toByteArray()
    assertArrayEquals(expected, result)
  }

  @Test
  fun `deserialize Data URL through JsonDeserializer interface`() {
    val module = com.fasterxml.jackson.databind.module.SimpleModule()
    module.addDeserializer(ByteArray::class.java, ByteArrayDeserializer())
    val mapper = ObjectMapper().registerModule(module)

    val json = "\"data:application/octet-stream;base64,SGVsbG8gV29ybGQ=\""
    val result: ByteArray = mapper.readValue(json, ByteArray::class.java)
    val expected = "Hello World".toByteArray()
    assertArrayEquals(expected, result)
  }
}

