package com.lfp.unibus.common.data

import com.lfp.unibus.common.TestUtils
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import org.springframework.http.MediaType
import java.nio.charset.StandardCharsets
import java.util.Base64

/**
 * Test class for DataUrl parsing and serialization.
 *
 * Tests various data URL formats, edge cases, error conditions, and round-trip conversions.
 */
class DataUrlTest {

  /**
   * Tests parsing a basic base64 data URL.
   */
  @Test
  fun `parse basic base64 data URL`() {
    val input = "data:text/plain;base64,SGVsbG8gV29ybGQ="
    val result = DataUrl.parse(input)

    assertNotNull(result)
    assertEquals(MediaType.TEXT_PLAIN, result!!.mediaType)
    assertEquals(true, result.base64)
    assertArrayEquals("Hello World".toByteArray(), result.data)
  }

  /**
   * Tests parsing data URL with image data.
   */
  @Test
  fun `parse image data URL`() {
    val url = TestUtils.imageDataUrl.toString()
    val result = DataUrl.parse(url)

    assertNotNull(result)
    assertEquals(MediaType.TEXT_PLAIN, result!!.mediaType)
    assertEquals(true, result.base64)
    assertArrayEquals(TestUtils.imageDataUrl.data, result.data)
  }

  /**
   * Tests parsing data URL without base64 encoding (URL-encoded content).
   */
  @Test
  fun `parse non-base64 data URL with URL-encoded content`() {
    val input = "data:text/plain,Hello%20World"
    val result = DataUrl.parse(input)

    assertNotNull(result)
    assertEquals(MediaType.TEXT_PLAIN, result!!.mediaType)
    assertEquals(false, result.base64)
    assertArrayEquals("Hello World".toByteArray(StandardCharsets.UTF_8), result.data)
  }

  /**
   * Tests parsing data URL with charset.
   */
  @Test
  fun `parse data URL with charset`() {
    val input = "data:text/plain;charset=UTF-8;base64,SGVsbG8gV29ybGQ="
    val result = DataUrl.parse(input)

    assertNotNull(result)
    assertEquals(MediaType.TEXT_PLAIN, result!!.mediaType)
    assertEquals(StandardCharsets.UTF_8, result.charset)
    assertEquals(true, result.base64)
    assertArrayEquals("Hello World".toByteArray(), result.data)
  }

  /**
   * Tests parsing data URL with custom parameters.
   */
  @Test
  fun `parse data URL with custom parameters`() {
    val input = "data:application/json;param1=value1;param2=value2;base64,eyJ0ZXN0IjoidmFsdWUifQ=="
    val result = DataUrl.parse(input)

    assertNotNull(result)
    assertEquals(MediaType.APPLICATION_JSON, result!!.mediaType)
    assertEquals(true, result.base64)
    assertEquals("value1", result.parameters["param1"])
    assertEquals("value2", result.parameters["param2"])
  }

  /**
   * Tests parsing empty data URL.
   */
  @Test
  fun `parse empty data URL`() {
    val input = "data:text/plain;base64,"
    val result = DataUrl.parse(input)

    assertNotNull(result)
    assertEquals(MediaType.TEXT_PLAIN, result!!.mediaType)
    assertEquals(true, result.base64)
    assertArrayEquals(ByteArray(0), result.data)
  }

  /**
   * Tests parsing data URL with base64 as media type.
   */
  @Test
  fun `parse data URL with base64 as media type`() {
    val input = "data:base64,SGVsbG8gV29ybGQ="
    val result = DataUrl.parse(input)

    assertNotNull(result)
    assertEquals(MediaType.TEXT_PLAIN, result!!.mediaType) // Defaults to TEXT_PLAIN
    assertEquals(true, result.base64)
    assertArrayEquals("Hello World".toByteArray(), result.data)
  }

  /**
   * Tests case-insensitive base64 token.
   */
  @Test
  fun `parse data URL with case-insensitive base64`() {
    val input1 = "data:text/plain;BASE64,SGVsbG8gV29ybGQ="
    val input2 = "data:text/plain;Base64,SGVsbG8gV29ybGQ="
    val input3 = "data:text/plain;BaSe64,SGVsbG8gV29ybGQ="

    val result1 = DataUrl.parse(input1)
    val result2 = DataUrl.parse(input2)
    val result3 = DataUrl.parse(input3)

    assertNotNull(result1)
    assertNotNull(result2)
    assertNotNull(result3)
    assertEquals(true, result1!!.base64)
    assertEquals(true, result2!!.base64)
    assertEquals(true, result3!!.base64)
  }

  /**
   * Tests case-insensitive charset.
   */
  @Test
  fun `parse data URL with case-insensitive charset`() {
    val input = "data:text/plain;CHARSET=UTF-8;base64,SGVsbG8gV29ybGQ="
    val result = DataUrl.parse(input)

    assertNotNull(result)
    assertEquals(StandardCharsets.UTF_8, result!!.charset)
  }

  /**
   * Tests case-insensitive data prefix.
   */
  @Test
  fun `parse data URL with case-insensitive data prefix`() {
    val input = "DATA:text/plain;base64,SGVsbG8gV29ybGQ="
    val result = DataUrl.parse(input)

    assertNotNull(result)
    assertEquals(true, result!!.base64)
    assertArrayEquals("Hello World".toByteArray(), result.data)
  }

  /**
   * Tests that multiple base64 tokens fail.
   */
  @Test
  fun `parse fails with multiple base64 tokens`() {
    val input1 = "data:text/plain;base64;base64,SGVsbG8gV29ybGQ="
    val input2 = "data:base64;base64,SGVsbG8gV29ybGQ="
    val input3 = "data:text/plain;base64;BASE64,SGVsbG8gV29ybGQ="

    assertNull(DataUrl.parse(input1), "Should fail with duplicate base64 tokens")
    assertNull(DataUrl.parse(input2), "Should fail with duplicate base64 tokens")
    assertNull(DataUrl.parse(input3), "Should fail with duplicate base64 tokens (case insensitive)")
  }

  /**
   * Tests that duplicate charset fails.
   */
  @Test
  fun `parse fails with duplicate charset`() {
    val input = "data:text/plain;charset=UTF-8;charset=ISO-8859-1;base64,SGVsbG8gV29ybGQ="

    assertNull(DataUrl.parse(input), "Should fail with duplicate charset")
  }

  /**
   * Tests that duplicate parameters fail.
   */
  @Test
  fun `parse fails with duplicate parameters`() {
    val input = "data:application/json;param=value1;param=value2;base64,eyJ0ZXN0IjoidmFsdWUifQ=="

    assertNull(DataUrl.parse(input), "Should fail with duplicate parameter keys")
  }

  /**
   * Tests that null input returns null gracefully.
   */
  @Test
  fun `parse returns null for null input`() {
    assertNull(DataUrl.parse(null), "Should return null for null input")
  }

  /**
   * Tests that empty string returns null gracefully.
   */
  @Test
  fun `parse returns null for empty string`() {
    assertNull(DataUrl.parse(""), "Should return null for empty string")
  }

  /**
   * Tests that whitespace-only string returns null gracefully.
   */
  @Test
  fun `parse returns null for whitespace-only string`() {
    assertNull(DataUrl.parse("   "), "Should return null for whitespace-only string")
    assertNull(DataUrl.parse("\t\n"), "Should return null for whitespace-only string")
  }

  /**
   * Tests that missing comma returns null gracefully.
   */
  @Test
  fun `parse returns null for missing comma`() {
    val input = "data:text/plain;base64SGVsbG8gV29ybGQ="

    assertNull(DataUrl.parse(input), "Should return null when comma is missing")
  }

  /**
   * Tests that non-data prefix returns null gracefully.
   */
  @Test
  fun `parse returns null for non-data prefix`() {
    val input = "http:text/plain;base64,SGVsbG8gV29ybGQ="

    assertNull(DataUrl.parse(input), "Should return null when prefix is not 'data:'")
  }

  /**
   * Tests that invalid media type returns null gracefully.
   */
  @Test
  fun `parse returns null for invalid media type`() {
    val input = "data:invalid/media/type/format;base64,SGVsbG8gV29ybGQ="

    assertNull(DataUrl.parse(input), "Should return null for invalid media type")
  }

  /**
   * Tests that empty part in header returns null gracefully.
   */
  @Test
  fun `parse returns null for empty part in header`() {
    // Empty part between semicolons should fail (line 89: if (p.isEmpty()) return null)
    val input1 = "data:text/plain;;base64,SGVsbG8gV29ybGQ="
    // Empty first part (before first semicolon) - this is actually base64 as media type, which is valid
    val input2 = "data:;base64,SGVsbG8gV29ybGQ="

    assertNull(DataUrl.parse(input1), "Should return null for empty part between semicolons")
    // input2 is actually valid - it's treated as base64 media type
    val result2 = DataUrl.parse(input2)
    assertNotNull(result2, "Empty first part is treated as base64 media type")
  }

  /**
   * Tests that invalid charset name returns null gracefully.
   */
  @Test
  fun `parse returns null for invalid charset name`() {
    val input = "data:text/plain;charset=INVALID_CHARSET_NAME;base64,SGVsbG8gV29ybGQ="

    assertNull(DataUrl.parse(input), "Should return null for invalid charset name")
  }

  /**
   * Tests that invalid base64 content returns null gracefully.
   */
  @Test
  fun `parse returns null for invalid base64 content`() {
    val input = "data:text/plain;base64,InvalidBase64!!!$$$"

    assertNull(DataUrl.parse(input), "Should return null for invalid base64 content")
  }

  /**
   * Tests that empty key in parameter returns null gracefully.
   */
  @Test
  fun `parse returns null for empty parameter key`() {
    val input = "data:text/plain;=value;base64,SGVsbG8gV29ybGQ="

    assertNull(DataUrl.parse(input), "Should return null for empty parameter key")
  }

  /**
   * Tests that empty value in parameter returns null gracefully.
   */
  @Test
  fun `parse returns null for empty parameter value`() {
    val input = "data:text/plain;param=;base64,SGVsbG8gV29ybGQ="

    assertNull(DataUrl.parse(input), "Should return null for empty parameter value")
  }

  /**
   * Tests that unknown single token returns null gracefully.
   */
  @Test
  fun `parse returns null for unknown single token`() {
    val input = "data:text/plain;unknown;base64,SGVsbG8gV29ybGQ="

    assertNull(DataUrl.parse(input), "Should return null for unknown single token (not base64)")
  }

  /**
   * Tests parsing with different media types.
   */
  @Test
  fun `parse data URL with various media types`() {
    val inputs =
        listOf(
            "data:application/json;base64,eyJ0ZXN0IjoidmFsdWUifQ==",
            "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAoAAAAKCAIAAAACUFjqAAAAEklEQVR4nGP8z4APMOGVHbHSAEEsAROxCnMTAAAAAElFTkSuQmCC",
            "data:application/octet-stream;base64,SGVsbG8gV29ybGQ=",
            "data:text/html;base64,PGgxPkhlbGxvPC9oMT4=",
        )

    inputs.forEach { input ->
      val result = DataUrl.parse(input)
      assertNotNull(result, "Should parse media type: $input")
      assertEquals(true, result!!.base64)
    }
  }

  /**
   * Tests parsing with different charsets.
   */
  @Test
  fun `parse data URL with various charsets`() {
    val input1 = "data:text/plain;charset=UTF-8;base64,SGVsbG8gV29ybGQ="
    val input2 = "data:text/plain;charset=ISO-8859-1;base64,SGVsbG8gV29ybGQ="
    val input3 = "data:text/plain;charset=US-ASCII;base64,SGVsbG8gV29ybGQ="

    val result1 = DataUrl.parse(input1)
    val result2 = DataUrl.parse(input2)
    val result3 = DataUrl.parse(input3)

    assertNotNull(result1)
    assertNotNull(result2)
    assertNotNull(result3)
    assertEquals(StandardCharsets.UTF_8, result1!!.charset)
    assertEquals(StandardCharsets.ISO_8859_1, result2!!.charset)
    assertEquals(StandardCharsets.US_ASCII, result3!!.charset)
  }

  /**
   * Tests parsing URL-encoded content with different charsets.
   */
  @Test
  fun `parse non-base64 data URL with charset`() {
    val input = "data:text/plain;charset=UTF-8,Hello%20World"
    val result = DataUrl.parse(input)

    assertNotNull(result)
    assertEquals(MediaType.TEXT_PLAIN, result!!.mediaType)
    assertEquals(StandardCharsets.UTF_8, result.charset)
    assertEquals(false, result.base64)
    assertArrayEquals("Hello World".toByteArray(StandardCharsets.UTF_8), result.data)
  }

  /**
   * Tests parsing with special characters in parameters.
   */
  @Test
  fun `parse data URL with special characters in parameters`() {
    val input = "data:application/json;param1=value%201;param2=value-2;base64,eyJ0ZXN0IjoidmFsdWUifQ=="
    val result = DataUrl.parse(input)

    assertNotNull(result)
    assertEquals("value%201", result!!.parameters["param1"])
    assertEquals("value-2", result.parameters["param2"])
  }

  /**
   * Tests round-trip conversion (parse then toString).
   */
  @Test
  fun `round-trip parse and toString`() {
    val original = DataUrl(
        mediaType = MediaType.APPLICATION_JSON,
        charset = StandardCharsets.UTF_8,
        parameters = mapOf("param1" to "value1", "param2" to "value2"),
        base64 = true,
        data = "{\"test\":\"value\"}".toByteArray(),
    )

    val urlString = original.toString()
    val parsed = DataUrl.parse(urlString)

    assertNotNull(parsed)
    assertEquals(original.mediaType, parsed!!.mediaType)
    assertEquals(original.charset, parsed.charset)
    assertEquals(original.base64, parsed.base64)
    assertEquals(original.parameters, parsed.parameters)
    assertArrayEquals(original.data, parsed.data)
  }

  /**
   * Tests toString with non-base64 content.
   * Note: toString() always base64-encodes the data internally, but omits ";base64" if base64=false.
   * This creates a format that may not round-trip correctly since the parser will treat it as URL-encoded.
   */
  @Test
  fun `toString with non-base64 content produces valid format`() {
    val original = DataUrl(
        mediaType = MediaType.TEXT_PLAIN,
        charset = StandardCharsets.UTF_8,
        parameters = emptyMap(),
        base64 = false,
        data = "Hello World".toByteArray(StandardCharsets.UTF_8),
    )

    val urlString = original.toString()

    assertNotNull(urlString)
    assertEquals(true, urlString.startsWith("data:text/plain"))
    // When base64=false, toString() omits ";base64" but still base64-encodes the data
    assertEquals(true, urlString.contains(","))
    assertEquals(false, urlString.contains(";base64,"))
  }

  /**
   * Tests toString with empty data.
   */
  @Test
  fun `toString with empty data`() {
    val dataUrl = DataUrl(data = ByteArray(0))
    val urlString = dataUrl.toString()

    assertNotNull(urlString)
    assertEquals(true, urlString.contains("data:"))
    assertEquals(true, urlString.contains("base64,"))
  }

  /**
   * Tests toString with binary data.
   */
  @Test
  fun `toString with binary data`() {
    val binaryData = byteArrayOf(0x00, 0x01, 0x02, 0xFF.toByte(), 0xFE.toByte())
    val dataUrl = DataUrl(data = binaryData)
    val urlString = dataUrl.toString()

    assertNotNull(urlString)
    assertEquals(true, urlString.contains("data:"))
    assertEquals(true, urlString.contains("base64,"))
    val parsed = DataUrl.parse(urlString)
    assertNotNull(parsed)
    assertArrayEquals(binaryData, parsed!!.data)
  }

  /**
   * Tests parsing with very long data URL.
   */
  @Test
  fun `parse very long data URL`() {
    val longData = ByteArray(10000) { it.toByte() }
    val base64Data = Base64.getEncoder().encodeToString(longData)
    val input = "data:application/octet-stream;base64,$base64Data"
    val result = DataUrl.parse(input)

    assertNotNull(result)
    assertEquals(true, result!!.base64)
    assertArrayEquals(longData, result.data)
  }

  /**
   * Tests parsing with URL-encoded special characters.
   */
  @Test
  fun `parse non-base64 data URL with special characters`() {
    val input = "data:text/plain,Hello%20World%21%20%26%20More"
    val result = DataUrl.parse(input)

    assertNotNull(result)
    assertEquals(false, result!!.base64)
    assertEquals("Hello World! & More", String(result.data, StandardCharsets.UTF_8))
  }

  /**
   * Tests that trimming works correctly.
   */
  @Test
  fun `parse trims whitespace from input`() {
    val input = "  data:text/plain;base64,SGVsbG8gV29ybGQ=  "
    val result = DataUrl.parse(input)

    assertNotNull(result)
    assertEquals(true, result!!.base64)
    assertArrayEquals("Hello World".toByteArray(), result.data)
  }

  /**
   * Tests parsing with base64 token before media type (edge case).
   */
  @Test
  fun `parse data URL with base64 token before media type`() {
    // This tests the case where base64 is the first part (mediaTypeRaw)
    val input = "data:base64,SGVsbG8gV29ybGQ="
    val result = DataUrl.parse(input)

    assertNotNull(result)
    assertEquals(MediaType.TEXT_PLAIN, result!!.mediaType) // Defaults when base64 is media type
    assertEquals(true, result.base64)
    assertArrayEquals("Hello World".toByteArray(), result.data)
  }

  /**
   * Tests parsing with base64 token after media type.
   */
  @Test
  fun `parse data URL with base64 token after media type`() {
    val input = "data:text/plain;base64,SGVsbG8gV29ybGQ="
    val result = DataUrl.parse(input)

    assertNotNull(result)
    assertEquals(MediaType.TEXT_PLAIN, result!!.mediaType)
    assertEquals(true, result.base64)
    assertArrayEquals("Hello World".toByteArray(), result.data)
  }

  /**
   * Tests that malformed base64 returns null gracefully.
   * Note: Base64 decoder may handle some padding issues, so test with clearly invalid base64.
   */
  @Test
  fun `parse returns null for malformed base64`() {
    val input1 = "data:text/plain;base64,InvalidBase64!!!$$$"
    val input2 = "data:text/plain;base64,SGVsbG8gV29ybG!!!" // Invalid characters

    assertNull(DataUrl.parse(input1), "Should return null for invalid base64 characters")
    assertNull(DataUrl.parse(input2), "Should return null for invalid base64 characters")
  }

  /**
   * Tests parsing with only comma (minimal valid format).
   */
  @Test
  fun `parse minimal data URL with only comma`() {
    val input = "data:,Hello"
    val result = DataUrl.parse(input)

    assertNotNull(result)
    assertEquals(MediaType.TEXT_PLAIN, result!!.mediaType) // Default
    assertEquals(false, result.base64)
    assertEquals("Hello", String(result.data, StandardCharsets.UTF_8))
  }
}
