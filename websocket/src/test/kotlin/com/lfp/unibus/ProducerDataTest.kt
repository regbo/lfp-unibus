package com.lfp.unibus

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.util.*

/**
 * Test class for ProducerData.
 *
 * Tests validation logic, header creation, and JSON serialization/deserialization.
 */
class ProducerDataTest {

  private val objectMapper = ObjectMapper()
  private val nodeFactory = JsonNodeFactory.instance

  @Test
  fun `create ProducerData with JSON key and value`() {
    val key = nodeFactory.textNode("test-key")
    val value = nodeFactory.objectNode().put("field", "value")
    val data = ProducerData(partition = 0, timestamp = null, key = key, keyBinary = null, value = value, valueBinary = null, headers = null)

    assertEquals(0, data.partition)
    assertEquals(key, data.key)
    assertEquals(value, data.value)
    assertNull(data.keyBinary)
    assertNull(data.valueBinary)
  }

  @Test
  fun `create ProducerData with binary key and value`() {
    val keyBinary = "key".toByteArray()
    val valueBinary = "value".toByteArray()
    val data = ProducerData(partition = null, timestamp = null, key = null, keyBinary = keyBinary, value = null, valueBinary = valueBinary, headers = null)

    assertNull(data.key)
    assertNull(data.value)
    assertArrayEquals(keyBinary, data.keyBinary)
    assertArrayEquals(valueBinary, data.valueBinary)
  }

  @Test
  fun `create ProducerData throws exception when both key and keyBinary are set`() {
    val key = nodeFactory.textNode("test-key")
    val keyBinary = "key".toByteArray()
    val value = nodeFactory.textNode("test-value")

    assertThrows(IllegalArgumentException::class.java) {
      ProducerData(partition = null, timestamp = null, key = key, keyBinary = keyBinary, value = value, valueBinary = null, headers = null)
    }
  }

  @Test
  fun `create ProducerData throws exception when both value and valueBinary are set`() {
    val key = nodeFactory.textNode("test-key")
    val value = nodeFactory.textNode("test-value")
    val valueBinary = "value".toByteArray()

    assertThrows(IllegalArgumentException::class.java) {
      ProducerData(partition = null, timestamp = null, key = key, keyBinary = null, value = value, valueBinary = valueBinary, headers = null)
    }
  }

  @Test
  fun `create Header with JSON value`() {
    val key = "header-key"
    val value = nodeFactory.textNode("header-value")
    val header = ProducerData.Header(key, value, null)

    assertEquals(key, header.key)
    assertEquals(value, header.value)
    assertNull(header.valueBinary)
  }

  @Test
  fun `create Header with binary value`() {
    val key = "header-key"
    val valueBinary = "header-value".toByteArray()
    val header = ProducerData.Header(key, null, valueBinary)

    assertEquals(key, header.key)
    assertNull(header.value)
    assertArrayEquals(valueBinary, header.valueBinary)
  }

  @Test
  fun `create Header throws exception when both value and valueBinary are set`() {
    val key = "header-key"
    val value = nodeFactory.textNode("header-value")
    val valueBinary = "header-value".toByteArray()

    assertThrows(IllegalArgumentException::class.java) {
      ProducerData.Header(key, value, valueBinary)
    }
  }

  @Test
  fun `Header create from array with key and value`() {
    val arrayNode = nodeFactory.arrayNode().add("key").add("value")
    val header = ProducerData.Header.create(arrayNode)

    assertEquals("key", header.key)
    assertNotNull(header.value)
    assertEquals("value", header.value?.textValue())
    assertNull(header.valueBinary)
  }

  @Test
  fun `Header create from array with key only`() {
    val arrayNode = nodeFactory.arrayNode().add("key")
    val header = ProducerData.Header.create(arrayNode)

    assertEquals("key", header.key)
    assertNull(header.value)
    assertNull(header.valueBinary)
  }

  @Test
  fun `Header create from object with single key-value pair`() {
    val objectNode = nodeFactory.objectNode().put("myKey", "myValue")
    val header = ProducerData.Header.create(objectNode)

    assertEquals("myKey", header.key)
    assertNotNull(header.value)
    assertEquals("myValue", header.value?.textValue())
    assertNull(header.valueBinary)
  }

  @Test
  fun `Header create from object with explicit fields`() {
    val objectNode = nodeFactory.objectNode().put("key", "test-key").put("value", "test-value")
    val header = ProducerData.Header.create(objectNode)

    assertEquals("test-key", header.key)
    assertNotNull(header.value)
    assertEquals("test-value", header.value?.textValue())
    assertNull(header.valueBinary)
  }

  @Test
  fun `Header create from object with valueBinary field`() {
    val base64Value = Base64.getEncoder().encodeToString("binary-data".toByteArray())
    val objectNode = nodeFactory.objectNode().put("key", "test-key").put("valueBinary", "data:application/octet-stream;base64,$base64Value")
    val header = ProducerData.Header.create(objectNode)

    assertEquals("test-key", header.key)
    assertNull(header.value)
    assertNotNull(header.valueBinary)
    assertArrayEquals("binary-data".toByteArray(), header.valueBinary)
  }

  @Test
  fun `Header create throws exception for invalid array`() {
    val arrayNode = nodeFactory.arrayNode().add("key").add("value").add("extra")
    assertThrows(IllegalArgumentException::class.java) {
      ProducerData.Header.create(arrayNode)
    }
  }

  @Test
  fun `Header create throws exception for invalid object`() {
    val objectNode = nodeFactory.objectNode().put("key1", "value1").put("key2", "value2")
    assertThrows(Exception::class.java) {
      ProducerData.Header.create(objectNode)
    }
  }

  @Test
  fun `oneOfCheck allows null for both properties`() {
    val testData = TestDataClass(null, null)
    assertDoesNotThrow {
      ProducerData.oneOfCheck(testData, TestDataClass::node, TestDataClass::binary)
    }
  }

  @Test
  fun `oneOfCheck allows only node property`() {
    val testData = TestDataClass(nodeFactory.textNode("test"), null)
    assertDoesNotThrow {
      ProducerData.oneOfCheck(testData, TestDataClass::node, TestDataClass::binary)
    }
  }

  @Test
  fun `oneOfCheck allows only binary property`() {
    val testData = TestDataClass(null, "test".toByteArray())
    assertDoesNotThrow {
      ProducerData.oneOfCheck(testData, TestDataClass::node, TestDataClass::binary)
    }
  }

  @Test
  fun `oneOfCheck throws exception when both properties are set`() {
    val testData = TestDataClass(nodeFactory.textNode("test"), "test".toByteArray())
    assertThrows(IllegalArgumentException::class.java) {
      ProducerData.oneOfCheck(testData, TestDataClass::node, TestDataClass::binary)
    }
  }

  private data class TestDataClass(val node: JsonNode?, val binary: ByteArray?)
}

