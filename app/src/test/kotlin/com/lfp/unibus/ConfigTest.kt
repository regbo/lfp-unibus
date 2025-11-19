package com.lfp.unibus

import com.fasterxml.jackson.databind.ObjectMapper
import com.lfp.unibus.service.ws.KafkaWebSocketHandler
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.springframework.core.convert.support.DefaultConversionService
import org.springframework.core.env.MapPropertySource
import org.springframework.core.env.StandardEnvironment
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping
import org.springframework.web.reactive.socket.server.support.WebSocketHandlerAdapter

/**
 * Test class for Config.
 *
 * Tests Spring bean creation and Kafka options extraction from environment properties.
 */
class ConfigTest {

  private val config = Config()

  @Test
  fun `webSocketHandlerAdapter creates WebSocketHandlerAdapter`() {
    val adapter = config.webSocketHandlerAdapter()
    assertNotNull(adapter)
    assertTrue(adapter is WebSocketHandlerAdapter)
  }

  @Test
  fun `handlerMapping creates SimpleUrlHandlerMapping`() {
    val handler = KafkaWebSocketHandler(
        DefaultConversionService(),
        ObjectMapper(),
        emptyMap()
    )
    val mapping = config.handlerMapping(handler)

    assertNotNull(mapping)
    assertTrue(mapping is SimpleUrlHandlerMapping)
    assertEquals(1, mapping.order)
    assertTrue(mapping.urlMap.containsKey("/**"))
    assertEquals(handler, mapping.urlMap["/**"])
  }

  @Test
  fun `kafkaOptions extracts properties with kafka prefix`() {
    val env = StandardEnvironment()
    val properties = mapOf(
        "kafka.bootstrap.servers" to "localhost:9092",
        "kafka.security.protocol" to "SSL",
        "other.property" to "value"
    )
    val propertySource = MapPropertySource("test", properties)
    env.propertySources.addFirst(propertySource)

    val options = config.kafkaOptions(env)

    assertEquals(2, options.size)
    assertEquals("localhost:9092", options["bootstrap.servers"])
    assertEquals("SSL", options["security.protocol"])
    assertFalse(options.containsKey("other.property"))
  }

  @Test
  fun `kafkaOptions returns unmodifiable map`() {
    val env = StandardEnvironment()
    val properties = mapOf("kafka.test.property" to "value")
    val propertySource = MapPropertySource("test", properties)
    env.propertySources.addFirst(propertySource)

    val options = config.kafkaOptions(env)

    assertThrows(UnsupportedOperationException::class.java) {
      (options as MutableMap<String, Any?>)["new.property"] = "value"
    }
  }

  @Test
  fun `kafkaOptions filters out empty values`() {
    val env = StandardEnvironment()
    val properties = mapOf(
        "kafka.bootstrap.servers" to "localhost:9092",
        "kafka.empty.property" to "",
        "kafka.null.property" to null as String?
    )
    val propertySource = MapPropertySource("test", properties.filterValues { it != null })
    env.propertySources.addFirst(propertySource)

    val options = config.kafkaOptions(env)

    assertEquals(1, options.size)
    assertEquals("localhost:9092", options["bootstrap.servers"])
    assertFalse(options.containsKey("empty.property"))
  }

  @Test
  fun `kafkaOptions handles properties without kafka prefix`() {
    val env = StandardEnvironment()
    val properties = mapOf(
        "kafka.bootstrap.servers" to "localhost:9092",
        "not.kafka.property" to "value"
    )
    val propertySource = MapPropertySource("test", properties)
    env.propertySources.addFirst(propertySource)

    val options = config.kafkaOptions(env)

    assertTrue(options.size >= 1)
    assertEquals("localhost:9092", options["bootstrap.servers"])
    assertFalse(options.containsKey("not.kafka.property"))
  }

  @Test
  fun `jacksonCustomizer creates customizer`() {
    val customizer = config.jacksonCustomizer()
    assertNotNull(customizer)
    assertTrue(customizer is org.springframework.boot.autoconfigure.jackson.Jackson2ObjectMapperBuilderCustomizer)
  }
}

