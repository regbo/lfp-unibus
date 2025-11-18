package com.lfp.unibus

import com.fasterxml.jackson.databind.module.SimpleModule
import org.springframework.boot.autoconfigure.jackson.Jackson2ObjectMapperBuilderCustomizer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.env.ConfigurableEnvironment
import org.springframework.core.env.EnumerablePropertySource
import org.springframework.core.env.Environment
import org.springframework.core.env.PropertySource
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping
import org.springframework.web.reactive.socket.server.support.WebSocketHandlerAdapter
import java.util.*

/**
 * Spring configuration class for the LFP Unibus application.
 *
 * Configures WebSocket handling, Kafka options from environment properties,
 * and Jackson object mapper customization for ByteArray deserialization.
 */
@Configuration
class Config {

  /**
   * Creates a WebSocket handler adapter bean.
   *
   * @return WebSocketHandlerAdapter instance
   */
  @Bean
  fun webSocketHandlerAdapter() = WebSocketHandlerAdapter()

  /**
   * Configures URL handler mapping for WebSocket connections.
   *
   * Maps all paths to the KafkaWebSocketHandler with priority 1.
   *
   * @param handler The KafkaWebSocketHandler instance
   * @return SimpleUrlHandlerMapping configured for WebSocket handling
   */
  @Bean
  fun handlerMapping(handler: KafkaWebSocketHandler): SimpleUrlHandlerMapping {
    return SimpleUrlHandlerMapping(mapOf("/**" to handler), 1)
  }

  /**
   * Extracts Kafka configuration options from environment properties.
   *
   * Scans all environment property sources for properties prefixed with kafka.
   * and returns them as a map.
   *
   * @param env The Spring Environment containing configuration properties
   * @return Unmodifiable map of Kafka configuration options (keys without kafka. prefix)
   */
  @Bean
  fun kafkaOptions(env: Environment): Map<String, Any?> {
    val propertySource =
        when (env) {
          is ConfigurableEnvironment -> {
            env.propertySources
          }
          is PropertySource<*> -> {
            listOf(env)
          }
          else -> {
            emptyList()
          }
        }

    val kafkaOptions =
        propertySource
            .filterIsInstance<EnumerablePropertySource<*>>()
            .flatMap { it.propertyNames.asList() }
            .distinct()
            .mapNotNull { key ->
              val kafkaKey = key.substringAfter("kafka.", "")
              if (kafkaKey.isNotEmpty()) {
                val kafkaValue = env.getProperty(key)
                if (kafkaValue != null && kafkaValue.isNotEmpty()) {
                  return@mapNotNull Pair<String, Any?>(kafkaKey, kafkaValue)
                }
              }
              null
            }
            .toMap(LinkedHashMap())
    return Collections.unmodifiableMap(kafkaOptions)
  }

  /**
   * Customizes Jackson ObjectMapper to use ByteArrayDeserializer for ByteArray types.
   *
   * This allows proper deserialization of Base64-encoded byte arrays in JSON payloads.
   *
   * @return Jackson2ObjectMapperBuilderCustomizer that registers ByteArrayDeserializer
   */
  @Bean
  fun jacksonCustomizer(): Jackson2ObjectMapperBuilderCustomizer {
    return Jackson2ObjectMapperBuilderCustomizer { builder ->
      val module =
          SimpleModule().apply { addDeserializer(ByteArray::class.java, ByteArrayDeserializer()) }
      builder.modulesToInstall(module)
    }
  }
}
