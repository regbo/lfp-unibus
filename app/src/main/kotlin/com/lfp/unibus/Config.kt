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

@Configuration
class Config {

  @Bean fun webSocketHandlerAdapter() = WebSocketHandlerAdapter()

  @Bean
  fun handlerMapping(handler: KafkaWebSocketHandler): SimpleUrlHandlerMapping {
    return SimpleUrlHandlerMapping(mapOf("/**" to handler), 1)
  }

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
    kafkaOptions["reactor.kafka.receiver.failOnError"] = true
    return Collections.unmodifiableMap(kafkaOptions)
  }

  @Bean
  fun jacksonCustomizer(): Jackson2ObjectMapperBuilderCustomizer {
    return Jackson2ObjectMapperBuilderCustomizer { builder ->
      val module =
          SimpleModule().apply { addDeserializer(ByteArray::class.java, ByteArrayDeserializer()) }
      builder.modulesToInstall(module)
    }
  }
}
