package com.lfp.unibus.common

import java.util.*
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.BytesDeserializer
import org.apache.kafka.common.serialization.BytesSerializer
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Configuration
import org.springframework.stereotype.Component

/**
 * Kafka configuration factory.
 *
 * Creates ProducerConfig and ConsumerConfig from environment properties and optional overrides.
 * Properties prefixed with "kafka." are automatically loaded from configuration.
 */
@EnableConfigurationProperties
@Configuration
class KafkaConfig(kafkaProperties: KafkaProperties) {

  private val requiredProperties = Collections.unmodifiableMap(Utils.flatten(kafkaProperties))

  /**
   * Creates ProducerConfig with optional property overrides.
   *
   * @param props Optional property overrides (can be prefixed with "producer.")
   * @return Configured ProducerConfig instance
   */
  fun producer(props: Map<String, Any?>? = null): ProducerConfig {
    val configProperties =
            configProperties(
                    props,
                    "producer",
                    mapOf(
                            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to
                                    BytesSerializer::class.java.name,
                            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to
                                    BytesSerializer::class.java.name,
                    ),
            )
    return ProducerConfig(configProperties)
  }

  /**
   * Creates ConsumerConfig with optional property overrides.
   *
   * @param props Optional property overrides (can be prefixed with "consumer.")
   * @return Configured ConsumerConfig instance
   */
  fun consumer(props: Map<String, Any?>? = null): ConsumerConfig {
    val configProperties =
            configProperties(
                    props,
                    "consumer",
                    mapOf(
                            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to
                                    BytesDeserializer::class.java.name,
                            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to
                                    BytesDeserializer::class.java.name,
                    ),
            )
    return ConsumerConfig(configProperties)
  }

  private fun configProperties(
          props: Map<String, Any?>?,
          prefix: String,
          vararg requiredProperties: Map<String, Any?>,
  ): LinkedHashMap<String, Any> {
    val configProperties = LinkedHashMap<String, Any>()
    if (props != null && !props.isEmpty()) {
      for (keyPrefix in setOf<String?>(null, prefix.let { "$it." })) {
        props.forEach { (k, v) ->
          if (v == null) return@forEach
          val configPropertyKey = if (keyPrefix == null) k else k.substringAfter(keyPrefix, "")
          if (!configPropertyKey.isEmpty() && !configProperties.containsKey(configPropertyKey)) {
            configProperties[configPropertyKey] = v
          }
        }
      }
    }
    for (requiredProps in (setOf(this.requiredProperties) + requiredProperties)) {
      requiredProps.forEach { (k, v) ->
        if (v == null) return@forEach
        configProperties.compute(
                k,
                { _, curValue ->
                  check(curValue == null || curValue == v) {
                    "required property $k can't be set to $curValue"
                  }
                  return@compute v
                },
        )
      }
    }
    return configProperties
  }

  companion object {}

  /**
   * Kafka properties loaded from configuration.
   *
   * Properties prefixed with "kafka." are automatically bound to this map.
   */
  @ConfigurationProperties(prefix = "kafka")
  @Component
  class KafkaProperties : LinkedHashMap<String, Any>()
}
