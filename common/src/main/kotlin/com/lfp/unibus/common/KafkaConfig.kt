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

@EnableConfigurationProperties
@Configuration
class KafkaConfig(kafkaProperties: KafkaProperties) {

  private val requiredProperties = Collections.unmodifiableMap(Utils.flatten(kafkaProperties))

  fun producer(props: Map<String, Any?>? = null): ProducerConfig {
    val configProperties =
        configProperties(
            props,
            "producer",
            mapOf(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to BytesSerializer::class.java.name,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to BytesSerializer::class.java.name,
            ),
        )
    return ProducerConfig(configProperties)
  }

  fun consumer(props: Map<String, Any?>? = null): ConsumerConfig {
    val configProperties =
        configProperties(
            props,
            "consumer",
            mapOf(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to BytesDeserializer::class.java.name,
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

  @ConfigurationProperties(prefix = "kafka")
  @Component
  class KafkaProperties : LinkedHashMap<String, Any>()
}
