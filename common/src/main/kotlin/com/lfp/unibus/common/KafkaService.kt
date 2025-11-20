package com.lfp.unibus.common

import com.lfp.unibus.common.Extensions.flatten
import com.lfp.unibus.common.Extensions.lookup
import java.lang.reflect.Modifier
import java.util.*
import kotlin.reflect.KClass
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.serialization.BytesDeserializer
import org.apache.kafka.common.serialization.BytesSerializer
import org.apache.kafka.common.utils.Bytes
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Configuration
import org.springframework.stereotype.Component
import reactor.kafka.receiver.KafkaReceiver
import reactor.kafka.receiver.ReceiverOptions
import reactor.kafka.sender.KafkaSender
import reactor.kafka.sender.SenderOptions

/**
 * Kafka configuration factory.
 *
 * Creates ProducerConfig and ConsumerConfig from environment properties and optional overrides.
 * Properties prefixed with "kafka." are automatically loaded from configuration.
 */
@EnableConfigurationProperties
@Configuration
class KafkaService(kafkaProperties: KafkaProperties) {

  private val requiredProperties = Collections.unmodifiableMap(kafkaProperties.flatten())

  /**
   * Creates ProducerConfig with optional property overrides.
   *
   * @param props Optional property overrides (can be prefixed with "producer.")
   * @return Configured ProducerConfig instance
   */
  fun producerConfigProperties(vararg configs: Map<String, Any?>?): Map<String, Any> {
    return configProperties(
        ConfigType.PRODUCER,
        *configs,
    )
  }

  /**
   * Creates a Kafka sender for producing messages.
   *
   * @param config Producer configuration
   * @return KafkaSender instance
   */
  fun producer(vararg configs: Map<String, Any?>?): KafkaSender<Bytes, Bytes> {
    return KafkaSender.create(SenderOptions.create(producerConfigProperties(*configs)))
  }

  /**
   * Creates ConsumerConfig with optional property overrides.
   *
   * @param props Optional property overrides (can be prefixed with "consumer.")
   * @return Configured ConsumerConfig instance
   */
  fun consumerConfigProperties(vararg configs: Map<String, Any?>?): Map<String, Any> {
    return configProperties(
        ConfigType.CONSUMER,
        *configs,
    )
  }

  /**
   * Creates a Kafka receiver for consuming messages from a single topic.
   *
   * Uses subscription if group ID is set, otherwise uses partition assignment.
   *
   * @param topic Topic to consume from
   * @param configs Optional property overrides (can be prefixed with "consumer.")
   * @return KafkaReceiver instance
   */
  fun consumer(
      topic: String,
      vararg configs: Map<String, Any?>,
  ): KafkaReceiver<Bytes, Bytes> {
    return consumer(listOf(topic), *configs)
  }

  /**
   * Creates a Kafka receiver for consuming messages.
   *
   * Uses subscription if group ID is set, otherwise uses partition assignment.
   *
   * @param config Consumer configuration
   * @param topic Primary topic to consume from
   * @param topics Additional topics to consume from
   * @return KafkaReceiver instance
   */
  fun consumer(
      topics: Collection<String>,
      vararg configs: Map<String, Any?>,
  ): KafkaReceiver<Bytes, Bytes> {
    val configProperties = consumerConfigProperties(*configs)
    val receiverOptions = ReceiverOptions.create<Bytes, Bytes>(configProperties)
    return KafkaReceiver.create(
        if (configProperties[ConsumerConfig.GROUP_ID_CONFIG] == null) {
          receiverOptions.assignment(topics.map { TopicPartition(it, 0) }.toList())
        } else {
          receiverOptions.subscription(topics.toList())
        }
    )
  }

  private fun configProperties(
      configType: ConfigType,
      vararg configs: Map<String, Any?>?,
  ): Map<String, Any> {
    val configProperties = LinkedHashMap<String, Any>()

    fun appendConfigValue(key: String, value: Any, require: Boolean = false): Boolean {
      if (value is String && value.isEmpty()) return false
      if (require) {
        val currentValue = configProperties[key]
        check(currentValue == null || currentValue == value) {
          "required property '$key' can't be set to '$currentValue'"
        }
      }
      configProperties[key] = value
      return true
    }

    fun appendConfig(config: Map<String, Any?>?, require: Boolean = false) {
      if (config == null || config.isEmpty()) return
      for (prefix in listOf(configType.prefix, null)) {
        for (destKey in configType.propertyNames) {
          if (!require && configProperties.containsKey(destKey)) {
            continue
          }
          val sourceKey = prefix?.let { "$it.$destKey" } ?: destKey
          for (value in config.lookup(sourceKey)) {
            if (appendConfigValue(destKey, value, require)) {
              break
            }
          }
        }
      }
    }

    for (config in configs.reversed()) {
      appendConfig(config)
    }
    appendConfig(configType.configProperties, true)
    this.requiredProperties.forEach { (k, v) -> appendConfigValue(k, v, true) }
    return configProperties
  }

  /**
   * Kafka properties loaded from configuration.
   *
   * Properties prefixed with "kafka." are automatically bound to this map.
   */
  @ConfigurationProperties(prefix = "kafka")
  @Component
  class KafkaProperties : LinkedHashMap<String, Any>()

  private enum class ConfigType(
      configType: KClass<out AbstractConfig>,
      val prefix: String,
      val configProperties: Map<String, Any>,
  ) {
    PRODUCER(
        ProducerConfig::class,
        "producer",
        mapOf(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to BytesSerializer::class.java.name,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to BytesSerializer::class.java.name,
        ),
    ),
    CONSUMER(
        ConsumerConfig::class,
        "consumer",
        mapOf(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to BytesDeserializer::class.java.name,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to BytesDeserializer::class.java.name,
        ),
    );

    val propertyNames =
        configType.java.fields
            .filter { field ->
              Modifier.isPublic(field.modifiers) &&
                  Modifier.isStatic(field.modifiers) &&
                  Modifier.isFinal(field.modifiers)
            }
            .filter { field -> String::class.java.isAssignableFrom(field.type) }
            .filter { field -> field.name.endsWith("_CONFIG") }
            .map { field -> field.get(null) as String }
            .toList()
  }
}
