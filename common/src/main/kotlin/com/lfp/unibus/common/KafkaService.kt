package com.lfp.unibus.common

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
import java.lang.reflect.Modifier
import java.util.*
import kotlin.reflect.KClass

/**
 * Kafka configuration factory.
 *
 * Creates ProducerConfig and ConsumerConfig from environment properties and optional overrides.
 * Properties prefixed with "kafka." are automatically loaded from configuration.
 */
@EnableConfigurationProperties
@Configuration
class KafkaService(kafkaProperties: KafkaProperties) {

  private val requiredProperties = Collections.unmodifiableMap(Utils.flatten(kafkaProperties))

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
    var receiverOptions = ReceiverOptions.create<Bytes, Bytes>(configProperties)
    receiverOptions =
        if (configProperties.get(ConsumerConfig.GROUP_ID_CONFIG) == null) {
          receiverOptions.assignment(topics.map { TopicPartition(it, 0) }.toList())
        } else {
          receiverOptions.subscription(topics.toList())
        }
    return KafkaReceiver.create(receiverOptions)
  }

  private fun configProperties(
      configType: ConfigType,
      vararg configs: Map<String, Any?>?,
  ): Map<String, Any> {
    val prefix = configType.prefix
    val configProperties = LinkedHashMap<String, Any>()

    fun append(config: Map<String, Any?>?, required: Boolean) {
      val keyMap = keyMap(prefix, config)
      for ((destKey, sourceKey) in keyMap) {
        if (!required && !configType.propertyNames.contains(destKey)) {
          continue
        }
        val value = config!![sourceKey]?.takeIf { it !is String || !it.isEmpty() }
        if (!required) {
          if (value == null) {
            configProperties.remove(destKey)
          } else {
            configProperties[destKey] = value
          }
        } else {
          configProperties.compute(
              destKey,
              { _, curValue ->
                check(curValue == null || curValue == value) {
                  "property $destKey can't be set to $curValue"
                }
                return@compute value
              },
          )
        }
      }
    }

    configs.forEach { append(it, false) }
    for (config in listOf(configType.configProperties, this.requiredProperties)) {
      append(config, true)
    }
    return configProperties
  }

  companion object {

    @JvmStatic
    private fun keyMap(
        prefix: String?,
        map: Map<String, Any?>?,
    ): Map<String, String> {
      if (map == null || map.isEmpty()) return emptyMap()
      val keyPrefix = prefix?.takeIf { it.isNotEmpty() }?.let { "$it." }
      val keyPairs =
          map.keys
              .filter { !it.isEmpty() }
              .map { key ->
                if (keyPrefix != null && key.startsWith(keyPrefix)) {
                  return@map Pair(key.substring(keyPrefix.length), key)
                }
                return@map Pair(key, key)
              }
              .filter { !it.first.isEmpty() }
      if (keyPrefix == null) {
        return keyPairs.toMap()
      }
      return keyPairs
          .sortedWith(
              compareBy<Pair<String, String>> { it.first }.thenByDescending { it.second.length }
          )
          .distinctBy { it.first }
          .toMap()
    }

    @JvmStatic
    fun propertyNames(configType: KClass<out AbstractConfig>): List<String> {
      return configType.java.fields
          .filter { field ->
            Modifier.isPublic(field.modifiers) &&
                Modifier.isStatic(field.modifiers) &&
                Modifier.isFinal(field.modifiers) &&
                field.type == String::class.java &&
                field.name.endsWith("_CONFIG")
          }
          .map { it.get(null) as String }
    }
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
      val prefix: String,
      val propertyNames: List<String>,
      val configProperties: Map<String, Any>,
  ) {
    PRODUCER(
        "producer",
        KafkaService.propertyNames(ProducerConfig::class),
        mapOf(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to BytesSerializer::class.java.name,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to BytesSerializer::class.java.name,
        ),
    ),
    CONSUMER(
        "consumer",
        KafkaService.propertyNames(ConsumerConfig::class),
        mapOf(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to BytesDeserializer::class.java.name,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to BytesDeserializer::class.java.name,
        ),
    ),
  }
}
