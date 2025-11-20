package com.lfp.unibus.common

import com.lfp.unibus.common.Extensions.flatten
import java.util.*
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.TopicDescription
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException
import org.apache.kafka.common.utils.Bytes
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Configuration
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono
import reactor.kafka.receiver.KafkaReceiver
import reactor.kafka.receiver.ReceiverOptions
import reactor.kafka.sender.KafkaSender
import reactor.kafka.sender.SenderOptions

private const val CLIENT_ID_PREFIX = "unibus"

/**
 * Kafka configuration factory.
 *
 * Creates ProducerConfig and ConsumerConfig from environment properties, exposes Reactor friendly
 * producers and consumers, and provides access to an Admin client for metadata lookups such as
 * topic descriptions. Properties prefixed with "kafka." are automatically loaded from configuration.
 */
@EnableConfigurationProperties
@Configuration
class KafkaService(kafkaProperties: KafkaProperties) {

  private val requiredProperties = Collections.unmodifiableMap(kafkaProperties.flatten())
  private val adminClientLazy = lazy { Admin.create(requiredProperties)!! }

  /**
   * Lazily creates or returns the shared Kafka Admin client.
   *
   * The Admin instance reuses the base application configuration and backs metadata lookups such as
   * topic descriptions.
   *
   * @return Shared Admin client
   */
  fun admin(): Admin {
    return adminClientLazy.value
  }

  /**
   * Describes a Kafka topic using the Admin client.
   *
   * Returns a Mono that emits the TopicDescription when present or completes empty when the topic
   * does not exist or the broker responds with UnknownTopicOrPartitionException.
   *
   * @param topic Target topic name
   * @return Mono emitting TopicDescription when found
   */
  fun describeTopic(topic: String): Mono<TopicDescription> {
    val topicDescription: Mono<TopicDescription> =
        Mono.fromCompletionStage {
              admin().describeTopics(listOf(topic)).allTopicNames().toCompletionStage()
            }
            .mapNotNull { it[topic] }
    return topicDescription.onErrorResume(UnknownTopicOrPartitionException::class.java) {
      Mono.empty()
    }
  }

  /**
   * Creates ProducerConfig with optional property overrides.
   *
   * Later maps override earlier ones, and keys may be prefixed with `producer.` to scope them
   * specifically to producer clients.
   *
   * @param configs Optional property maps; null entries are ignored
   * @return Configured ProducerConfig instance
   */
  fun producerConfigProperties(vararg configs: Map<String, Any?>?): Map<String, Any> {
    return clientProperties(
        ClientProperties.PRODUCER,
        *configs,
    )
  }

  /**
   * Creates a Kafka sender for producing messages.
   *
   * @param configs Optional property overrides (can be prefixed with `producer.`)
   * @return KafkaSender instance
   */
  fun producer(vararg configs: Map<String, Any?>?): KafkaSender<Bytes, Bytes> {
    return KafkaSender.create(SenderOptions.create(producerConfigProperties(*configs)))
  }

  /**
   * Creates ConsumerConfig with optional property overrides.
   *
   * Later maps override earlier ones, and keys may be prefixed with `consumer.` to scope them
   * to consumer clients.
   *
   * @param configs Optional property maps; null entries are ignored
   * @return Configured ConsumerConfig instance
   */
  fun consumerConfigProperties(vararg configs: Map<String, Any?>?): Map<String, Any> {
    return clientProperties(
        ClientProperties.CONSUMER,
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
      vararg configs: Map<String, Any?>?,
  ): KafkaReceiver<Bytes, Bytes> {
    return consumer(listOf(topic), *configs)
  }

  /**
   * Creates a Kafka receiver for consuming messages.
   *
   * Uses subscription if group ID is set, otherwise falls back to partition assignment.
   *
   * @param topics Collection of topics to subscribe to
   * @param configs Optional property overrides (can be prefixed with `consumer.`)
   * @return KafkaReceiver instance
   */
  fun consumer(
      topics: Collection<String>,
      vararg configs: Map<String, Any?>?,
  ): KafkaReceiver<Bytes, Bytes> {
    val configProperties = consumerConfigProperties(*configs)
    val receiverOptions =
        ReceiverOptions.create<Bytes, Bytes>(configProperties).subscription(topics.toList())
    return KafkaReceiver.create(receiverOptions)
  }

  /**
   * Builds an immutable map of client properties for the requested client type.
   *
   * Later property maps override earlier ones. Prefixed keys (for example `consumer.acks`) are
   * resolved to their scoped counterparts before merging with the base application configuration.
   *
   * @param clientProperties Target client type (producer or consumer)
   * @param configs Optional property maps to merge
   * @return Immutable configuration map ready for Kafka clients
   */
  private fun clientProperties(
      clientProperties: ClientProperties,
      vararg configs: Map<String, Any?>?,
  ): Map<String, Any> {
    val props = clientProperties.get(*configs)
    props.putAll(this.requiredProperties)
    return Collections.unmodifiableMap(props)
  }

  companion object {
    /**
     * Generates a random identifier for Kafka client metadata.
     *
     * Removes hyphen characters to comply with common Kafka naming conventions.
     *
     * @return Random identifier string
     */
    @JvmStatic
    fun uuid(): String {
      return UUID.randomUUID().toString().replace("-", "")
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
}
