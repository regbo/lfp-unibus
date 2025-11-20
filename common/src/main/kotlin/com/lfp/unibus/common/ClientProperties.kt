package com.lfp.unibus.common

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.BytesDeserializer
import org.apache.kafka.common.serialization.BytesSerializer

/**
 * Shared Kafka client configuration helpers.
 *
 * Provides consumer and producer property handling with required defaults and controlled whitelist
 * of user supplied overrides. WebSocket query parameters can prefix keys with `consumer.` or
 * `producer.` to target a specific client; unprefixed keys apply to both when valid.
 */
enum class ClientProperties(
    private val requiredProperties: Map<String, Any>,
    private val whitelist: List<String>,
) {
  CONSUMER(
      mapOf(
          ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to BytesDeserializer::class.java.name,
          ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to BytesDeserializer::class.java.name,
      ),
      listOf(
          // Sets a logical name for the consumer
          // @see ConsumerConfig.CLIENT_ID_DOC
          ConsumerConfig.CLIENT_ID_CONFIG,

          // Sets the consumer group
          // @see ConsumerConfig.GROUP_ID_DOC
          ConsumerConfig.GROUP_ID_CONFIG,

          // Controls behavior when no committed offset exists
          // @see ConsumerConfig.AUTO_OFFSET_RESET_DOC
          ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,

          // Enables or disables automatic offset commits
          // @see ConsumerConfig.ENABLE_AUTO_COMMIT_DOC
          ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,

          // Chooses the algorithm for partition assignment
          // @see ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_DOC
          ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,

          // Excludes internal topics from fetches
          // @see ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_DOC
          ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG,

          // Identifies the rack for locality aware fetch
          // @see ConsumerConfig.CLIENT_RACK_DOC
          ConsumerConfig.CLIENT_RACK_CONFIG,
      ),
  ) {
    /**
     * Ensures a stable group identifier when callers do not provide one.
     *
     * Adds both the generic client identifier and a consumer group identifier.
     */
    override fun modifyProperties(config: MutableMap<String, Any>) {
      super.modifyProperties(config)
      config.computeIfAbsent(
          ConsumerConfig.GROUP_ID_CONFIG,
          { "${ID_PREFIX}.${keyPrefix}group.${KafkaService.uuid()}" },
      )
    }
  },
  PRODUCER(
      mapOf(
          ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to BytesSerializer::class.java.name,
          ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to BytesSerializer::class.java.name,
      ),
      listOf(
          // Sets a logical name for the producer
          // @see ProducerConfig.CLIENT_ID_DOC
          ProducerConfig.CLIENT_ID_CONFIG,

          // Controls confirmation behavior from the broker
          // @see ProducerConfig.ACKS_DOC
          ProducerConfig.ACKS_CONFIG,

          // Selects record compression
          // @see ProducerConfig.COMPRESSION_TYPE_DOC
          ProducerConfig.COMPRESSION_TYPE_CONFIG,

          // Enables strong delivery guarantees
          // @see ProducerConfig.ENABLE_IDEMPOTENCE_DOC
          ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,

          // Unique transaction id
          // @see ProducerConfig.TRANSACTIONAL_ID_DOC
          ProducerConfig.TRANSACTIONAL_ID_CONFIG,
      ),
  );

  internal val keyPrefix = "${name.lowercase()}."

  /**
   * Builds a Kafka configuration map constrained to the client whitelist.
   *
   * Later maps override earlier ones; prefixed keys (e.g. `producer.acks`) take precedence over
   * generic keys (`acks`). Unknown properties throw unless they are valid for another client type,
   * in which case they are ignored.
   *
   * @param properties Optional map chain (later arguments win)
   * @return Mutable map of validated client properties
   */
  fun get(vararg properties: Map<String, *>?): MutableMap<String, Any> {
    val clientProperties = LinkedHashMap<String, Any>()
    for (props in properties.reversed()) {
      val keyPairs =
          keyMapping(props)
              .filter { it.first == null || it.first == this }
              .sortedBy { if (it.first == null) 1 else 0 }
              .map { it.second }
      for (keyPair in keyPairs) {
        val destKey = keyPair.first
        if (clientProperties.containsKey(destKey)) continue
        if (!this.whitelist.contains(destKey)) {
          val valid = entries.flatMap { it.whitelist }.any { destKey == it }
          check(valid) { "Invalid $name property: '$destKey'" }
          continue
        }
        val sourceKey = keyPair.second
        val value = props!![sourceKey]
        if (value == null || (value is String && value.isEmpty())) continue
        clientProperties[destKey] = value
      }
    }
    modifyProperties(clientProperties)
    clientProperties.putAll(requiredProperties)
    return clientProperties
  }

  /**
   * Applies default client level properties when not already provided.
   *
   * @param config Mutable configuration map receiving defaults
   */
  internal open fun modifyProperties(config: MutableMap<String, Any>) {
    config.computeIfAbsent(
        CommonClientConfigs.CLIENT_ID_CONFIG,
        { "$ID_PREFIX.${keyPrefix}client.${KafkaService.uuid()}" },
    )
  }

  companion object {
    private const val ID_PREFIX = "unibus"

    /**
     * Builds a sequence of mapping hints for the provided properties map.
     *
     * Each entry indicates the target client type (or null for unscoped keys) and the resolved
     * destination key, allowing prefixed properties to override the generic form.
     */
    @JvmStatic
    private fun keyMapping(
        properties: Map<String, *>?
    ): Sequence<Pair<ClientProperties?, Pair<String, String>>> {
      if (properties == null || properties.isEmpty()) return emptySequence()
      return sequence {
        for (sourceKey in properties.keys) {
          if (sourceKey.isEmpty()) continue
          var keyMapping: Pair<ClientProperties?, Pair<String, String>>? = null
          for (clientType in entries) {
            if (sourceKey.startsWith(clientType.keyPrefix)) {
              // trim leading prefix so consumer.foo maps to foo
              val destKey = sourceKey.substring(clientType.keyPrefix.length)
              keyMapping = Pair(clientType, Pair(destKey, sourceKey))
              break
            }
          }
          if (keyMapping == null) {
            keyMapping = Pair(null, Pair(sourceKey, sourceKey))
          }
          yield(keyMapping)
        }
      }
    }
  }
}
