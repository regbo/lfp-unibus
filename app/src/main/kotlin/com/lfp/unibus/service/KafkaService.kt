package com.lfp.unibus.service

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Bytes
import org.springframework.stereotype.Service
import reactor.kafka.receiver.KafkaReceiver
import reactor.kafka.receiver.ReceiverOptions
import reactor.kafka.sender.KafkaSender
import reactor.kafka.sender.SenderOptions

/**
 * Service for creating Kafka producers and consumers.
 *
 * Provides factory methods for creating Reactor Kafka senders and receivers.
 */
@Service
class KafkaService {

  /**
   * Creates a Kafka sender for producing messages.
   *
   * @param config Producer configuration
   * @return KafkaSender instance
   */
  fun producer(config: ProducerConfig): KafkaSender<Bytes, Bytes> {
    return KafkaSender.create(SenderOptions.create(config.originals()))
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
      config: ConsumerConfig,
      topic: String,
      vararg topics: String,
  ): KafkaReceiver<Bytes, Bytes> {
    var receiverOptions = ReceiverOptions.create<Bytes, Bytes>(config.originals())
    val topicSet = setOf(topic) + topics
    receiverOptions =
        if (config.getString(ConsumerConfig.GROUP_ID_CONFIG)?.isNotEmpty() ?: true) {
          receiverOptions.assignment(topicSet.map { TopicPartition(it, 0) }.toList())
        } else {
          receiverOptions.subscription(topicSet.toList())
        }
    return KafkaReceiver.create(receiverOptions)
  }
}
