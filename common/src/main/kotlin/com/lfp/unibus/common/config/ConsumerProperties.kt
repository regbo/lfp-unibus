package com.lfp.unibus.common.config

import org.apache.kafka.clients.consumer.ConsumerConfig

/**
 * Wrapper for Kafka ConsumerConfig properties.
 *
 * Provides typed access to consumer configuration properties.
 *
 * @param config ConsumerConfig instance
 */
class ConsumerProperties(config: ConsumerConfig) : AbstractProperties<ConsumerConfig>(config) {}
