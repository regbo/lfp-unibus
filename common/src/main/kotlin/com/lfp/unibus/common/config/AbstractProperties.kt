package com.lfp.unibus.common.config

import org.apache.kafka.common.config.AbstractConfig

/**
 * Abstract base class for Kafka configuration properties wrappers.
 *
 * Provides a typed wrapper around Kafka AbstractConfig instances for easier access
 * to configuration properties.
 *
 * @param config Kafka configuration instance
 */
abstract class AbstractProperties<T : AbstractConfig>(val config: T) {}
