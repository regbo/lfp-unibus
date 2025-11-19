package com.lfp.unibus.common.config

import org.apache.kafka.common.config.AbstractConfig

abstract class AbstractProperties<T : AbstractConfig>(val config: T) {}
