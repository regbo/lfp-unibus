package com.lfp.unibus.common.data

/**
 * Type of payload sent through WebSocket.
 *
 * Used to distinguish between consumed Kafka records and producer result messages.
 */
enum class PayloadType {
  /** Kafka consumer record payload */
  RECORD,
  /** Kafka producer result payload */
  RESULT,
}