package com.lfp.unibus.websocket

import com.lfp.unibus.common.KafkaService
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

/**
 * Main Spring Boot application class for the LFP Unibus service.
 *
 * Provides a WebSocket-based interface to Apache Kafka for bidirectional message streaming.
 */
@SpringBootApplication(scanBasePackages = ["com.lfp.unibus"]) class App

/**
 * Application entry point.
 *
 * Starts the Spring Boot application and initializes WebSocket server and Kafka integration.
 */
fun main() {
  val ctx = runApplication<App>()
  val kafkaService = ctx.getBean(KafkaService::class.java)
}
