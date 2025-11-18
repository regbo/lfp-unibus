package com.lfp.unibus

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

/**
 * Main Spring Boot application class for the LFP Unibus service.
 *
 * This application provides a WebSocket-based interface to Apache Kafka, allowing clients to
 * produce and consume Kafka messages through WebSocket connections.
 */
@SpringBootApplication class App

/**
 * Application entry point.
 *
 * Starts the Spring Boot application which initializes the WebSocket server and Kafka integration
 * components.
 */
fun main() {
    runApplication<App>()
}
