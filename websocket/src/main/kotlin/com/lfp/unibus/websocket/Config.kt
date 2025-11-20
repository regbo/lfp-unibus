package com.lfp.unibus.websocket

import com.lfp.unibus.websocket.service.KafkaWebSocketHandler
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping
import org.springframework.web.reactive.socket.server.support.WebSocketHandlerAdapter

/**
 * Spring configuration for WebSocket handling.
 *
 * Configures WebSocket handler adapter and URL mapping.
 */
@Configuration
class Config {

  /**
   * Creates the reactive WebSocket handler adapter.
   *
   * Required for Spring WebFlux to upgrade HTTP requests and delegate frames to KafkaWebSocketHandler.
   *
   * @return WebSocketHandlerAdapter instance
   */
  @Bean fun webSocketHandlerAdapter() = WebSocketHandlerAdapter()

  /**
   * URL handler mapping for WebSocket connections.
   *
   * Maps all paths to KafkaWebSocketHandler with priority 1.
   *
   * @param handler KafkaWebSocketHandler instance
   * @return SimpleUrlHandlerMapping configured for WebSocket handling
   */
  @Bean
  fun handlerMapping(handler: KafkaWebSocketHandler): SimpleUrlHandlerMapping {
    return SimpleUrlHandlerMapping(mapOf("/**" to handler), 1)
  }
}
