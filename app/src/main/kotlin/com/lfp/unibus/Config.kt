package com.lfp.unibus

import com.lfp.unibus.service.ws.KafkaWebSocketHandler
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
   * WebSocket handler adapter bean.
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
