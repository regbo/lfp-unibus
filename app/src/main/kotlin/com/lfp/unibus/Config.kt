package com.lfp.unibus

import com.lfp.unibus.service.ws.KafkaWebSocketHandler
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.stereotype.Component
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping
import org.springframework.web.reactive.socket.server.support.WebSocketHandlerAdapter
import org.springframework.web.server.ServerWebExchange
import org.springframework.web.server.WebExceptionHandler
import reactor.core.publisher.Mono

/**
 * Spring configuration class for the LFP Unibus application.
 *
 * Configures WebSocket handling and global error handling.
 */
@Configuration
class Config {

  /**
   * Creates a WebSocket handler adapter bean.
   *
   * @return WebSocketHandlerAdapter instance
   */
  @Bean fun webSocketHandlerAdapter() = WebSocketHandlerAdapter()

  /**
   * Configures URL handler mapping for WebSocket connections.
   *
   * Maps all paths to the KafkaWebSocketHandler with priority 1.
   *
   * @param handler The KafkaWebSocketHandler instance
   * @return SimpleUrlHandlerMapping configured for WebSocket handling
   */
  @Bean
  fun handlerMapping(handler: KafkaWebSocketHandler): SimpleUrlHandlerMapping {
    return SimpleUrlHandlerMapping(mapOf("/**" to handler), 1)
  }

  /**
   * Global error handler for WebFlux exceptions.
   *
   * Logs errors and completes the response.
   */
  @Component
  class GlobalErrorHandler : WebExceptionHandler {
    private val log = LoggerFactory.getLogger(this::class.java)

    override fun handle(exchange: ServerWebExchange, ex: Throwable): Mono<Void> {
      log.error("Web error", ex)
      return exchange.response.setComplete()
    }
  }
}
