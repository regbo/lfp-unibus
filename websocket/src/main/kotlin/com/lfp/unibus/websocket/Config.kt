package com.lfp.unibus.websocket

import com.lfp.unibus.websocket.service.KafkaWebSocketHandler
import com.lfp.unibus.websocket.service.mqtt.KafkaMqttHandler
import com.lfp.unibus.websocket.service.mqtt.MqttDetectionHandler
import org.springframework.boot.web.embedded.netty.NettyServerCustomizer
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
   * Required for Spring WebFlux to upgrade HTTP requests and delegate frames to
   * KafkaWebSocketHandler.
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

  @Bean
  fun nettyServerCustomizer(kafkaMqttHandler: KafkaMqttHandler): NettyServerCustomizer {
    return NettyServerCustomizer { httpServer ->
      httpServer.tcpConfiguration { tcp ->
        tcp.doOnChannelInit { observer, channel, remoteAddress ->
          val pipeline = channel.pipeline()
          pipeline.addFirst("detector", MqttDetectionHandler(kafkaMqttHandler))
        }
        tcp.doOnConnection { context ->
          val pipeline = context.channel().pipeline()
          pipeline.forEach { println(it) }
        }
      }
    }
  }
}
