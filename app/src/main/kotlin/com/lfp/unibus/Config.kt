package com.lfp.unibus

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping
import org.springframework.web.reactive.socket.server.support.WebSocketHandlerAdapter

@Configuration
class WebSocketConfig {

    @Bean
    fun handlerMapping(handler: KafkaWebSocketHandler): SimpleUrlHandlerMapping {
        return SimpleUrlHandlerMapping(mapOf("/**" to handler), 1)
    }

    @Bean
    fun webSocketHandlerAdapter() = WebSocketHandlerAdapter()
}