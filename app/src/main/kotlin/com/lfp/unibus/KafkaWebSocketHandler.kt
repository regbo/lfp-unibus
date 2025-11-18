package com.lfp.unibus

import org.springframework.stereotype.Component
import org.springframework.web.reactive.socket.WebSocketHandler
import org.springframework.web.reactive.socket.WebSocketMessage
import org.springframework.web.reactive.socket.WebSocketSession
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Duration
import java.util.concurrent.atomic.AtomicLong

@Component
class ReactiveWebSocketHandler : WebSocketHandler {

    private val counter = AtomicLong(0)
    private val intervalFlux: Flux<String> =
        Flux.interval(Duration.ofSeconds(1))
            .map { "server message ${counter.incrementAndGet()}" }

    override fun handle(session: WebSocketSession): Mono<Void> {
        val sendFlow = session.send(intervalFlux.map(session::textMessage))

        val receiveFlow = session.receive()
            .map(WebSocketMessage::getPayloadAsText)
            .doOnNext { msg -> println("Received: $msg") }
            .then()

        return Mono.`when`(sendFlow, receiveFlow)
    }
}