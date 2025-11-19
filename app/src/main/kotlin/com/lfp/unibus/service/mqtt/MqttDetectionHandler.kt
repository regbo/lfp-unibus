package com.lfp.unibus.service.mqtt

import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.ByteToMessageDecoder
import io.netty.handler.codec.mqtt.MqttDecoder
import io.netty.handler.codec.mqtt.MqttEncoder
import io.netty.handler.timeout.IdleStateHandler
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit

/**
 * Netty channel handler that detects MQTT protocol magic bytes.
 *
 * MQTT protocol packets start with a fixed header byte where bits 7-4 represent the message type.
 * Common MQTT message types:
 * - CONNECT: 0x10 (16)
 * - CONNACK: 0x20 (32)
 * - PUBLISH: 0x30 (48)
 * - PUBACK: 0x40 (64)
 * - PUBREC: 0x50 (80)
 * - PUBREL: 0x60 (96)
 * - PUBCOMP: 0x70 (112)
 * - SUBSCRIBE: 0x80 (128)
 * - SUBACK: 0x90 (144)
 * - UNSUBSCRIBE: 0xA0 (160)
 * - UNSUBACK: 0xB0 (176)
 * - PINGREQ: 0xC0 (192)
 * - PINGRESP: 0xD0 (208)
 * - DISCONNECT: 0xE0 (224)
 *
 * This handler inspects the first byte(s) of incoming raw TCP data and logs when MQTT protocol is
 * detected before passing the data to the next handler in the pipeline. It is added early in the
 * pipeline to inspect raw bytes before HTTP processing.
 */
class MqttDetectionHandler(val kafkaMqttHandler: KafkaMqttHandler) : ByteToMessageDecoder() {

  private val logger = LoggerFactory.getLogger(MqttDetectionHandler::class.java)
  private val handlers =
      mapOf(
          "mqttDecoder" to MqttDecoder(),
          "mqttEncoder" to MqttEncoder.INSTANCE,
          "idleStateHandler" to IdleStateHandler(45, 0, 0, TimeUnit.SECONDS),
          "kafkaMqttHandler" to kafkaMqttHandler,
      )

  override fun decode(ctx: ChannelHandlerContext, input: ByteBuf, out: MutableList<Any>) {

    if (!input.isReadable) return

    input.markReaderIndex()
    val b = input.readUnsignedByte().toInt()
    input.resetReaderIndex()
    val isHttp = b == 'G'.code || b == 'P'.code || b == 'H'.code || b == 'O'.code || b == 'D'.code
    val isMqtt =
        !isHttp &&
            run {
              val upper = b ushr 4
              // valid MQTT types are 1 through 14
              if (upper !in 1..14) return@run false
              // forbid ASCII letters
              if (b in 65..90 || b in 97..122) return@run false
              return@run true
            }
    val pipeline = ctx.pipeline()
    val handlerNames = pipeline.names()
    if (isMqtt) {
      for (handlerName in handlerNames.reversed()) {
        val handlerCtx = pipeline.context(handlerName)
        if (handlerCtx == null) {
          continue
        } else if (handlerCtx.handler() == this) {
          break
        } else {
          pipeline.remove(handlerName)
        }
      }
      val handlerName = pipeline.context(this).name()
      handlers.entries.reversed().forEach { (k, v) -> pipeline.addAfter(handlerName, k, v) }
    } else {
      // leave the pipeline empty or add something else
    }
    pipeline.remove(this)
  }
}
