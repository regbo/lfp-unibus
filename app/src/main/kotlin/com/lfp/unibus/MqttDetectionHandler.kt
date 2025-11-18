package com.lfp.unibus

import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundHandlerAdapter
import org.slf4j.LoggerFactory

/**
 * Netty channel handler that detects MQTT protocol magic bytes.
 *
 * MQTT protocol packets start with a fixed header byte where bits 7-4 represent
 * the message type. Common MQTT message types:
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
 * This handler inspects the first byte(s) of incoming raw TCP data and logs when
 * MQTT protocol is detected before passing the data to the next handler in the pipeline.
 * It is added early in the pipeline to inspect raw bytes before HTTP processing.
 */
class MqttDetectionHandler : ChannelInboundHandlerAdapter() {

  private val logger = LoggerFactory.getLogger(MqttDetectionHandler::class.java)

  /**
   * MQTT message type values (bits 7-4 of the first byte).
   * These are the upper nibble values that identify MQTT message types.
   */
  private val mqttMessageTypes = setOf(
      0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80, 0x90, 0xA0, 0xB0, 0xC0, 0xD0, 0xE0
  )

  override fun channelRead(ctx: ChannelHandlerContext, msg: Any) {
    if (msg is ByteBuf) {
      if (msg.readableBytes() > 0) {
        val readerIndex = msg.readerIndex()
        val firstByte = msg.getByte(readerIndex).toInt() and 0xFF
        val messageTypeUpperNibble = firstByte and 0xF0

        if (mqttMessageTypes.contains(messageTypeUpperNibble)) {
          val remoteAddress = ctx.channel().remoteAddress()
          logger.info(
              "MQTT protocol detected: messageType=0x{}, firstByte=0x{}, remoteAddress={}",
              String.format("%02X", messageTypeUpperNibble),
              String.format("%02X", firstByte),
              remoteAddress
          )
        }
      }
    }
    ctx.fireChannelRead(msg)
  }

  override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
    logger.error("Error in MQTT detection handler", cause)
    ctx.fireExceptionCaught(cause)
  }
}

