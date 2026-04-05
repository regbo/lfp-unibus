package com.lfp.unibus.websocket.service.mqtt

import io.netty.channel.ChannelHandler
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundHandlerAdapter
import io.netty.channel.SimpleChannelInboundHandler
import io.netty.handler.codec.mqtt.*
import io.netty.handler.timeout.IdleState
import io.netty.handler.timeout.IdleStateEvent
import io.netty.util.ReferenceCountUtil
import java.nio.charset.StandardCharsets

@ChannelHandler.Sharable
class MqttHeartBeatBrokerHandler private constructor() : SimpleChannelInboundHandler<MqttMessage>() {

  companion object {
    val INSTANCE = MqttHeartBeatBrokerHandler()
  }

    override fun channelRead0(ctx: ChannelHandlerContext, mqttMessage: MqttMessage) {

    println("Received MQTT message: $mqttMessage")

    when (mqttMessage.fixedHeader().messageType()) {
      MqttMessageType.CONNECT -> {
        val connAckHeader =
            MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0)

        val connAckVariableHeader =
            MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_ACCEPTED, false)

        val connAck = MqttConnAckMessage(connAckHeader, connAckVariableHeader)
        ctx.writeAndFlush(connAck)
        ctx.read()
      }

      MqttMessageType.PINGREQ -> {
        val pingRespHeader =
            MqttFixedHeader(MqttMessageType.PINGRESP, false, MqttQoS.AT_MOST_ONCE, false, 0)
        val pingResp = MqttMessage(pingRespHeader)
        ctx.writeAndFlush(pingResp)
      }
      MqttMessageType.PUBLISH -> {
        val publishMessage = mqttMessage as MqttPublishMessage
        println("Received MQTT message: $mqttMessage")
        println("Payload: ${publishMessage.payload()!!.toString(StandardCharsets.UTF_8)}")
      }

      MqttMessageType.DISCONNECT -> {
        ctx.close()
      }

      else -> {
        println("Unexpected message type: ${mqttMessage.fixedHeader().messageType()}")
        ReferenceCountUtil.release(mqttMessage)
        ctx.close()
      }
    }
  }



    override fun userEventTriggered(ctx: ChannelHandlerContext, evt: Any) {
    if (evt is IdleStateEvent && evt.state() == IdleState.READER_IDLE) {
      println("Channel heartbeat lost")
      ctx.close()
    } else {
      super.userEventTriggered(ctx, evt)
    }
  }

  override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
    cause.printStackTrace()
    ctx.close()
  }
}
