package com.lfp.unibus.mqtt

import io.netty.channel.ChannelHandlerContext
import io.netty.channel.SimpleChannelInboundHandler
import io.netty.handler.codec.mqtt.MqttMessage

class MqttDetector : SimpleChannelInboundHandler<MqttMessage>() {
    override fun channelRead0(ctx: ChannelHandlerContext?, msg: MqttMessage?) {
        TODO("Not yet implemented")
    }
}
