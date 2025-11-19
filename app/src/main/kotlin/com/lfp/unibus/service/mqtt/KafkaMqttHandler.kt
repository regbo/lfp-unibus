package com.lfp.unibus.service.mqtt

import com.lfp.unibus.common.KafkaConfig
import com.lfp.unibus.service.KafkaService
import io.netty.buffer.Unpooled
import io.netty.channel.ChannelHandler
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.SimpleChannelInboundHandler
import io.netty.handler.codec.mqtt.*
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.utils.Bytes
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import reactor.core.Disposable
import reactor.core.publisher.Mono
import reactor.kafka.sender.KafkaSender
import reactor.kafka.sender.SenderRecord
import java.util.*
import java.util.concurrent.ConcurrentHashMap

/**
 * MQTT handler that bridges MQTT connections to Apache Kafka.
 *
 * Handles MQTT connections and translates MQTT PUBLISH/SUBSCRIBE operations into Kafka
 * producer/consumer operations. MQTT topics map directly to Kafka topics.
 *
 * Features:
 * - MQTT PUBLISH messages are forwarded to Kafka as producer records
 * - MQTT SUBSCRIBE messages set up Kafka consumers that publish to MQTT topics
 * - Bidirectional communication: Kafka consumer records are published to MQTT subscribers
 *
 * @param kafkaOptions Map of Kafka configuration options from environment properties
 */
@ChannelHandler.Sharable
@Component
class KafkaMqttHandler(
    var kafkaService: KafkaService,
    var kafkaConfig: KafkaConfig,
) : SimpleChannelInboundHandler<MqttMessage>() {

  private val logger = LoggerFactory.getLogger(this::class.java)

  /** Map of MQTT client ID to Kafka sender for publishing messages. */
  private val kafkaSenders = ConcurrentHashMap<String, KafkaSender<Bytes, Bytes>>()

  /** Map of MQTT topic to Kafka consumer disposables for cleanup. */
  private val kafkaConsumers = ConcurrentHashMap<String, Disposable>()

  /** Map of MQTT client ID to channel context for publishing messages back to MQTT clients. */
  private val clientChannels = ConcurrentHashMap<String, ChannelHandlerContext>()

  override fun channelActive(ctx: ChannelHandlerContext) {
    logger.debug("MQTT client connected: {}", ctx.channel().remoteAddress())
    super.channelActive(ctx)
  }

  override fun channelInactive(ctx: ChannelHandlerContext) {
    logger.debug("MQTT client disconnected: {}", ctx.channel().remoteAddress())
    cleanupClient(ctx)
    super.channelInactive(ctx)
  }

  override fun acceptInboundMessage(msg: Any?): Boolean {
    return super.acceptInboundMessage(msg)
  }

  override fun channelRead0(ctx: ChannelHandlerContext?, msg: MqttMessage?) {
    if (ctx != null) {
      try {
        val messageType = msg?.fixedHeader()?.messageType()

        when (messageType) {
          MqttMessageType.CONNECT -> handleConnect(ctx, msg as MqttConnectMessage)
          MqttMessageType.PUBLISH -> handlePublish(ctx, msg as MqttPublishMessage)
          MqttMessageType.SUBSCRIBE -> handleSubscribe(ctx, msg as MqttSubscribeMessage)
          MqttMessageType.UNSUBSCRIBE -> handleUnsubscribe(ctx, msg as MqttUnsubscribeMessage)
          MqttMessageType.PINGREQ -> handlePingReq(ctx)
          MqttMessageType.DISCONNECT -> handleDisconnect(ctx)
          else -> {
            logger.warn("Unhandled MQTT message type: {}", msg)
          }
        }
      } catch (e: Exception) {
        logger.error("Error handling MQTT message", e)
        ctx.close()
      }
    }
  }

  /**
   * Handles MQTT CONNECT message.
   *
   * Sends CONNACK response and stores the client channel context.
   *
   * @param ctx Channel handler context
   * @param msg MQTT CONNECT message
   */
  private fun handleConnect(ctx: ChannelHandlerContext, msg: MqttConnectMessage) {
    val clientId = msg.payload().clientIdentifier()
    logger.info(
        "MQTT CONNECT from client: {}, remoteAddress: {}",
        clientId,
        ctx.channel().remoteAddress(),
    )

    clientChannels[clientId] = ctx
    val connackFixedHeader =
        MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0)
    val mqttConnAckVariableHeader =
        MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_ACCEPTED, false)
    val connack = MqttConnAckMessage(connackFixedHeader, mqttConnAckVariableHeader)
    ctx.writeAndFlush(connack)
  }

  /**
   * Handles MQTT PUBLISH message.
   *
   * Forwards the message to Kafka as a producer record.
   *
   * @param ctx Channel handler context
   * @param msg MQTT PUBLISH message
   */
  private fun handlePublish(ctx: ChannelHandlerContext, msg: MqttPublishMessage) {
    val topic = msg.variableHeader().topicName()
    val payload = msg.payload()
    val qos = msg.fixedHeader().qosLevel()

    logger.debug(
        "MQTT PUBLISH: topic={}, qos={}, payloadSize={}",
        topic,
        qos,
        payload.readableBytes(),
    )

    val kafkaSender = getOrCreateKafkaSender(getClientId(ctx))

    val payloadBytes = ByteArray(payload.readableBytes())
    payload.readBytes(payloadBytes)
    payload.resetReaderIndex()

    val kafkaRecord =
        ProducerRecord<Bytes, Bytes>(topic, null, null, null, Bytes.wrap(payloadBytes), null)

    val senderRecord = SenderRecord.create(kafkaRecord, null)

    kafkaSender
        .send(Mono.just(senderRecord))
        .subscribe(
            { result ->
              logger.debug("Forwarded MQTT PUBLISH to Kafka: topic={}", topic)
              if (qos.value() > 0) {
                sendPubAck(ctx, msg.variableHeader().packetId())
              }
            },
            { error ->
              logger.error("Error forwarding MQTT PUBLISH to Kafka: topic={}", topic, error)
            },
        )
  }

  /**
   * Handles MQTT SUBSCRIBE message.
   *
   * Sets up Kafka consumer for the subscribed topics and forwards Kafka records to MQTT.
   *
   * @param ctx Channel handler context
   * @param msg MQTT SUBSCRIBE message
   */
  private fun handleSubscribe(ctx: ChannelHandlerContext, msg: MqttSubscribeMessage) {
    val packetId = msg.variableHeader().messageId()
    val topicSubscriptions = msg.payload().topicSubscriptions()

    logger.info(
        "MQTT SUBSCRIBE: packetId={}, topics={}",
        packetId,
        topicSubscriptions.map { it.topicName() },
    )

    val clientId = getClientId(ctx)

    topicSubscriptions.forEach { subscription ->
      val mqttTopic = subscription.topicName()
      val qos = subscription.qualityOfService()

      val consumer =
          kafkaService.consumer(
              kafkaConfig.consumer(mapOf(ConsumerConfig.CLIENT_ID_CONFIG to getClientId(ctx))),
              mqttTopic,
          )

      val disposable =
          consumer.receive().subscribe { record -> publishToMqtt(ctx, mqttTopic, record, qos) }

      kafkaConsumers[mqttTopic] = disposable
    }

    val grantedQosLevels = topicSubscriptions.map { it.qualityOfService().value() }.toList()
    val subAckMessage =
        MqttMessageFactory.newMessage(
            MqttFixedHeader(MqttMessageType.SUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
            MqttMessageIdVariableHeader.from(packetId),
            MqttSubAckPayload(grantedQosLevels as Iterable<Int>),
        )
    ctx.writeAndFlush(subAckMessage)
  }

  /**
   * Handles MQTT UNSUBSCRIBE message.
   *
   * Stops Kafka consumers for the unsubscribed topics.
   *
   * @param ctx Channel handler context
   * @param msg MQTT UNSUBSCRIBE message
   */
  private fun handleUnsubscribe(ctx: ChannelHandlerContext, msg: MqttUnsubscribeMessage) {
    val packetId = msg.variableHeader().messageId()
    val topics = msg.payload().topics()

    logger.info("MQTT UNSUBSCRIBE: packetId={}, topics={}", packetId, topics)

    topics.forEach { topic ->
      kafkaConsumers[topic]?.dispose()
      kafkaConsumers.remove(topic)
    }

    val unsubAckMessage =
        MqttMessageFactory.newMessage(
            MqttFixedHeader(MqttMessageType.UNSUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
            MqttMessageIdVariableHeader.from(packetId),
            null,
        )
    ctx.writeAndFlush(unsubAckMessage)
  }

  /**
   * Handles MQTT PINGREQ message.
   *
   * Sends PINGRESP response.
   *
   * @param ctx Channel handler context
   */
  private fun handlePingReq(ctx: ChannelHandlerContext) {
    val pingRespMessage =
        MqttMessageFactory.newMessage(
            MqttFixedHeader(MqttMessageType.PINGRESP, false, MqttQoS.AT_MOST_ONCE, false, 0),
            null,
            null,
        )
    ctx.writeAndFlush(pingRespMessage)
  }

  /**
   * Handles MQTT DISCONNECT message.
   *
   * Cleans up client resources.
   *
   * @param ctx Channel handler context
   */
  private fun handleDisconnect(ctx: ChannelHandlerContext) {
    logger.info("MQTT DISCONNECT: {}", ctx.channel().remoteAddress())
    cleanupClient(ctx)
  }

  /**
   * Publishes a Kafka record to MQTT subscribers.
   *
   * @param ctx Channel handler context
   * @param topic MQTT topic name
   * @param record Kafka consumer record
   * @param qos MQTT QoS level
   */
  private fun publishToMqtt(
      ctx: ChannelHandlerContext,
      topic: String,
      record: ConsumerRecord<Bytes, Bytes>,
      qos: MqttQoS,
  ) {
    val payload = record.value()?.get() ?: return
    val publishMessage =
        MqttMessageFactory.newMessage(
            MqttFixedHeader(MqttMessageType.PUBLISH, false, qos, false, 0),
            MqttPublishVariableHeader(topic, 0),
            Unpooled.wrappedBuffer(payload),
        )

    ctx.writeAndFlush(publishMessage)
    logger.debug("Published Kafka record to MQTT: topic={}, offset={}", topic, record.offset())
  }

  /**
   * Sends PUBACK message for QoS 1+ messages.
   *
   * @param ctx Channel handler context
   * @param packetId MQTT packet ID
   */
  private fun sendPubAck(ctx: ChannelHandlerContext, packetId: Int) {
    val pubAckMessage =
        MqttMessageFactory.newMessage(
            MqttFixedHeader(MqttMessageType.PUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
            MqttMessageIdVariableHeader.from(packetId),
            null,
        )
    ctx.writeAndFlush(pubAckMessage)
  }

  /**
   * Gets or creates a Kafka sender for a client.
   *
   * @param clientId MQTT client ID
   * @return Kafka sender instance
   */
  private fun getOrCreateKafkaSender(clientId: String): KafkaSender<Bytes, Bytes> {
    return kafkaSenders.computeIfAbsent(clientId) {
      var producerConfig = kafkaConfig.producer(mapOf(ProducerConfig.CLIENT_ID_CONFIG to clientId))
      val producer = kafkaService.producer(producerConfig)
      producer
    }
  }

  /**
   * Gets the client ID from the channel context.
   *
   * @param ctx Channel handler context
   * @return Client ID or generated ID if not found
   */
  private fun getClientId(ctx: ChannelHandlerContext): String {
    return clientChannels.entries.firstOrNull { it.value == ctx }?.key
        ?: "anonymous.${UUID.randomUUID()}"
  }

  /**
   * Cleans up resources for a disconnected client.
   *
   * @param ctx Channel handler context
   */
  private fun cleanupClient(ctx: ChannelHandlerContext) {
    val clientId = getClientId(ctx)
    clientChannels.remove(clientId)

    kafkaSenders[clientId]?.close()
    kafkaSenders.remove(clientId)

    kafkaConsumers.values.forEach { it.dispose() }
    kafkaConsumers.clear()

    logger.debug("Cleaned up resources for client: {}", clientId)
  }

  override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
    logger.error("Exception in MQTT handler", cause)
    cleanupClient(ctx)
    ctx.close()
  }
}
