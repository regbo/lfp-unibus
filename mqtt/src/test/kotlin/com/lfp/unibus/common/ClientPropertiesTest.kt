package com.lfp.unibus.common

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.BytesDeserializer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class ClientPropertiesTest {

  @Test
  fun `consumer get includes whitelisted settings and required defaults`() {
    val props =
        ClientProperties.CONSUMER.get(
            mapOf(
                ConsumerConfig.GROUP_ID_CONFIG to "group-1",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "false",
            )
        )

    assertEquals("group-1", props[ConsumerConfig.GROUP_ID_CONFIG])
    assertEquals("false", props[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG])
    assertEquals(BytesDeserializer::class.java.name, props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG])
    assertEquals(BytesDeserializer::class.java.name, props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG])
  }

  @Test
  fun `consumer get ignores properties whitelisted for other clients`() {
    val props =
        ClientProperties.CONSUMER.get(
            mapOf(
                ConsumerConfig.GROUP_ID_CONFIG to "group-1",
                ProducerConfig.ACKS_CONFIG to "all",
            )
        )

    assertEquals("group-1", props[ConsumerConfig.GROUP_ID_CONFIG])
    assertFalse(props.containsKey(ProducerConfig.ACKS_CONFIG))
  }

  @Test
  fun `consumer get rejects unknown properties`() {
    val error =
        assertThrows(IllegalStateException::class.java) {
          ClientProperties.CONSUMER.get(mapOf("unknown.config" to "value"))
        }

    assertTrue(error.message!!.contains("Invalid CONSUMER property"))
  }

  @Test
  fun `producer scoped properties override generic keys`() {
    val props =
        ClientProperties.PRODUCER.get(
            mapOf(
                ProducerConfig.ACKS_CONFIG to "1",
                "producer.${ProducerConfig.ACKS_CONFIG}" to "all",
            )
        )

    assertEquals("all", props[ProducerConfig.ACKS_CONFIG])
  }

  @Test
  fun `consumer ignores producer scoped unknown properties`() {
    val props =
        ClientProperties.CONSUMER.get(
            mapOf(
                "producer.unknown.prop" to "value",
                ConsumerConfig.GROUP_ID_CONFIG to "group-1",
            )
        )

    assertEquals("group-1", props[ConsumerConfig.GROUP_ID_CONFIG])
    assertFalse(props.containsKey("producer.unknown.prop"))
  }

  @Test
  fun `later property maps override earlier ones`() {
    val props =
        ClientProperties.PRODUCER.get(
            mapOf(ProducerConfig.ACKS_CONFIG to "1"),
            mapOf(ProducerConfig.ACKS_CONFIG to "all"),
        )

    assertEquals("all", props[ProducerConfig.ACKS_CONFIG])
  }

  @Test
  fun `default identifiers added when missing`() {
    val props = ClientProperties.CONSUMER.get()

    val clientId = props[CommonClientConfigs.CLIENT_ID_CONFIG] as String
    val groupId = props[ConsumerConfig.GROUP_ID_CONFIG] as String

    assertTrue(clientId.startsWith("unibus."))
    assertTrue(groupId.startsWith("unibus."))
  }
}
