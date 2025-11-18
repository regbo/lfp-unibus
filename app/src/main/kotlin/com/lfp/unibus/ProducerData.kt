package com.lfp.unibus

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ContainerNode
import kotlin.reflect.KProperty1

@Suppress("ArrayInDataClass")
data class ProducerData(
    val partition: Int?,
    val timestamp: Long?,
    val key: JsonNode?,
    val keyBinary: ByteArray?,
    val value: JsonNode?,
    val valueBinary: ByteArray?,
    val headers: List<Header>?,
) {

  init {
    oneOfCheck(this, ProducerData::key, ProducerData::keyBinary)
    oneOfCheck(this, ProducerData::value, ProducerData::valueBinary)
  }

  companion object {

    @JvmStatic
    fun <T> oneOfCheck(
        data: T,
        nodeProperty: KProperty1<T, JsonNode?>,
        binaryProperty: KProperty1<T, ByteArray?>,
    ) {
      if (nodeProperty.get(data) != null && binaryProperty.get(data) != null) {
        throw IllegalArgumentException(
            "${data!!::class.simpleName} Error: Only one of ${nodeProperty.name} or ${binaryProperty.name} may be set"
        )
      }
    }
  }

  data class Header(val key: String?, val value: JsonNode?, val valueBinary: ByteArray?) {

    init {
      oneOfCheck(this, Header::value, Header::valueBinary)
    }

    companion object {

      @JvmStatic
      @JsonCreator()
      fun create(node: ContainerNode<*>): Header {
        if (node.isArray || (node.isObject && node.size() == 1)) {
          if (node.isArray && !node.isEmpty && node.size() <= 2) {
            val key = node.get(0)?.textValue()
            val value = if (node.size() > 1) node.get(1) else null
            return Header(key, value, null)
          } else if (node.isObject && node.size() == 1) {
            val key = node.fieldNames().next()
            val value = node.get(key)
            return Header(key, value, null)
          }
          throw IllegalArgumentException(
              "${Header::class.simpleName} Error: Invalid pair $node"
          )
        } else {
          return Header(
              node.get(Header::key.name).textValue(),
              node.get(Header::value.name),
              node.get(Header::valueBinary.name)?.let {
                ByteArrayDeserializer.deserialize(it.textValue())
              },
          )
        }
      }
    }
  }
}
