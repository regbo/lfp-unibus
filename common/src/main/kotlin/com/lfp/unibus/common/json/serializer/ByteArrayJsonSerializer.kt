package com.lfp.unibus.common.json.serializer

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.ObjectCodec
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.node.TextNode
import com.lfp.unibus.common.data.DataUrl
import org.springframework.http.MediaType
import java.nio.ByteBuffer
import java.nio.charset.CodingErrorAction
import java.nio.charset.StandardCharsets

class ByteArrayJsonSerializer : JsonSerializer<ByteArray>() {

  override fun serialize(value: ByteArray?, gen: JsonGenerator, serializers: SerializerProvider?) {
    serialize(value, gen)
  }

  companion object {
    @JvmStatic
    private val DECODER =
        StandardCharsets.UTF_8.newDecoder()
            .onMalformedInput(CodingErrorAction.REPORT)
            .onUnmappableCharacter(CodingErrorAction.REPORT)

    @JvmStatic
    fun serialize(value: ByteArray?, gen: JsonGenerator) {
      if (value == null) return
      val binaryNode =
          runCatching { DECODER.decode(ByteBuffer.wrap(value)) }
              .map { null }
              .getOrElse {
                val dataUrl = DataUrl(mediaType = MediaType.APPLICATION_OCTET_STREAM, data = value)
                TextNode.valueOf(dataUrl.toString())
              }
      if (binaryNode != null) {
        gen.writeObject(binaryNode)
      } else {
        val node = toNode(gen.codec, value)
        gen.writeObject(node)
      }
    }

    @JvmStatic
    private fun toNode(codec: ObjectCodec, value: ByteArray): JsonNode {
      return runCatching {
            if (codec is ObjectMapper) {
              codec.readTree(value)
            } else {
              codec.factory.createParser(value).use { p -> codec.readTree(p) }
            }
          }
          .getOrElse { TextNode.valueOf(String(value)) }
    }
  }
}
