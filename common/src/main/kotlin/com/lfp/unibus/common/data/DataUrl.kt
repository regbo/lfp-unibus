package com.lfp.unibus.common.data

import org.springframework.http.MediaType
import java.net.URLDecoder
import java.nio.charset.Charset
import java.util.*

@Suppress("ArrayInDataClass")
data class DataUrl(
    val mediaType: MediaType = DEFAULT_MEDIA_TYPE,
    val charset: Charset? = null,
    val parameters: Map<String, String> = emptyMap(),
    val base64: Boolean = true,
    val data: ByteArray = EMPTY_BYTE_ARRAY,
) {

  override fun toString(): String {
    val b64 = Base64.getEncoder().encodeToString(data)
    return buildString {
      append("data:")
      append(mediaType.toString())
      if (charset != null) {
        append(";charset=")
        append(charset.name())
      }
      for ((k, v) in parameters) {
        append(";")
        append(k)
        append("=")
        append(v)
      }
      if (base64) append(";base64,") else append(",")
      append(b64)
    }
  }

  companion object {

    @JvmStatic private val EMPTY_BYTE_ARRAY = ByteArray(0)
    @JvmStatic private val DEFAULT_MEDIA_TYPE = MediaType.TEXT_PLAIN
    private const val DATA_URL_PREFIX = "data:"
    private const val BASE64_TOKEN = "base64"

    @JvmStatic
    fun parse(input: String?): DataUrl? {
      val s = input?.trim() ?: return null
      if (!s.startsWith(DATA_URL_PREFIX, ignoreCase = true)) return null

      val comma = s.indexOf(",")
      if (comma < 0) return null

      val header = s.substring(DATA_URL_PREFIX.length, comma)
      val payload = s.substring(comma + 1)

      val parts = header.split(";")
      var base64: Boolean? = null
      val mediaTypeRaw = parts.firstOrNull()?.takeIf { !it.isEmpty() }
      if (BASE64_TOKEN.equals(mediaTypeRaw, ignoreCase = true)) {
        base64 = true
      }
      val mediaType: MediaType?
      if (base64 != null || mediaTypeRaw == null) {
        mediaType = null
      } else {
        mediaType = runCatching { MediaType.parseMediaType(mediaTypeRaw) }.getOrNull()
        if (mediaType == null) return null
      }

      var charset: Charset? = null

      val params = linkedMapOf<String, String>()

      for (i in 1 until parts.size) {
        val p = parts[i]
        if (p.isEmpty()) return null

        val kv = p.split("=", limit = 2)

        if (kv.size == 1) {
          if (p.equals(BASE64_TOKEN, ignoreCase = true)) {
            if (base64 != null) return null
            base64 = true
          } else {
            return null
          }
          continue
        }

        val key = kv[0]
        val value = kv[1]

        if (key.isEmpty() || value.isEmpty()) return null

        if (key.equals("charset", ignoreCase = true)) {
          if (charset != null) return null

          charset = runCatching { Charset.forName(value) }.getOrNull() ?: return null
        } else {
          if (params.containsKey(key)) return null
          params[key] = value
        }
      }

      val bytes =
          if (base64 == true) {
            runCatching { Base64.getDecoder().decode(payload) }.getOrNull() ?: return null
          } else {
            val cs = charset ?: Charsets.UTF_8
            runCatching { URLDecoder.decode(payload, Charsets.UTF_8.name()).toByteArray(cs) }
                .getOrNull() ?: return null
          }

      return DataUrl(
          mediaType = mediaType ?: DEFAULT_MEDIA_TYPE,
          charset = charset,
          parameters = params,
          base64 = base64 == true,
          data = bytes,
      )
    }
  }
}
