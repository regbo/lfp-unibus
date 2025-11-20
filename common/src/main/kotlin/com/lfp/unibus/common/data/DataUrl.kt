package com.lfp.unibus.common.data

import java.net.URLDecoder
import java.nio.charset.Charset
import java.util.*
import org.springframework.http.MediaType

/**
 * Data URL representation for binary data.
 *
 * Supports parsing and formatting of data: URLs with Base64 encoding. Used for serializing binary
 * data in JSON payloads.
 */
@Suppress("ArrayInDataClass")
data class DataUrl(
    val mediaType: MediaType? = null,
    val charset: Charset? = null,
    val parameters: Map<String, String> = emptyMap(),
    val base64: Boolean = true,
    val data: ByteArray = EMPTY_BYTE_ARRAY,
) {

  override fun toString(): String {

    val sb = StringBuilder()

    fun appendMeta(key: String, value: String? = null) {
      if (key.isEmpty()) return
      if (!sb.isEmpty()) sb.append(";")
      sb.append(key)
      if (value != null) {
        sb.append("=")
        sb.append(value)
      }
    }

    if (mediaType != null) {
      appendMeta(mediaType.toString())
    }
    if (charset != null) {
      appendMeta("charset", charset.name())
    }
    for ((k, v) in parameters) {
      appendMeta(k, v)
    }
    if (base64) {
      appendMeta("base64")
    }
    sb.append(",")
    sb.append(Base64.getEncoder().encodeToString(data))
    sb.insert(0, "data:")
    return sb.toString()
  }

  companion object {

    @JvmStatic private val EMPTY_BYTE_ARRAY = ByteArray(0)
    private const val DATA_URL_PREFIX = "data:"
    private const val BASE64_TOKEN = "base64"

    /**
     * Parses a data URL string.
     *
     * Supports data URLs with media type, charset, parameters, and Base64 encoding.
     *
     * @param input Data URL string to parse
     * @return Parsed DataUrl instance or null if invalid
     */
    @JvmStatic
    fun parse(input: String?): DataUrl? {
      val s = input?.trim() ?: return null
      if (!s.startsWith(DATA_URL_PREFIX, ignoreCase = true)) return null

      val comma = s.indexOf(",")
      if (comma < 0) return null

      val header = s.substring(DATA_URL_PREFIX.length, comma)
      val payload = s.substring(comma + 1)

      val parts = if (header.isEmpty()) emptyList() else header.split(";")
      if (!parts.isEmpty() && parts.any { it.isEmpty() }) return null

      val base64 = parts.lastOrNull()?.let { BASE64_TOKEN.equals(it, ignoreCase = true) } ?: false

      fun parameterEntry(part: String?): Pair<String, String>? {
        if (part.isNullOrEmpty()) return null
        val paramParts = part.split("=", limit = 2)
        if (paramParts.size != 2) return null
        return Pair(paramParts[0], paramParts[1])
      }

      val parameters = LinkedHashMap<String, String>()

      fun appendParameter(key: String, value: String): Boolean {
        if (key.isEmpty() || value.isEmpty()) return false
        val keyLower = key.lowercase()
        if (!parameters.containsKey(keyLower)) {
          parameters[keyLower] = value
          return true
        } else {
          return parameters[keyLower] == value
        }
      }

      val mediaType: MediaType? =
          if (parts.isEmpty() || (base64 && parts.size == 1)) {
            null
          } else {
            val firstPart = parts.first()
            val parameterEntry = parameterEntry(firstPart)
            if (parameterEntry != null) {
              if (!appendParameter(parameterEntry.first, parameterEntry.second)) return null
              null
            } else {
              val parsedMediaType = runCatching { MediaType.parseMediaType(firstPart) }.getOrNull()
              if (parsedMediaType == null) return null
              parsedMediaType
            }
          }
      val parameterOffset = parameters.size + (if (mediaType != null) 1 else 0)
      val parameterCount = if (base64) parts.size - 1 else parts.size
      for (i in parameterOffset until parameterCount) {
        val parameterEntry = parameterEntry(parts[i]) ?: return null
        if (!appendParameter(parameterEntry.first, parameterEntry.second)) return null
      }
      val charsetName = parameters.remove("charset")
      val charset: Charset? =
          if (charsetName == null) {
            null
          } else {
            val parsedCharset = runCatching { Charset.forName(charsetName) }.getOrNull()
            if (parsedCharset == null) return null
            parsedCharset
          }

      val bytes =
          if (base64) {
            runCatching { Base64.getDecoder().decode(payload) }.getOrNull() ?: return null
          } else {
            val cs = charset ?: Charsets.UTF_8
            runCatching { URLDecoder.decode(payload, Charsets.UTF_8.name()).toByteArray(cs) }
                .getOrNull() ?: return null
          }

      return DataUrl(
          mediaType = mediaType,
          charset = charset,
          parameters = parameters,
          base64 = base64,
          data = bytes,
      )
    }
  }
}
