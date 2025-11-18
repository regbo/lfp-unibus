package com.lfp.unibus

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import java.util.*

/**
 * Custom Jackson deserializer for ByteArray that handles Base64-encoded data.
 *
 * Supports deserializing Base64 strings directly or Data URL format strings
 * (e.g., "data:application/octet-stream;base64,<base64-data>").
 *
 * Implements JsonDeserializer for ByteArray type.
 */
class ByteArrayDeserializer : JsonDeserializer<ByteArray>() {

  /**
   * Deserializes a JSON value to a ByteArray.
   *
   * @param p The JSON parser
   * @param ctxt The deserialization context
   * @return The deserialized ByteArray
   */
  override fun deserialize(p: JsonParser, ctxt: DeserializationContext): ByteArray {
    return deserialize(p.text)
  }

  companion object {

    /**
     * Regular expression pattern for matching Data URL format strings.
     * Matches: data:[<mediatype>][;<encoding>],<data>
     */
    private val regex = Regex("^data:(?:([^/;,]+)/([^;,]+))?(?:;([^,]+))?,(.*)$")

    /**
     * Deserializes a text string to a ByteArray.
     *
     * If the text is in Data URL format (e.g., "data:application/octet-stream;base64,<data>"),
     * extracts the Base64 portion. Otherwise, treats the entire string as Base64.
     *
     * @param text The text string to deserialize (Base64 or Data URL format)
     * @return The decoded ByteArray
     */
    @JvmStatic
    fun deserialize(text: String): ByteArray {
      val m = regex.matchEntire(text)
      val base64 = if (m != null) m.groupValues[4] else text

      return Base64.getDecoder().decode(base64)
    }
  }
}
