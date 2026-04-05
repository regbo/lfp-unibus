package com.lfp.unibus.common

import org.springframework.web.util.UriComponents
import org.springframework.web.util.UriComponentsBuilder
import java.net.URI

/** Utility functions for object mapping and map operations. */
object Extensions {
  /**
   * Flattens nested map structure to dot-notation keys.
   *
   * Example: {"kafka": {"bootstrap": {"servers": "localhost"}}} -> {"kafka.bootstrap.servers":
   * "localhost"}
   *
   * @param map Nested map to flatten
   * @return Flattened map with dot-notation keys
   */
  fun Map<*, *>?.flatten(): LinkedHashMap<String, Any> {
    val result = LinkedHashMap<String, Any>()
    if (this == null || this.isEmpty()) return result
    fun walk(prefix: String, value: Any) {
      when (value) {
        is Map<*, *> -> {
          value.forEach { (k, v) ->
            if (k is String && v != null) {
              val next = if (prefix.isEmpty()) k else "$prefix.$k"
              walk(next, v)
            }
          }
        }

        else -> result[prefix] = value
      }
    }
    walk("", this)
    return result
  }

  fun URI?.components(): UriComponents? {
    return this?.let { UriComponentsBuilder.fromUri(this).build() }
  }

  fun URI?.queryProperties(): Map<String, String?> {
    return this.components()?.queryParams?.asSingleValueMap() ?: emptyMap()
  }

  fun URI?.topic(): String? {
    val pathSegments = this.components()?.pathSegments ?: emptyList()
    if (pathSegments.isEmpty() || (pathSegments.size == 1 && pathSegments.first().isEmpty()))
        return null
    return pathSegments.joinToString("_")
  }
}
