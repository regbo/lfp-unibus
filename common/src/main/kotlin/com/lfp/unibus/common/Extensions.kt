package com.lfp.unibus.common

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


}
