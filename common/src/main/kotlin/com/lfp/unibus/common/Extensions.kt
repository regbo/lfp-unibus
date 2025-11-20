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

  /**
   * Looks up values in a nested map structure using flexible key path matching.
   *
   * This function searches for values by trying different combinations of the provided keys,
   * splitting keys on dots and attempting matches from longest to shortest key combinations. It
   * handles nested maps and can return multiple matches if different key combinations resolve to
   * values in the map structure.
   *
   * The lookup algorithm:
   * 1. Splits all provided keys on "." to create a list of key parts
   * 2. Tries matching key combinations from longest to shortest
   * 3. For each match found, recursively searches nested maps with remaining key parts
   * 4. Returns all matching values as a sequence
   *
   * Example:
   * ```
   * val map = mapOf("a.b" to mapOf("c" to 1), "a" to mapOf("b.c" to 2))
   * map.lookup("a.b.c").toList() // Returns [1, 2]
   * ```
   *
   * @param keys Variable number of key strings that may contain dots for nested lookups
   * @return Sequence of matching values found in the map structure
   */
  fun Map<*, *>?.lookup(vararg keys: String): Sequence<Any> {

    fun isEmptyMap(map: Map<*, *>?): Boolean {
      return map == null || map.isEmpty()
    }

    fun lookupKeyParts(map: Map<*, *>?, parts: List<String>): Sequence<Any> {
      if (isEmptyMap(map) || parts.isEmpty()) return emptySequence()

      return sequence {
        for (i in parts.size downTo 1) {
          val head = parts.subList(0, i)
          val value = map!![head.joinToString(".")]
          if (value != null) {
            val tail = parts.subList(i, parts.size)
            if (tail.isEmpty()) {
              yield(value)
            } else if (value is Map<*, *>) {
              yieldAll(lookupKeyParts(value, tail))
            }
          }
        }
      }
    }

    if (isEmptyMap(this)) return emptySequence()

    val parts = keys.flatMap { it.split(".") }

    return lookupKeyParts(this, parts)
  }
}
