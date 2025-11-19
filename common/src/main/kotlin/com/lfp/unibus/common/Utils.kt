package com.lfp.unibus.common

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.json.JsonMapper

object Utils {

  private val objectMapper = lazy {
    val builder = JsonMapper.builder().findAndAddModules()
    return@lazy builder.build()
  }

  fun objectMapper(): ObjectMapper {
    return this.objectMapper.value
  }

  fun get(map: Map<String, Any?>?, vararg keys: String): Any? {
    var value: Any? = map
    for (key in keys) {
      if (value is Map<*, *>) {
        value = value[key]
      } else {
        return null
      }
    }
    return value
  }

  fun flatten(map: Map<String, Any?>?): LinkedHashMap<String, Any> {
    val result = LinkedHashMap<String, Any>()
    if (map == null || map.isEmpty()) return result
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
    walk("", map)
    return result
  }
}
