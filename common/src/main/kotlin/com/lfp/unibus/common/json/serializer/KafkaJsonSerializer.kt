package com.lfp.unibus.common.json.serializer

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import org.springframework.beans.BeanUtils
import kotlin.reflect.KClass

/**
 * Generic Jackson serializer for Kafka types.
 *
 * Serializes Kafka objects (ConsumerRecord, SenderResult, RecordMetadata) by reflecting
 * over their properties and methods, converting them to JSON objects.
 *
 * @param cls Kotlin class of the type to serialize
 */
class KafkaJsonSerializer<T : Any>(cls: KClass<T>) : StdSerializer<T>(cls.java) {

  private val handledTypeProperties = lazy {
    val beanProps =
        BeanUtils.getPropertyDescriptors(handledType()).mapNotNull { pd ->
          pd.readMethod?.let { Pair(pd.name, it) }
        }
    val methodProps =
        handledType()
            .methods
            .filter { it.parameterCount == 0 }
            .mapNotNull { method ->
              if (beanProps.find { method == it.second } == null) {
                Pair(method.name, method)
              } else {
                null
              }
            }
    val props =
        (beanProps + methodProps)
            .filter { it.second.declaringClass != Any::class.java }
            .filter {
              try {
                Any::class.java.getMethod(it.second.name)
                false
              } catch (_: NoSuchMethodException) {
                true
              }
            }
            .distinctBy { it.first }
    return@lazy props
  }

  override fun serialize(value: T, gen: JsonGenerator, serializers: SerializerProvider) {
    var props = handledTypeProperties.value
    gen.writeStartObject()
    for (prop in props) {
      gen.writeFieldName(prop.first)
      val value = prop.second.invoke(value)
      serializers.defaultSerializeValue(value, gen)
    }
    gen.writeEndObject()
  }
}
