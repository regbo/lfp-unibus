package com.lfp.unibus.common.json.serializer

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import kotlin.reflect.KClass
import org.springframework.beans.BeanUtils

/**
 * Generic Jackson serializer for Kafka types.
 *
 * Serializes Kafka objects (ConsumerRecord, SenderResult, RecordMetadata) by reflecting over their
 * properties and methods, converting them to JSON objects.
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
            .filter { method -> method.parameterCount == 0 }
            .filter { method -> beanProps.find { method == it.second } == null }
            .map { method -> Pair(method.name, method) }
    val props =
        (beanProps + methodProps)
            .filter { it.second.declaringClass != Any::class.java }
            .filter {
              runCatching { Any::class.java.getMethod(it.second.name) }
                  .map { false }
                  .getOrElse { true }
            }
            .distinctBy { it.first }
    return@lazy props
  }

  /**
   * Serializes a Kafka object to JSON by reflecting over its properties and methods.
   *
   * @param value Object to serialize
   * @param gen JSON generator
   * @param serializers Serializer provider
   */
  override fun serialize(value: T, gen: JsonGenerator, serializers: SerializerProvider) {
    val props = handledTypeProperties.value
    gen.writeStartObject()
    for (prop in props) {
      gen.writeFieldName(prop.first)
      val value = prop.second.invoke(value)
      serializers.defaultSerializeValue(value, gen)
    }
    gen.writeEndObject()
  }
}
