package com.lfp.unibus.common

import com.lfp.unibus.common.Extensions.lookup
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class ExtensionsTest {
  @Test
  fun lookup1() {
    val map = mapOf("a" to mapOf("b.c" to mapOf("d" to 1)))
    val values = map.lookup("a.b", "c", "d").toList()
    assertEquals(listOf(1), values)
  }

  @Test
  fun lookup2() {
    val map = mapOf("a.b.c" to mapOf("d" to 1), "a.b" to mapOf("c.d" to 2))
    val values = map.lookup("a.b.c.d").toList()
    assertEquals(listOf(1, 2), values)
  }

  @Test
  fun lookupEmptyMap() {
    val map = emptyMap<String, Any>()
    val values = map.lookup("a.b.c").toList()
    assertEquals(emptyList(), values)
  }

  @Test
  fun lookupNullMap() {
    val map: Map<*, *>? = null
    val values = map.lookup("a.b.c").toList()
    assertEquals(emptyList(), values)
  }

  @Test
  fun lookupSimpleSingleKey() {
    val map = mapOf("a" to 1, "b" to 2)
    val values = map.lookup("a").toList()
    assertEquals(listOf(1), values)
  }

  @Test
  fun lookupNonExistentKey() {
    val map = mapOf("a" to 1, "b" to 2)
    val values = map.lookup("c").toList()
    assertEquals(emptyList(), values)
  }

  @Test
  fun lookupNestedSinglePath() {
    val map = mapOf("a" to mapOf("b" to mapOf("c" to 1)))
    val values = map.lookup("a.b.c").toList()
    assertEquals(listOf(1), values)
  }

  @Test
  fun lookupMultipleSeparateKeys() {
    val map = mapOf("a" to 1, "b" to 2, "c" to 3)
    val values = map.lookup("a", "b", "c").toList()
    assertEquals(listOf(1, 2, 3), values)
  }

  @Test
  fun lookupMixedDotAndNonDotKeys() {
    val map = mapOf("a.b" to mapOf("c" to 1), "a" to mapOf("b.c" to 2))
    val values = map.lookup("a", "b.c").toList()
    assertEquals(listOf(2), values)
  }

  @Test
  fun lookupMultipleMatchesAtDifferentLevels() {
    val map = mapOf(
      "a.b.c.d" to 1,
      "a.b" to mapOf("c.d" to 2),
      "a" to mapOf("b.c.d" to 3)
    )
    val values = map.lookup("a.b.c.d").toList()
    assertEquals(listOf(1, 2, 3), values)
  }

  @Test
  fun lookupDeeplyNestedStructure() {
    val map = mapOf(
      "level1" to mapOf(
        "level2" to mapOf(
          "level3" to mapOf(
            "level4" to mapOf(
              "value" to 42
            )
          )
        )
      )
    )
    val values = map.lookup("level1.level2.level3.level4.value").toList()
    assertEquals(listOf(42), values)
  }

  @Test
  fun lookupWithPartialMatches() {
    val map = mapOf(
      "x.y.z" to 1,
      "x.y" to mapOf("z" to 2),
      "x" to mapOf("y.z" to 3)
    )
    val values = map.lookup("x.y.z").toList()
    assertEquals(listOf(1, 2, 3), values)
  }

  @Test
  fun lookupWithMultipleVarargKeys() {
    val map = mapOf("a" to mapOf("b.c" to mapOf("d.e" to mapOf("f" to 1))))
    val values = map.lookup("a.b", "c.d", "e.f").toList()
    assertEquals(listOf(1), values)
  }

  @Test
  fun lookupWithEmptyKeyParts() {
    val map = mapOf("a" to 1, "" to 2)
    val values = map.lookup("").toList()
    assertEquals(listOf(2), values)
  }

  @Test
  fun lookupWithNoKeys() {
    val map = mapOf("a" to 1, "b" to 2)
    val values = map.lookup().toList()
    assertEquals(emptyList(), values)
  }

  @Test
  fun lookupWithNonMapValueInPath() {
    val map = mapOf("a" to 1, "a.b" to 2)
    val values = map.lookup("a.b").toList()
    assertEquals(listOf(2), values)
  }

  @Test
  fun lookupComplexNestedScenario() {
    val map = mapOf(
      "config" to mapOf(
        "database.host" to "localhost",
        "database" to mapOf("port" to 5432)
      ),
      "config.database" to mapOf("name" to "mydb")
    )
    val values = map.lookup("config.database.host").toList()
    assertEquals(listOf("localhost"), values)
    
    val values2 = map.lookup("config.database.port").toList()
    assertEquals(listOf(5432), values2)
    
    val values3 = map.lookup("config.database.name").toList()
    assertEquals(listOf("mydb"), values3)
  }

  @Test
  fun lookupWithStringValues() {
    val map = mapOf(
      "user.name" to "John",
      "user" to mapOf("email" to "john@example.com")
    )
    val values = map.lookup("user.name").toList()
    assertEquals(listOf("John"), values)
  }

  @Test
  fun lookupWithNumericValues() {
    val map = mapOf(
      "count" to 100,
      "stats" to mapOf("total" to 200)
    )
    val values1 = map.lookup("count").toList()
    assertEquals(listOf(100), values1)
    
    val values2 = map.lookup("stats.total").toList()
    assertEquals(listOf(200), values2)
  }
}
