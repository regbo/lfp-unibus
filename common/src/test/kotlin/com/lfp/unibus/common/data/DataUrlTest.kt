package com.lfp.unibus.common.data

import org.junit.jupiter.api.Test

class DataUrlTest {
  private val content =
      "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQImWNgYAAAAAMAAWgmWQ0AAAAASUVORK5CYII="

  @Test
  fun parse() {
    val url = "data:text/html;charset=utf-8;Base64,${content}"
    println(DataUrl.parse(url))
  }
}