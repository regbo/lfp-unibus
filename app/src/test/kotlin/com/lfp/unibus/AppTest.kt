package com.lfp.unibus

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

/**
 * Test class for App.
 *
 * Tests that the Spring Boot application class can be instantiated.
 */
class AppTest {
  @Test
  fun `app can be instantiated`() {
    val app = App()
    assertNotNull(app)
  }
}
