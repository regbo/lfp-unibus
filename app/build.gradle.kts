plugins {
  alias(libs.plugins.kotlin.jvm)
  alias(libs.plugins.kotlin.spring)
  application
}

repositories { mavenCentral() }

dependencies {
  testImplementation("org.jetbrains.kotlin:kotlin-test-junit5")
  testImplementation(libs.junit.jupiter.engine)
  testRuntimeOnly("org.junit.platform:junit-platform-launcher")
  implementation(platform(libs.spring.boot.dependencies))
  implementation("org.springframework.boot:spring-boot-starter-webflux")
  implementation("jakarta.websocket:jakarta.websocket-api")

  implementation(libs.reactor.kafka)
  implementation(libs.kafka.clients)
}

val jdkVersion = properties.get("jdk.version") as String

java { toolchain { languageVersion = JavaLanguageVersion.of(jdkVersion) } }

kotlin { jvmToolchain(jdkVersion.toInt()) }

application { mainClass = "com.lfp.unibus.MainKt" }

tasks.named<Test>("test") { useJUnitPlatform() }

application { mainClass = "com.lfp.unibus.MainKt" }

tasks.named<Test>("test") { useJUnitPlatform() }

application { mainClass = "com.lfp.unibus.MainKt" }

tasks.named<Test>("test") { useJUnitPlatform() }
