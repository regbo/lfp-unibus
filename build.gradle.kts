import org.jetbrains.kotlin.gradle.dsl.KotlinProjectExtension

plugins {
  alias(libs.plugins.kotlin.jvm) apply false
  alias(libs.plugins.kotlin.spring) apply false
}

subprojects {
  val libs = rootProject.libs

  listOf(libs.plugins.kotlin.jvm, libs.plugins.kotlin.spring).forEach { pluginProvider ->
    pluginManager.apply(pluginProvider.get().pluginId)
  }

  val jdkVersion = (properties["jdk.version"] as String).toInt()

  extensions.configure<JavaPluginExtension>("java") {
    toolchain { languageVersion = JavaLanguageVersion.of(jdkVersion) }
  }
  extensions.configure<KotlinProjectExtension>("kotlin") { jvmToolchain(jdkVersion) }

  tasks.named<Test>("test") { useJUnitPlatform() }

  repositories { mavenCentral() }

  dependencies {
    // main
    add("implementation", platform(libs.spring.boot.dependencies))

    // test
    add("testImplementation", "org.jetbrains.kotlin:kotlin-test-junit5")
    add("testImplementation", libs.junit.jupiter.engine)
    add("testRuntimeOnly", "org.junit.platform:junit-platform-launcher")
  }
}
