plugins { `java-library` }

dependencies {
  api("org.springframework.boot:spring-boot")
  api("org.springframework:spring-web")
  api("com.fasterxml.jackson.datatype:jackson-datatype-jdk8")
  api("com.fasterxml.jackson.module:jackson-module-kotlin")
  api(libs.reactor.kafka)
  api(libs.kafka.clients)
}
