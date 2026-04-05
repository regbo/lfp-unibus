plugins { `java-library` }

dependencies {
  implementation("org.springframework.boot:spring-boot-starter-reactor-netty")
  implementation("io.netty:netty-codec-mqtt:4.2.7.Final"){
      exclude(group = "io.netty")
  }
}
