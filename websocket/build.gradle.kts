plugins { application }

dependencies {
  implementation(project(":common"))
  implementation("org.springframework.boot:spring-boot-starter-webflux")
  implementation("org.springframework.boot:spring-boot-autoconfigure")
  implementation("org.eclipse.paho:org.eclipse.paho.mqttv5.client:1.2.5")
  implementation("io.netty:netty-codec-mqtt:4.2.7.Final")
}

application { mainClass = "com.lfp.unibus.AppKt" }
