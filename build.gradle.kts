plugins {
    kotlin("jvm") version "2.2.21"
    kotlin("plugin.spring") version "2.2.21"
    id("org.springframework.boot") version "4.0.1"
    id("io.spring.dependency-management") version "1.1.7"
//    id("application")
}

group = "kh.com.im"
version = "0.0.1-SNAPSHOT"
description = "temporal-demo"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

repositories {
    mavenCentral()
}

dependencies {

    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-data-jdbc")
    implementation("org.springframework.boot:spring-boot-starter-batch")

    implementation("org.mybatis.spring.boot:mybatis-spring-boot-starter:4.0.1")
    implementation("org.postgresql:postgresql")

    implementation("org.jetbrains.kotlin:kotlin-reflect")
    implementation("tools.jackson.module:jackson-module-kotlin")

    implementation("io.temporal:temporal-sdk:1.32.1")

    testImplementation("io.temporal:temporal-testing:1.32.1")

    testImplementation("org.springframework.boot:spring-boot-starter-webmvc-test")
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit5")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")

}

kotlin {
    compilerOptions {
        freeCompilerArgs.addAll("-Xjsr305=strict", "-Xannotation-default-target=param-property")
    }
}

tasks.withType<Test> {
    useJUnitPlatform()
}

//application {
//    mainClass.set("kh.com.im.temporaldemo.TemporalDemoApplication")
//}

//tasks.register<JavaExec>("runWorker") {
//    group = "application"
//    description = "Run the Temporal worker"
//    classpath = sourceSets.main.get().runtimeClasspath
//    mainClass.set("helloworkflow.SayHelloWorker")
//}
//
//tasks.register<JavaExec>("runStarter") {
//    group = "application"
//    description = "Run the workflow starter"
//    classpath = sourceSets.main.get().runtimeClasspath
//    mainClass.set("helloworkflow.Starter")
//}