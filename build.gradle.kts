plugins {
    java
}

group = "com.aptvantage"
version = "LOCAL-SNAPSHOT"

allprojects {
    repositories {
        mavenCentral()
    }
}

tasks.test {
    useJUnitPlatform()
}