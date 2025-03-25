plugins {
    java
}

allprojects {
    repositories {
        mavenCentral()
    }
}

tasks.test {
    useJUnitPlatform()
}