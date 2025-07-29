import com.vanniktech.maven.publish.SonatypeHost

plugins {
    id("java-library")
    id("com.vanniktech.maven.publish") version "0.31.0"
}

java {
    withSourcesJar()
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(17))
    }
}

val jacksonVersion = "2.17.2"
val flywayVersion = "11.3.4"
val jdbiVersion = "3.48.0"
val testcontainersVersion = "1.20.6"
val floggerVersion = "0.8"

dependencies {
    implementation("org.postgresql:postgresql:42.7.3")
    implementation("com.zaxxer:HikariCP:5.1.0")
    implementation("com.fasterxml.jackson.core:jackson-databind:$jacksonVersion")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:$jacksonVersion")
    implementation("io.github.resilience4j:resilience4j-retry:2.2.0")
    implementation("com.github.kagkarlsson:db-scheduler:14.0.1")
    implementation("ch.qos.logback:logback-classic:1.5.6")
    implementation("net.bytebuddy:byte-buddy:1.17.2")
    // https://mvnrepository.com/artifact/com.google.flogger/flogger
    implementation("com.google.flogger:flogger:${floggerVersion}")
    // https://mvnrepository.com/artifact/com.google.flogger/flogger-slf4j-backend
    runtimeOnly("com.google.flogger:flogger-slf4j-backend:${floggerVersion}")


    // https://mvnrepository.com/artifact/org.flywaydb/flyway-core
    implementation("org.flywaydb:flyway-core:$flywayVersion")
    // https://mvnrepository.com/artifact/org.flywaydb/flyway-database-postgresql
    runtimeOnly("org.flywaydb:flyway-database-postgresql:$flywayVersion")
    // https://mvnrepository.com/artifact/org.awaitility/awaitility
    implementation("org.awaitility:awaitility:4.3.0")
    // https://mvnrepository.com/artifact/org.jdbi/jdbi3-core
    implementation("org.jdbi:jdbi3-core:$jdbiVersion")
    // https://mvnrepository.com/artifact/org.jdbi/jdbi3-postgres
    testImplementation("org.jdbi:jdbi3-postgres:$jdbiVersion")

    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.testcontainers:junit-jupiter:$testcontainersVersion")
    // https://mvnrepository.com/artifact/org.testcontainers/postgresql
    testImplementation("org.testcontainers:postgresql:$testcontainersVersion")
    // https://mvnrepository.com/artifact/org.mockito/mockito-core
    testImplementation("org.mockito:mockito-core:5.16.1")


}

tasks.withType<Test> {
    testLogging.showStandardStreams = project.hasProperty("stdout")
    useJUnitPlatform()
}

tasks.jar {
    exclude("logback.xml")
}

/* For snapshot releases
 * ./gradlew publishAllPublicationsToMavenCentralRepository -Pversion=SOME-SNAPSHOT
 *
 * For prod releases
 *  ./gradlew publishToMavenCentral -Pversion=semanticVersion --no-configuration-cache
 * Then go here to manually verify publication:
 * https://central.sonatype.com/publishing/deployments
 */
mavenPublishing {
    publishToMavenCentral(SonatypeHost.CENTRAL_PORTAL)
    signAllPublications()
    coordinates(project.group.toString(), "aptflow-core", project.version.toString())
    pom {
        name.set("aptflow-core")
        description.set("Lightweight JVM workflow library")
        inceptionYear.set("2025")
        url.set("https://github.com/aptvantage/aptflow-core")
        licenses {
            license {
                name.set("The Apache License, Version 2.0")
                url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                distribution.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
            }
        }
        developers {
            developer {
                id.set("mnebus")
                name.set("Michael Nebus")
                url.set("https://github.com/mnebus/")
            }
        }
        scm {
            url.set("https://github.com/aptvantage/aptflow/")
            connection.set("scm:git:git://github.com/aptvantage/aptflow.git")
            developerConnection.set("scm:git:ssh://git@github.com/aptvantage/aptflow.git")
        }
    }

}
