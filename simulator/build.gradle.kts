// Commerce simulator. Fleshed out in Plan 10. Plain Java + JDBC, no Spring.
plugins {
    application
}

dependencies {
    implementation("org.postgresql:postgresql:42.7.4")
    implementation("com.zaxxer:HikariCP:5.1.0")
}

application {
    mainClass.set("io.github.nini4746.cdcview.simulator.Simulator")
}
