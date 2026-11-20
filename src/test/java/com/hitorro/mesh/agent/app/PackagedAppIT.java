/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.mesh.agent.app;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Starts the PACKAGED application ({@code java -jar} on the repackaged Spring Boot jar, as a
 * deployment does) and checks that it comes up and {@code /actuator/health} is UP in Jackson 2's
 * format. A sliced or in-IDE test context can't catch packaging problems: on main the driver jar
 * lacked its optional dependencies (Spring Boot 4 leaves them out), so it failed to start with
 * {@code NoClassDefFoundError: org/apache/parquet/io/InputFile}. Runs in {@code verify} (failsafe),
 * after {@code package}.
 */
class PackagedAppIT {

    @Test
    void packagedJarStartsAndIsHealthy() throws Exception {
        String jar = System.getProperty("app.jar");
        assertThat(jar).as("app.jar system property (set by failsafe)").isNotNull();
        assertThat(new File(jar)).exists();
        int port;
        try (ServerSocket s = new ServerSocket(0)) { port = s.getLocalPort(); }
        Path home = Files.createTempDirectory("packaged-app-it");
        Path log = home.resolve("app.log");
        List<String> cmd = new ArrayList<>(List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Duser.home=" + home, "-Dhitorro.agent.home=" + home.resolve("agent"),
                "-jar", jar, "--server.port=" + port));
        cmd.addAll(ARGS);
        Process p = new ProcessBuilder(cmd).redirectErrorStream(true).redirectOutput(log.toFile()).start();
        HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
        try {
            String health = null;
            long deadline = System.currentTimeMillis() + 180_000;
            while (System.currentTimeMillis() < deadline && p.isAlive()) {
                try {
                    HttpResponse<String> r = http.send(HttpRequest.newBuilder(
                            URI.create("http://127.0.0.1:" + port + "/actuator/health")).timeout(Duration.ofSeconds(3)).build(),
                            HttpResponse.BodyHandlers.ofString());
                    if (r.statusCode() == 200) { health = r.body(); break; }
                } catch (Exception ignored) {
                }
                Thread.sleep(500);
            }
            String out = Files.readString(log);
            assertThat(out).doesNotContain("NoClassDefFoundError").doesNotContain("APPLICATION FAILED TO START");
            assertThat(p.isAlive()).as("app process alive; log:%n%s", out).isTrue();
            assertThat(health).isEqualTo("{\"status\":\"UP\",\"groups\":[\"liveness\",\"readiness\"]}");
            HttpResponse<String> r = http.send(HttpRequest.newBuilder(
                    URI.create("http://127.0.0.1:" + port + JSON_PATH)).build(), HttpResponse.BodyHandlers.ofString());
            assertThat(r.statusCode()).isEqualTo(JSON_STATUS);
            assertThat(r.body()).isEqualTo(JSON_BODY);
        } finally {
            p.destroy();
            if (!p.waitFor(20, java.util.concurrent.TimeUnit.SECONDS)) p.destroyForcibly();
        }
    }

    /** In-memory transport, no tables. */
    static final List<String> ARGS = List.of("--hitorro.mesh.agent.transport=inmemory",
            "--hitorro.mesh.agent.id=packaged-it", "--hitorro.mesh.agent.capabilities[0]=jvssql");
    static final String JSON_PATH = "/actuator/info";
    static final int JSON_STATUS = 200;
    static final String JSON_BODY = "{}";
}
