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
 * Starts the PACKAGED application the way it is deployed: the hitorro-mesh Helm chart
 * (hitorro-mesh-k8s) passes {@code --spring.config.location=file:/config/<app>.yml}, which REPLACES the
 * packaged application.yml, and cloud-stack's deploy/mesh image and overlay set
 * {@code SPRING_HTTP_CONVERTERS_PREFERREDJSONMAPPER=jackson2} and the actuator exposure. The config file
 * below has the chart's shape (offline: in-memory transport).
 *
 * <p>4.0.0 as first released crashed there: Boot's own {@code endpointJsonMapper} (created because
 * {@code management.endpoints.jackson.isolated-json-mapper: false} was only in application.yml) clashed with
 * this app's bean of the same name. With that fixed, the app still wrote Jackson 3's format, because the
 * Jackson settings were also only in application.yml and {@code preferred-json-mapper=jackson2} made Boot
 * skip its Jackson 3 converter. This checks that it starts, health is in Jackson 2's format, and Spring
 * MVC's mapper has Jackson 2's defaults (an error body's {@code timestamp} ends in {@code +00:00}, Jackson
 * 3's {@code Z}).
 */
class DeployedConfigIT {

    /** Chart-shaped Spring config (see hitorro-mesh-k8s helm/hitorro-mesh/templates). */
    static final String CONFIG = """
server:
  port: 8091
spring:
  main:
    banner-mode: off
hitorro:
  mesh:
    agent:
      id: "agent-it"
      transport: inmemory
      heartbeat-interval: 2s
      capabilities:
        - "jvssql"
""";

    @Test
    void startsWithTheDeployedConfigurationAndWritesJackson2() throws Exception {
        String jar = System.getProperty("app.jar");
        assertThat(jar).as("app.jar system property (set by failsafe)").isNotNull();
        assertThat(new File(jar)).exists();
        int port;
        try (ServerSocket s = new ServerSocket(0)) { port = s.getLocalPort(); }
        Path home = Files.createTempDirectory("deployed-config-it");
        Path config = home.resolve("app.yml");
        Files.writeString(config, CONFIG.replace("@HOME@", home.toString()));
        Path log = home.resolve("app.log");
        List<String> cmd = new ArrayList<>(List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Duser.home=" + home, "-Dhitorro.agent.home=" + home.resolve("agent")));
        cmd.addAll(List.of("-jar", jar));
        cmd.addAll(List.of("--spring.config.location=file:" + config, "--server.port=" + port));
        ProcessBuilder pb = new ProcessBuilder(cmd).redirectErrorStream(true).redirectOutput(log.toFile());
        pb.environment().put("SPRING_HTTP_CONVERTERS_PREFERREDJSONMAPPER", "jackson2");
        pb.environment().put("MANAGEMENT_ENDPOINTS_WEB_EXPOSURE_INCLUDE", "health,info,metrics,prometheus");
        Process p = pb.start();
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
            assertThat(out).doesNotContain("APPLICATION FAILED TO START").doesNotContain("NoClassDefFoundError");
            assertThat(p.isAlive()).as("app process alive; log:%n%s", out).isTrue();
            assertThat(health).isEqualTo("{\"status\":\"UP\",\"groups\":[\"liveness\",\"readiness\"]}");
            HttpResponse<String> r = http.send(HttpRequest.newBuilder(
                    URI.create("http://127.0.0.1:" + port + "/actuator/info")).build(), HttpResponse.BodyHandlers.ofString());
            assertThat(r.statusCode()).isEqualTo(200);
            assertThat(r.body()).isEqualTo("{}");
            r = http.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/no/such/path")).build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(r.statusCode()).isEqualTo(404);
            assertThat(r.body()).matches("\\{\"timestamp\":\"\\d{4}-\\d\\d-\\d\\dT\\d\\d:\\d\\d:\\d\\d\\.\\d{3}\\+00:00\","
                    + "\"status\":404,\"error\":\"Not Found\",\"path\":\"/no/such/path\"}");
            assertThat(out).contains("preferred-json-mapper=jackson2 is a 3.x setting");
        } finally {
            p.destroy();
            if (!p.waitFor(20, java.util.concurrent.TimeUnit.SECONDS)) p.destroyForcibly();
        }
    }
}
