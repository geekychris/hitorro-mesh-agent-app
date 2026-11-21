/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.mesh.agent.app;

import org.springframework.boot.EnvironmentPostProcessor;
import org.springframework.boot.SpringApplication;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;

import java.util.Map;

/**
 * The JSON settings the app depends on, as the LOWEST-precedence property source. They used to live
 * only in application.yml, and hitorro-mesh-k8s' chart starts the app with
 * {@code --spring.config.location=file:/config/*.yml}, which replaces that file: Boot then built its
 * own isolated actuator mapper (a second {@code endpointJsonMapper} bean, so the app didn't start) and
 * MVC lost the Jackson 2 format. Any config file, env var or argument still overrides these.
 */
public class Jackson2Defaults implements EnvironmentPostProcessor {

    public static final String SOURCE = "hitorroMeshJsonDefaults";

    static final Map<String, Object> DEFAULTS = Map.of(
            "spring.jackson.use-jackson2-defaults", "true",
            "spring.jackson.factory.constraints.read.max-nesting-depth", "1000",
            "spring.jackson.factory.constraints.write.max-nesting-depth", "1000",
            "spring.jackson.factory.constraints.read.max-string-length", "20000000",
            "management.endpoints.jackson.isolated-json-mapper", "false");

    @Override
    public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
        if (!environment.getPropertySources().contains(SOURCE)) {
            environment.getPropertySources().addLast(new MapPropertySource(SOURCE, DEFAULTS));
        }
    }
}
