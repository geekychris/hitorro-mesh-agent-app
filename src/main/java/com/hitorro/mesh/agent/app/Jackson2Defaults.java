/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.mesh.agent.app;

import org.apache.commons.logging.Log;
import org.springframework.boot.EnvironmentPostProcessor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.logging.DeferredLogFactory;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;

import java.util.Map;

/**
 * The JSON settings the app depends on, as the LOWEST-precedence property source. They used to live
 * only in application.yml, and hitorro-mesh-k8s' chart starts the app with
 * {@code --spring.config.location=file:/config/*.yml}, which replaces that file: Boot then built its
 * own isolated actuator mapper (a second {@code endpointJsonMapper} bean, so the app didn't start) and
 * MVC lost the Jackson 2 format. Any config file, env var or argument still overrides these.
 *
 * <p>One setting is corrected instead: {@code spring.http.converters.preferred-json-mapper=jackson2}, which
 * 3.x deployments set (cloud-stack's deploy/mesh image and overlay do) to pick Boot's Jackson 2 mapper. This
 * app has no Jackson 2 mapper, and the value makes Boot skip its Jackson 3 converter, so Spring MVC falls back
 * to a {@code JsonMapper} with Jackson 3's defaults and none of the settings above. It is mapped to
 * {@code jackson}, with a warning.
 */
public class Jackson2Defaults implements EnvironmentPostProcessor {

    public static final String SOURCE = "hitorroMeshJsonDefaults";
    static final String OVERRIDE = "hitorroMeshJsonOverrides";
    static final String PREFERRED = "spring.http.converters.preferred-json-mapper";

    private final Log log;

    public Jackson2Defaults(DeferredLogFactory logs) {
        this.log = logs.getLog(Jackson2Defaults.class);
    }

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
        if ("jackson2".equalsIgnoreCase(environment.getProperty(PREFERRED, "").trim())
                && !environment.getPropertySources().contains(OVERRIDE)) {
            log.warn(PREFERRED + "=jackson2 is a 3.x setting (this app has no Jackson 2 mapper); using jackson."
                    + " Remove it from the deployment (env SPRING_HTTP_CONVERTERS_PREFERREDJSONMAPPER).");
            environment.getPropertySources().addFirst(new MapPropertySource(OVERRIDE, Map.of(PREFERRED, "jackson")));
        }
    }
}
