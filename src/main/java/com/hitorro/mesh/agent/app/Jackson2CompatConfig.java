/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.mesh.agent.app;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.hitorro.util.json.Jackson2JdkModule;
import com.hitorro.util.json.JsonMappers;
import org.springframework.boot.actuate.endpoint.jackson.EndpointJsonMapper;
import org.springframework.boot.health.actuate.endpoint.IndicatedHealthDescriptor;
import org.springframework.boot.jackson.autoconfigure.JsonFactoryBuilderCustomizer;
import org.springframework.boot.jackson.autoconfigure.JsonMapperBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.MapperFeature;
import tools.jackson.databind.cfg.DateTimeFeature;


/**
 * Keeps the REST/JSON format the app had on Jackson 2 (Spring Boot's auto-configured Jackson 2
 * mapper), now that Spring MVC uses Boot's Jackson 3 {@code JsonMapper}. The mapper-level part is
 * set by {@link Jackson2Defaults} (lowest-precedence defaults, so it survives a deployment that
 * replaces application.yml with {@code --spring.config.location}): {@code spring.jackson.use-jackson2-defaults} (Jackson 2's defaults plus Boot's:
 * ISO dates, unknown properties ignored, ...) and Jackson 2.18's stream limits
 * ({@code spring.jackson.factory.constraints.*}). This adds what has no property: the factory-level
 * Jackson 2 defaults and hitorro-core's {@link Jackson2JdkModule} (Jackson 2's {@code Locale},
 * {@code Month} and {@code byte[]} handling, which Jackson 3 changed), as {@code JsonMappers} has.
 * <p>
 * Actuator endpoints have their own mapper. On Jackson 2 Boot built it from
 * {@code Jackson2ObjectMapperBuilder.json()} (Jackson 2 order, unknown properties and the default
 * view ignored) with dates and durations as ISO strings and {@code NON_NULL} inclusion; Boot 4's
 * Jackson 3 one is a plain {@code JsonMapper.builder()}, which sorts properties alphabetically
 * ({@code /actuator/health} became {@code {"groups":..,"status":"UP"}}). application.yml turns that
 * one off ({@code management.endpoints.jackson.isolated-json-mapper: false}) and
 * {@link #hitorroEndpointJsonMapper()} provides the Jackson 2 equivalent.
 */
@Configuration(proxyBeanMethods = false)
public class Jackson2CompatConfig {

    @Bean
    JsonFactoryBuilderCustomizer jackson2FactoryDefaults() {
        return builder -> builder.configureForJackson2();
    }

    @Bean
    JsonMapperBuilderCustomizer jackson2JdkTypes() {
        return builder -> builder.addModule(new Jackson2JdkModule());
    }

    /**
     * Named so it can never clash with Boot's own {@code endpointJsonMapper} (created whenever
     * {@code isolated-json-mapper} isn't false, e.g. when a deployment replaces application.yml
     * with {@code --spring.config.location}), and {@code @Primary} so the actuator uses this one
     * even then.
     */
    @Bean
    @Primary
    EndpointJsonMapper hitorroEndpointJsonMapper() {
        var mapper = JsonMappers.builder()           // Jackson 2 defaults, factory features and 2.18 limits
                .disable(DateTimeFeature.WRITE_DATES_AS_TIMESTAMPS)
                .disable(DateTimeFeature.WRITE_DURATIONS_AS_TIMESTAMPS)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .disable(MapperFeature.DEFAULT_VIEW_INCLUSION)
                .changeDefaultPropertyInclusion(i -> i.withValueInclusion(JsonInclude.Include.NON_NULL)
                        .withContentInclusion(JsonInclude.Include.NON_NULL))
                .addMixIn(IndicatedHealthDescriptor.class, IndicatedHealthOrder.class)
                .build();                                // (JsonMappers.builder() adds Jackson2JdkModule)
        return () -> mapper;
    }


    /**
     * A health component is {@code {"status":..,"details":..}} on Jackson 2 (the inherited status getter
     * first). The descriptor has getters only, and Jackson 3's order for those follows the JVM's method
     * order, which changes between runs; this pins Jackson 2's.
     */
    @JsonPropertyOrder({"status", "details"})
    private abstract static class IndicatedHealthOrder {
    }
}
