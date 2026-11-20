/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.mesh.agent.app;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.hitorro.util.json.JsonMappers;
import org.springframework.boot.actuate.endpoint.jackson.EndpointJsonMapper;
import org.springframework.boot.jackson.autoconfigure.JsonFactoryBuilderCustomizer;
import org.springframework.boot.jackson.autoconfigure.JsonMapperBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.MapperFeature;
import tools.jackson.databind.cfg.DateTimeFeature;
import tools.jackson.databind.module.SimpleModule;
import tools.jackson.databind.ser.std.ToStringSerializer;

import java.util.Locale;

/**
 * Keeps the REST/JSON format the app had on Jackson 2 (Spring Boot's auto-configured Jackson 2
 * mapper), now that Spring MVC uses Boot's Jackson 3 {@code JsonMapper}. The mapper-level part is
 * in application.yml: {@code spring.jackson.use-jackson2-defaults} (Jackson 2's defaults plus Boot's:
 * ISO dates, unknown properties ignored, ...) and Jackson 2.18's stream limits
 * ({@code spring.jackson.factory.constraints.*}). This adds what has no property: the factory-level
 * Jackson 2 defaults and Jackson 2's {@link Locale} format ({@code "fr_CA"}, not the language tag).
 * Same settings as hitorro-spring-boot 4.0.0's {@code HitorroJacksonAutoConfiguration}.
 * <p>
 * Actuator endpoints have their own mapper. On Jackson 2 Boot built it from
 * {@code Jackson2ObjectMapperBuilder.json()} (Jackson 2 order, unknown properties and the default
 * view ignored) with dates and durations as ISO strings and {@code NON_NULL} inclusion; Boot 4's
 * Jackson 3 one is a plain {@code JsonMapper.builder()}, which sorts properties alphabetically
 * ({@code /actuator/health} became {@code {"groups":..,"status":"UP"}}). application.yml turns that
 * one off ({@code management.endpoints.jackson.isolated-json-mapper: false}) and
 * {@link #endpointJsonMapper()} provides the Jackson 2 equivalent.
 */
@Configuration(proxyBeanMethods = false)
public class Jackson2CompatConfig {

    @Bean
    JsonFactoryBuilderCustomizer jackson2FactoryDefaults() {
        return builder -> builder.configureForJackson2();
    }

    @Bean
    JsonMapperBuilderCustomizer jackson2LocaleFormat() {
        return builder -> builder.addModule(localeModule());
    }

    @Bean
    EndpointJsonMapper endpointJsonMapper() {
        var mapper = JsonMappers.builder()           // Jackson 2 defaults, factory features and 2.18 limits
                .disable(DateTimeFeature.WRITE_DATES_AS_TIMESTAMPS)
                .disable(DateTimeFeature.WRITE_DURATIONS_AS_TIMESTAMPS)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .disable(MapperFeature.DEFAULT_VIEW_INCLUSION)
                .changeDefaultPropertyInclusion(i -> i.withValueInclusion(JsonInclude.Include.NON_NULL)
                        .withContentInclusion(JsonInclude.Include.NON_NULL))
                .addModule(localeModule())
                .build();
        return () -> mapper;
    }

    private static SimpleModule localeModule() {
        return new SimpleModule("jackson2-locale").addSerializer(Locale.class, ToStringSerializer.instance);
    }
}
