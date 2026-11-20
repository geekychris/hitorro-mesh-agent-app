/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.agent.app;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hitorro.mesh.MeshTransport;
import com.hitorro.mesh.InMemoryMeshTransport;
import com.hitorro.mesh.agent.MeshAgent;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootVersion;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.json.JsonCompareMode;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Starts the whole agent application offline (in-memory transport, no NATS,
 * no S3, journal and pipeline state under a temp dir) and checks that Jackson 2
 * {@link JsonNode} values in responses are written as plain JSON. Spring Boot 4
 * uses Jackson 3 for HTTP by default, which would write a Jackson 2 JsonNode as
 * a Java bean; application.yml selects the Jackson 2 converter with
 * spring.http.converters.preferred-json-mapper.
 */
@SpringBootTest(properties = {
        "hitorro.mesh.agent.id=agent-test",
        "hitorro.mesh.agent.transport=inmemory",
        "hitorro.mesh.agent.capabilities=jvssql",
})
@AutoConfigureMockMvc
@Import(MeshAgentApplicationTest.JsonNodeProbeController.class)
class MeshAgentApplicationTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String[] STATE_PROPERTIES = {"hitorro.agent.home", "hitorro.pipelines.home"};
    private static final String[] previous = new String[STATE_PROPERTIES.length];

    // RuntimeTableJournal and the pipelines module read these as JVM system
    // properties, so they must be set before the context starts.
    static {
        try {
            Path home = Files.createTempDirectory("mesh-agent-app-test");
            for (int i = 0; i < STATE_PROPERTIES.length; i++) {
                previous[i] = System.getProperty(STATE_PROPERTIES[i]);
                System.setProperty(STATE_PROPERTIES[i], home.resolve(STATE_PROPERTIES[i]).toString());
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @AfterAll
    static void restoreStateProperties() {
        for (int i = 0; i < STATE_PROPERTIES.length; i++) {
            if (previous[i] == null) System.clearProperty(STATE_PROPERTIES[i]);
            else System.setProperty(STATE_PROPERTIES[i], previous[i]);
        }
    }

    /** Test-only endpoint that returns a Jackson 2 tree, as HiTorro code hands out. */
    @RestController
    static class JsonNodeProbeController {
        static final String DOC =
                "{\"id\":\"a-1\",\"count\":3,\"ratio\":0.5,\"flag\":true,\"none\":null,"
                + "\"nested\":{\"list\":[1,\"two\",{\"three\":3}]}}";

        @GetMapping("/test/jackson2/node")
        JsonNode node() throws IOException {
            return MAPPER.readTree(DOC);
        }
    }

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ApplicationContext context;

    @Test
    void contextLoadsOfflineOnSpringBoot4() {
        assertThat(SpringBootVersion.getVersion()).startsWith("4.");
        assertThat(context.getBean(MeshTransport.class)).isInstanceOf(InMemoryMeshTransport.class);
        assertThat(context.getBean(MeshAgent.class).agentId()).isEqualTo("agent-test");
        assertThat(context.getBean(RuntimeTableInstaller.class)).isNotNull();
        assertThat(context.getBean(S3AdapterInstaller.class)).isNotNull();
    }

    @Test
    void jsonNodeIsWrittenAsPlainJson() throws Exception {
        mockMvc.perform(get("/test/jackson2/node"))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(content().json(JsonNodeProbeController.DOC, JsonCompareMode.STRICT))
                .andExpect(content().string(not(containsString("nodeType"))));
    }

    @Test
    void actuatorHealthResponds() throws Exception {
        mockMvc.perform(get("/actuator/health"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("UP"));
    }
}
