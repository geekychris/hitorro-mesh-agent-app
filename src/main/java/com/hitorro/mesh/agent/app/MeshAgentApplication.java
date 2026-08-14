/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.mesh.agent.app;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hitorro.jsontypesystem.Type;
import com.hitorro.mesh.InMemoryMeshTransport;
import com.hitorro.mesh.MeshTransport;
import com.hitorro.mesh.agent.AgentConfig;
import com.hitorro.mesh.agent.LocalTable;
import com.hitorro.mesh.agent.MeshAgent;
import com.hitorro.mesh.nats.NatsMeshTransport;
import com.hitorro.mesh.streaming.kafka.KafkaStreamingLocalTable;
import com.hitorro.mesh.streaming.nats.NatsJetStreamLocalTable;
import com.hitorro.streams.kafka.KafkaSource;
import com.hitorro.streams.nats.NatsJetStreamSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;

import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Runnable Spring Boot mesh agent.
 *
 * <p>Startup order:</p>
 * <ol>
 *   <li>Merge capabilities from properties + {@code HITORRO_MESH_CAPABILITIES} env var.</li>
 *   <li>Load each configured {@link com.hitorro.mesh.agent.LocalTable} from its NDJSON source.</li>
 *   <li>Open a {@link MeshTransport} — {@link NatsMeshTransport} or {@link InMemoryMeshTransport}.</li>
 *   <li>Construct {@link MeshAgent} and call {@code start()}.</li>
 * </ol>
 *
 * <p>Actuator is on the classpath — {@code /actuator/health} reports the
 * spring health, and the agent will show as healthy while its heartbeat
 * thread is alive. The driver's separate {@code LiveAgentRegistry} decides
 * whether it's mesh-visible.</p>
 */
@SpringBootApplication
@EnableConfigurationProperties(AgentProperties.class)
public class MeshAgentApplication implements DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(MeshAgentApplication.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private MeshAgent agent;
    private MeshTransport transport;

    public static void main(String[] args) {
        SpringApplication.run(MeshAgentApplication.class, args);
    }

    @Bean
    public MeshTransport meshTransport(AgentProperties props) {
        return switch (props.getTransport()) {
            case NATS -> {
                com.hitorro.mesh.nats.NatsSecurity sec = props.getNatsSecurity() == null
                        ? com.hitorro.mesh.nats.NatsSecurity.none()
                        : props.getNatsSecurity().toSecurity();
                log.info("mesh: opening NATS transport to {} (tls={}, auth={})",
                        props.getNatsUrl(),
                        sec.requiresTls() || props.getNatsUrl().startsWith("tls://"),
                        describeAuth(sec));
                yield NatsMeshTransport.openUrl(props.getNatsUrl(), sec);
            }
            case INMEMORY -> {
                log.info("mesh: using in-memory transport (single-JVM mode)");
                yield new InMemoryMeshTransport();
            }
        };
    }

    private static String describeAuth(com.hitorro.mesh.nats.NatsSecurity sec) {
        if (sec.credentialsFile() != null && !sec.credentialsFile().isBlank()) return "creds-file";
        if (sec.token() != null && !sec.token().isBlank()) return "token";
        if (sec.username() != null && !sec.username().isBlank()) return "user/pass";
        return "none";
    }

    @Bean
    public MeshAgent meshAgent(MeshTransport transport,
                               AgentProperties props,
                               ResourceLoader loader,
                               Environment env) throws Exception {
        if (props.getId() == null || props.getId().isBlank()) {
            throw new IllegalStateException("hitorro.mesh.agent.id is required");
        }
        this.transport = transport;

        Set<String> caps = mergeCapabilities(props, env);
        List<LocalTable> localTables = loadTables(props.getTables(), loader);
        List<LocalTable> broadcastTables = loadTables(props.getBroadcastTables(), loader);

        AgentConfig cfg = new AgentConfig(
                props.getId(),
                caps,
                props.getHeartbeatInterval(),
                localTables,
                broadcastTables);

        MeshAgent a = new MeshAgent(transport, cfg);
        a.start();
        this.agent = a;

        log.info("mesh: agent {} online, capabilities={}, partitioned-tables={}, broadcast-tables={}",
                cfg.agentId(), cfg.capabilities(),
                summarize(localTables), summarize(broadcastTables));
        return a;
    }

    private Set<String> mergeCapabilities(AgentProperties props, Environment env) {
        Set<String> merged = new LinkedHashSet<>(props.getCapabilities());
        String envCaps = env.getProperty("HITORRO_MESH_CAPABILITIES");
        if (envCaps != null && !envCaps.isBlank()) {
            for (String s : envCaps.split(",")) {
                String t = s.trim();
                if (!t.isEmpty()) merged.add(t);
            }
        }
        if (merged.isEmpty()) {
            throw new IllegalStateException(
                    "no capabilities configured — set hitorro.mesh.agent.capabilities or HITORRO_MESH_CAPABILITIES env");
        }
        return merged;
    }

    private List<LocalTable> loadTables(List<AgentProperties.TableConfig> configs, ResourceLoader loader) throws Exception {
        List<LocalTable> out = new ArrayList<>();
        for (AgentProperties.TableConfig tc : configs) {
            if (tc.getName() == null || tc.getTypeJsonResource() == null) {
                throw new IllegalStateException(
                        "table config incomplete — name and type-json-resource required: " + tc.getName());
            }
            int sourceCount = (tc.getNdjsonFile() != null ? 1 : 0)
                    + (tc.getKafka() != null ? 1 : 0)
                    + (tc.getNats() != null ? 1 : 0);
            if (sourceCount != 1) {
                throw new IllegalStateException(
                        "table " + tc.getName() + ": exactly one of ndjson-file, kafka, nats required (got " + sourceCount + ")");
            }
            Type type = new Type();
            Resource typeRes = loader.getResource(tc.getTypeJsonResource());
            try (var in = typeRes.getInputStream()) {
                type.init(MAPPER.readTree(in));
            }
            LocalTable table;
            if (tc.getNdjsonFile() != null) {
                NdjsonLocalTable t = new NdjsonLocalTable(
                        tc.getName(), type, tc.getPartitionKey(), URI.create(tc.getNdjsonFile()));
                log.info("mesh: loaded table {} partition={} source=ndjson rows={}",
                        tc.getName(), tc.getPartitionKey(), t.rowCount());
                table = t;
            } else if (tc.getKafka() != null) {
                table = buildKafkaTable(tc, type);
                log.info("mesh: opened table {} partition={} source=kafka topic={}",
                        tc.getName(), tc.getPartitionKey(), tc.getKafka().getTopic());
            } else {
                table = buildNatsTable(tc, type);
                log.info("mesh: opened table {} partition={} source=nats stream={} subject={}",
                        tc.getName(), tc.getPartitionKey(),
                        tc.getNats().getStream(), tc.getNats().getSubject());
            }
            out.add(table);
        }
        return out;
    }

    private LocalTable buildKafkaTable(AgentProperties.TableConfig tc, Type type) {
        AgentProperties.KafkaSourceConfig k = tc.getKafka();
        require("kafka.bootstrap-servers", k.getBootstrapServers(), tc);
        require("kafka.group-id", k.getGroupId(), tc);
        require("kafka.topic", k.getTopic(), tc);
        KafkaSource.Builder b = KafkaSource.builder()
                .bootstrapServers(k.getBootstrapServers())
                .groupId(k.getGroupId())
                .topic(k.getTopic());
        if (k.getAutoOffsetReset() != null) b.autoOffsetReset(k.getAutoOffsetReset());
        if (k.getAutoCommit() != null) b.autoCommit(k.getAutoCommit());
        if (k.getMaxPollRecords() != null) b.maxPollRecords(k.getMaxPollRecords());
        if (k.getPollTimeout() != null) b.pollTimeout(k.getPollTimeout());
        return new KafkaStreamingLocalTable(tc.getName(), type, tc.getPartitionKey(), b.build());
    }

    private LocalTable buildNatsTable(AgentProperties.TableConfig tc, Type type) throws Exception {
        AgentProperties.NatsJetStreamSourceConfig n = tc.getNats();
        require("nats.url", n.getUrl(), tc);
        require("nats.stream", n.getStream(), tc);
        require("nats.subject", n.getSubject(), tc);
        require("nats.durable-name", n.getDurableName(), tc);
        NatsJetStreamSource.Builder b = NatsJetStreamSource.builder()
                .url(n.getUrl())
                .stream(n.getStream())
                .subject(n.getSubject())
                .durableName(n.getDurableName());
        if (n.getBatchSize() != null) b.batchSize(n.getBatchSize());
        if (n.getFetchTimeout() != null) b.fetchTimeout(n.getFetchTimeout());
        if (n.getConnectTimeout() != null) b.connectTimeout(n.getConnectTimeout());
        if (n.getAckWait() != null) b.ackWait(n.getAckWait());
        return new NatsJetStreamLocalTable(tc.getName(), type, tc.getPartitionKey(), b.build());
    }

    private static void require(String field, Object value, AgentProperties.TableConfig tc) {
        if (value == null || (value instanceof String s && s.isBlank())) {
            throw new IllegalStateException("table " + tc.getName() + ": " + field + " required");
        }
    }

    private String summarize(List<LocalTable> ts) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < ts.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(ts.get(i).name()).append(":").append(ts.get(i).partitionKey());
        }
        return sb.append("]").toString();
    }

    @Override
    public void destroy() {
        if (agent != null) agent.close();
        if (transport != null) transport.close();
    }
}
