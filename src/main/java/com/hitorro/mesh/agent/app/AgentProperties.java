/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.mesh.agent.app;

import com.hitorro.mesh.nats.NatsSecurityProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Spring Boot properties for a mesh agent.
 *
 * <pre>
 * hitorro:
 *   mesh:
 *     agent:
 *       id: mac02                            # required — unique across the mesh
 *       transport: nats                      # nats | inmemory
 *       nats-url: nats://mesh.local:4222     # required if transport=nats
 *       heartbeat-interval: 2s
 *       capabilities:
 *         - jvssql
 *         - partition:docs:shard-3
 *       tables:                              # local tables served by this agent
 *         - name: docs                       # NDJSON-backed (batch)
 *           partition-key: shard-3
 *           type-json-resource: classpath:types/docs.json
 *           ndjson-file: file:///data/docs/shard-3.ndjson
 *         - name: events                     # Kafka-backed (streaming) — phase 6b
 *           partition-key: shard-3
 *           type-json-resource: file:/config/events-type.json
 *           kafka:
 *             bootstrap-servers: kafka-broker:9092
 *             group-id: mesh-agent-shard-3
 *             topic: events
 *             auto-offset-reset: latest
 *         - name: metrics                    # NATS JetStream-backed (streaming) — phase 6b
 *           partition-key: shard-3
 *           type-json-resource: file:/config/metrics-type.json
 *           nats:
 *             url: nats://nats.mesh:4222
 *             stream: METRICS
 *             subject: metrics.shard-3.&gt;
 *             durable-name: mesh-agent-shard-3
 * </pre>
 *
 * <p>Exactly one of {@code ndjson-file}, {@code kafka}, or {@code nats} must be
 * set per table entry — the presence of the nested block picks the source.</p>
 *
 * <p>Capabilities may also be sourced from the {@code HITORRO_MESH_CAPABILITIES}
 * environment variable (comma-separated) so the same JAR works cleanly under
 * both Orion (env-based capability declaration) and K8s (annotation → env
 * via Downward API).</p>
 */
@ConfigurationProperties(prefix = "hitorro.mesh.agent")
public class AgentProperties {

    private String id;
    private Transport transport = Transport.NATS;
    private String natsUrl = "nats://localhost:4222";
    /**
     * TLS + authentication config for the NATS transport. Empty by default —
     * agents connect plaintext with no auth (dev + single-tenant deployments).
     * Populate {@code nats-security.*} in YAML to enable TLS, .creds files,
     * token, or user/pass auth. See {@link NatsSecurityProperties}.
     */
    private NatsSecurityProperties natsSecurity = new NatsSecurityProperties();
    private Duration heartbeatInterval = Duration.ofSeconds(2);
    private List<String> capabilities = new ArrayList<>();
    private List<TableConfig> tables = new ArrayList<>();

    /**
     * Broadcast tables — small dimension tables replicated to every agent.
     * Same config shape as {@link #tables} but no {@code partitionKey}. Every
     * jvssql query at this agent can JOIN against them. Phase 4a.
     */
    private List<TableConfig> broadcastTables = new ArrayList<>();

    public enum Transport { NATS, INMEMORY }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public Transport getTransport() { return transport; }
    public void setTransport(Transport transport) { this.transport = transport; }

    public String getNatsUrl() { return natsUrl; }
    public void setNatsUrl(String natsUrl) { this.natsUrl = natsUrl; }

    public NatsSecurityProperties getNatsSecurity() { return natsSecurity; }
    public void setNatsSecurity(NatsSecurityProperties natsSecurity) { this.natsSecurity = natsSecurity; }

    public Duration getHeartbeatInterval() { return heartbeatInterval; }
    public void setHeartbeatInterval(Duration heartbeatInterval) { this.heartbeatInterval = heartbeatInterval; }

    public List<String> getCapabilities() { return capabilities; }
    public void setCapabilities(List<String> capabilities) { this.capabilities = capabilities; }

    public List<TableConfig> getTables() { return tables; }
    public void setTables(List<TableConfig> tables) { this.tables = tables; }

    public List<TableConfig> getBroadcastTables() { return broadcastTables; }
    public void setBroadcastTables(List<TableConfig> broadcastTables) { this.broadcastTables = broadcastTables; }

    /**
     * One local table entry. Every entry has {@code name} + {@code type-json-resource}
     * + optional {@code partition-key}, plus exactly one source block:
     * {@code ndjson-file} (batch), {@code kafka} (streaming), or {@code nats}
     * (streaming). The loader dispatches on whichever is set.
     */
    public static class TableConfig {
        private String name;
        private String partitionKey;
        private String typeJsonResource;
        private String ndjsonFile;
        private KafkaSourceConfig kafka;
        private NatsJetStreamSourceConfig nats;

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }

        public String getPartitionKey() { return partitionKey; }
        public void setPartitionKey(String partitionKey) { this.partitionKey = partitionKey; }

        public String getTypeJsonResource() { return typeJsonResource; }
        public void setTypeJsonResource(String typeJsonResource) { this.typeJsonResource = typeJsonResource; }

        public String getNdjsonFile() { return ndjsonFile; }
        public void setNdjsonFile(String ndjsonFile) { this.ndjsonFile = ndjsonFile; }

        public KafkaSourceConfig getKafka() { return kafka; }
        public void setKafka(KafkaSourceConfig kafka) { this.kafka = kafka; }

        public NatsJetStreamSourceConfig getNats() { return nats; }
        public void setNats(NatsJetStreamSourceConfig nats) { this.nats = nats; }
    }

    /**
     * Mirrors {@code KafkaSource.Builder}. Only {@code bootstrap-servers}, {@code group-id},
     * and {@code topic} are required — the rest have sensible defaults matching
     * the underlying builder.
     */
    public static class KafkaSourceConfig {
        private String bootstrapServers;
        private String groupId;
        private String topic;
        private String autoOffsetReset;
        private Boolean autoCommit;
        private Integer maxPollRecords;
        private Duration pollTimeout;

        public String getBootstrapServers() { return bootstrapServers; }
        public void setBootstrapServers(String s) { this.bootstrapServers = s; }
        public String getGroupId() { return groupId; }
        public void setGroupId(String s) { this.groupId = s; }
        public String getTopic() { return topic; }
        public void setTopic(String s) { this.topic = s; }
        public String getAutoOffsetReset() { return autoOffsetReset; }
        public void setAutoOffsetReset(String s) { this.autoOffsetReset = s; }
        public Boolean getAutoCommit() { return autoCommit; }
        public void setAutoCommit(Boolean b) { this.autoCommit = b; }
        public Integer getMaxPollRecords() { return maxPollRecords; }
        public void setMaxPollRecords(Integer n) { this.maxPollRecords = n; }
        public Duration getPollTimeout() { return pollTimeout; }
        public void setPollTimeout(Duration d) { this.pollTimeout = d; }
    }

    /**
     * Mirrors {@code NatsJetStreamSource.Builder}. {@code url}, {@code stream},
     * {@code subject}, and {@code durable-name} are required.
     */
    public static class NatsJetStreamSourceConfig {
        private String url;
        private String stream;
        private String subject;
        private String durableName;
        private Integer batchSize;
        private Duration fetchTimeout;
        private Duration connectTimeout;
        private Duration ackWait;

        public String getUrl() { return url; }
        public void setUrl(String s) { this.url = s; }
        public String getStream() { return stream; }
        public void setStream(String s) { this.stream = s; }
        public String getSubject() { return subject; }
        public void setSubject(String s) { this.subject = s; }
        public String getDurableName() { return durableName; }
        public void setDurableName(String s) { this.durableName = s; }
        public Integer getBatchSize() { return batchSize; }
        public void setBatchSize(Integer n) { this.batchSize = n; }
        public Duration getFetchTimeout() { return fetchTimeout; }
        public void setFetchTimeout(Duration d) { this.fetchTimeout = d; }
        public Duration getConnectTimeout() { return connectTimeout; }
        public void setConnectTimeout(Duration d) { this.connectTimeout = d; }
        public Duration getAckWait() { return ackWait; }
        public void setAckWait(Duration d) { this.ackWait = d; }
    }
}
