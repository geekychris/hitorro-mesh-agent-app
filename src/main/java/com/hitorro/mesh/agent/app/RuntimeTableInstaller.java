/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.agent.app;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hitorro.jsontypesystem.Type;
import com.hitorro.mesh.Codecs;
import com.hitorro.mesh.MeshTransport;
import com.hitorro.mesh.RegisterTableMessage;
import com.hitorro.mesh.Subjects;
import com.hitorro.mesh.UnregisterTableMessage;
import com.hitorro.mesh.agent.LocalTable;
import com.hitorro.mesh.agent.MeshAgent;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.net.URI;

/**
 * Subscribes to the mesh control channel and hot-installs tables the
 * driver announces via {@link RegisterTableMessage}. Sits in the
 * agent-app module because building a concrete {@link LocalTable}
 * needs {@code NdjsonLocalTable} / {@code ParquetLocalTable} which
 * live here (and drag along Jackson / basefile / parquet-avro).
 *
 * <p>The {@link com.hitorro.mesh.agent.MeshAgent} in the core module
 * just holds the shared {@link com.hitorro.mesh.agent.RuntimeTableRegistry}
 * — this installer subscribes on that agent's transport and mutates the
 * registry. {@code TaskExecutor}'s lookup already consults the registry
 * first, so a newly-installed table becomes queryable on the next
 * incoming task.</p>
 *
 * <p>Idempotent: re-registering the same {@code (name, partitionKey)}
 * atomically replaces the previous entry.</p>
 */
@Component
public class RuntimeTableInstaller {

    private static final Logger log = LoggerFactory.getLogger(RuntimeTableInstaller.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final MeshAgent agent;
    private final RuntimeTableJournal journal;
    private MeshTransport.Subscription sub;
    private MeshTransport.Subscription unregisterSub;

    public RuntimeTableInstaller(MeshAgent agent) {
        this.agent = agent;
        this.journal = new RuntimeTableJournal(agent.agentId());
    }

    @PostConstruct
    public void start() {
        // Replay the journal so runtime-registered tables survive restarts.
        // installOne() below records "did-load" instead of appending — the
        // journal is already the source of truth for these entries.
        int replayed = 0, replayFailed = 0;
        for (RegisterTableMessage m : journal.loadActive()) {
            if (installOne(m, /*persist=*/false)) replayed++;
            else replayFailed++;
        }
        if (replayed + replayFailed > 0) {
            log.info("mesh: agent {} replayed {} runtime table(s) from journal ({} failed)",
                    agent.agentId(), replayed, replayFailed);
        }
        // Boot-time compaction — collapses accumulated tombstones +
        // replacements down to the current active set. Cheap: happens
        // once per boot on a small file.
        journal.compact();

        sub = agent.transport().subscribe(Subjects.agentControlRegisterTable(), this::handle);
        unregisterSub = agent.transport().subscribe(
                Subjects.agentControlUnregisterTable(), this::handleUnregister);
        log.info("mesh: agent {} subscribed to {} + {} for runtime table registration",
                agent.agentId(),
                Subjects.agentControlRegisterTable(),
                Subjects.agentControlUnregisterTable());
    }

    @PreDestroy
    public void stop() {
        if (sub != null) { sub.close(); sub = null; }
        if (unregisterSub != null) { unregisterSub.close(); unregisterSub = null; }
    }

    private void handleUnregister(byte[] bytes) {
        UnregisterTableMessage msg;
        try {
            msg = Codecs.decode(bytes, UnregisterTableMessage.class);
        } catch (Exception e) {
            log.warn("mesh: unregister-table decode failed: {}", e.toString());
            return;
        }
        // Broadcast tables live under BOTH pk=null and pk="broadcast" —
        // remove both slots when we don't know which the original was.
        agent.runtimeTables().unregister(msg.name(), msg.partitionKey());
        if (msg.partitionKey() == null) {
            agent.runtimeTables().unregister(msg.name(), "broadcast");
        }
        // Drop the corresponding partition capability so the driver
        // stops routing partition-scoped queries here.
        if (msg.partitionKey() != null && !msg.partitionKey().isBlank()) {
            agent.removeRuntimeCapability("partition:" + msg.name() + ":" + msg.partitionKey());
        }
        journal.appendUnregister(msg.name(), msg.partitionKey());
        log.info("mesh: agent {} unregistered runtime table {} (partition={})",
                agent.agentId(), msg.name(), msg.partitionKey());
    }

    private void handle(byte[] bytes) {
        RegisterTableMessage msg;
        try {
            msg = Codecs.decode(bytes, RegisterTableMessage.class);
        } catch (Exception e) {
            log.warn("mesh: control message decode failed: {}", e.toString());
            return;
        }
        // Per-agent routing filter: if targetAgentId is set and doesn't
        // match this agent, silently skip. Every other agent that DOES
        // match will install; driver's PartitionPlacement decides who
        // gets what.
        if (msg.targetAgentId() != null && !msg.targetAgentId().isBlank()
                && !msg.targetAgentId().equals(agent.agentId())) {
            return;
        }
        installOne(msg, /*persist=*/true);
    }

    /** Install one message into the runtime registry.
     *  @param persist true when the source is a live NATS message and we
     *                 should durably record it; false during boot replay. */
    private boolean installOne(RegisterTableMessage msg, boolean persist) {
        try {
            // Broadcast tables need DUAL registration (mirrors dataset pattern):
            //   - pk=null   → engineWithBroadcasts iterates for JOIN
            //   - pk="broadcast" → requireLocalTable resolves SELECT * FROM x
            // Distributed tables get single registration under their pk.
            LocalTable base = build(msg, msg.broadcast() ? null : msg.partitionKey());
            agent.runtimeTables().register(base);
            if (msg.broadcast()) {
                agent.runtimeTables().register(rewrapWithKey(base, "broadcast"));
            }
            if (persist) journal.appendRegister(msg);
            // Advertise the partition capability so the driver's dispatcher
            // can route partition-scoped queries here. Broadcast tables
            // are handled by the [jvssql] capability already; only
            // distributed tables need the per-partition marker.
            if (!msg.broadcast() && msg.partitionKey() != null && !msg.partitionKey().isBlank()) {
                agent.addRuntimeCapability("partition:" + msg.name() + ":" + msg.partitionKey());
            }
            log.info("mesh: agent {} {} runtime table {} (broadcast={}, format={}, uri={})",
                    agent.agentId(), persist ? "registered" : "replayed",
                    msg.name(), msg.broadcast(), msg.format(), msg.uri());
            return true;
        } catch (Exception e) {
            // Unwrap the reflection wrapper so the real cause is visible.
            Throwable cause = e;
            while (cause instanceof java.lang.reflect.InvocationTargetException ite
                    && ite.getCause() != null) {
                cause = ite.getCause();
            }
            log.warn("mesh: agent {} failed to register runtime table {}: {}: {}",
                    agent.agentId(), msg.name(),
                    cause.getClass().getSimpleName(), cause.getMessage(), cause);
            return false;
        }
    }

    /** Thin adapter — same rows/type as {@code src}, different partition key.
     *  Lets us install the same underlying data under two keys without
     *  loading the source file twice. */
    private static LocalTable rewrapWithKey(LocalTable src, String newKey) {
        return new LocalTable() {
            @Override public String name()               { return src.name(); }
            @Override public Type type()                 { return src.type(); }
            @Override public String partitionKey()       { return newKey; }
            @Override public java.util.Iterator<com.hitorro.jsontypesystem.JVS> openScan() {
                return src.openScan();
            }
            @Override public com.hitorro.jvssql.config.StreamConfig streamConfig() {
                return src.streamConfig();
            }
        };
    }

    private LocalTable build(RegisterTableMessage msg, String pk) throws Exception {
        Type type = new Type();
        type.init(MAPPER.readTree(msg.typeJson()));
        String fmt = msg.format() == null ? "ndjson" : msg.format().toLowerCase();
        return switch (fmt) {
            case "ndjson" -> new NdjsonLocalTable(msg.name(), type, pk, URI.create(msg.uri()));
            case "parquet" -> {
                try {
                    // Reflection so this class doesn't drag hadoop-* at compile time —
                    // ParquetLocalTable lives with the parquet reader deps.
                    Class<?> cls = Class.forName(
                            "com.hitorro.mesh.agent.app.ParquetLocalTable");
                    yield (LocalTable) cls.getConstructor(
                            String.class, Type.class, String.class, URI.class)
                        .newInstance(msg.name(), type, pk, URI.create(msg.uri()));
                } catch (ClassNotFoundException e) {
                    throw new IllegalStateException(
                            "parquet runtime tables require ParquetLocalTable on the classpath");
                }
            }
            case "kafka" -> buildKafka(msg, type, pk);
            case "nats"  -> buildNats(msg, type, pk);
            default -> throw new IllegalArgumentException(
                    "unknown table format: " + fmt + " (supported: ndjson, parquet, kafka, nats)");
        };
    }

    /** Build a KafkaStreamingLocalTable from the message's sourceConfig.
     *  Required keys: bootstrap-servers, group-id, topic. Optional:
     *  auto-offset-reset, auto-commit, max-poll-records, poll-timeout. */
    private static LocalTable buildKafka(RegisterTableMessage msg, Type type, String pk) {
        java.util.Map<String, String> cfg = msg.sourceConfig();
        String bootstrap = required(cfg, "bootstrap-servers");
        String groupId   = required(cfg, "group-id");
        String topic     = required(cfg, "topic");
        com.hitorro.streams.kafka.KafkaSource.Builder b =
                com.hitorro.streams.kafka.KafkaSource.builder()
                        .bootstrapServers(bootstrap)
                        .groupId(groupId)
                        .topic(topic);
        if (cfg.get("auto-offset-reset") != null) b.autoOffsetReset(cfg.get("auto-offset-reset"));
        if (cfg.get("auto-commit") != null) b.autoCommit(Boolean.parseBoolean(cfg.get("auto-commit")));
        if (cfg.get("max-poll-records") != null) b.maxPollRecords(Integer.parseInt(cfg.get("max-poll-records")));
        if (cfg.get("poll-timeout") != null) b.pollTimeout(java.time.Duration.parse(cfg.get("poll-timeout")));
        return new com.hitorro.mesh.streaming.kafka.KafkaStreamingLocalTable(
                msg.name(), type, pk, b.build());
    }

    /** Build a NatsJetStreamLocalTable from the message's sourceConfig.
     *  Required keys: url, stream, subject. Optional: durable-name,
     *  batch-size, fetch-timeout, connect-timeout, ack-wait. */
    private static LocalTable buildNats(RegisterTableMessage msg, Type type, String pk) throws Exception {
        java.util.Map<String, String> cfg = msg.sourceConfig();
        String url     = required(cfg, "url");
        String stream  = required(cfg, "stream");
        String subject = required(cfg, "subject");
        com.hitorro.streams.nats.NatsJetStreamSource.Builder b =
                com.hitorro.streams.nats.NatsJetStreamSource.builder()
                        .url(url)
                        .stream(stream)
                        .subject(subject);
        if (cfg.get("durable-name")    != null) b.durableName(cfg.get("durable-name"));
        if (cfg.get("batch-size")      != null) b.batchSize(Integer.parseInt(cfg.get("batch-size")));
        if (cfg.get("fetch-timeout")   != null) b.fetchTimeout(java.time.Duration.parse(cfg.get("fetch-timeout")));
        if (cfg.get("connect-timeout") != null) b.connectTimeout(java.time.Duration.parse(cfg.get("connect-timeout")));
        if (cfg.get("ack-wait")        != null) b.ackWait(java.time.Duration.parse(cfg.get("ack-wait")));
        return new com.hitorro.mesh.streaming.nats.NatsJetStreamLocalTable(
                msg.name(), type, pk, b.build());
    }

    private static String required(java.util.Map<String, String> m, String key) {
        String v = m == null ? null : m.get(key);
        if (v == null || v.isBlank()) {
            throw new IllegalArgumentException("streaming sourceConfig missing required key: " + key);
        }
        return v;
    }
}
