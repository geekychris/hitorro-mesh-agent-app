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
    private MeshTransport.Subscription sub;

    public RuntimeTableInstaller(MeshAgent agent) {
        this.agent = agent;
    }

    @PostConstruct
    public void start() {
        sub = agent.transport().subscribe(Subjects.allAgentControl(), this::handle);
        log.info("mesh: agent {} subscribed to {} for runtime table registration",
                agent.agentId(), Subjects.allAgentControl());
    }

    @PreDestroy
    public void stop() {
        if (sub != null) { sub.close(); sub = null; }
    }

    private void handle(byte[] bytes) {
        RegisterTableMessage msg;
        try {
            msg = Codecs.decode(bytes, RegisterTableMessage.class);
        } catch (Exception e) {
            log.warn("mesh: control message decode failed: {}", e.toString());
            return;
        }
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
            log.info("mesh: agent {} registered runtime table {} (broadcast={}, format={}, uri={})",
                    agent.agentId(), msg.name(), msg.broadcast(), msg.format(), msg.uri());
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
            default -> throw new IllegalArgumentException(
                    "unknown table format: " + fmt + " (supported: ndjson, parquet)");
        };
    }
}
