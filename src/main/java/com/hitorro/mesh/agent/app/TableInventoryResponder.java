/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.agent.app;

import com.hitorro.mesh.Codecs;
import com.hitorro.mesh.MeshTransport;
import com.hitorro.mesh.Subjects;
import com.hitorro.mesh.TableInventoryReply;
import com.hitorro.mesh.TableInventoryRequest;
import com.hitorro.mesh.agent.AgentConfig;
import com.hitorro.mesh.agent.LocalTable;
import com.hitorro.mesh.agent.MeshAgent;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Subscribes to {@link Subjects#agentControlInventoryRequest()} and
 * replies with a {@link TableInventoryReply} carrying this agent's
 * current table inventory — both boot-time and runtime.
 *
 * <p>The driver uses this to prove which agents actually hold a
 * runtime-registered table (vs merely being fan-out targets that
 * might have failed the install). Reply subject includes the request's
 * replyId + this agent's id so a driver-side wildcard subscription
 * fans in every response.</p>
 */
@Component
public class TableInventoryResponder {

    private static final Logger log = LoggerFactory.getLogger(TableInventoryResponder.class);

    private final MeshAgent agent;
    private MeshTransport.Subscription sub;

    public TableInventoryResponder(MeshAgent agent) {
        this.agent = agent;
    }

    @PostConstruct
    public void start() {
        sub = agent.transport().subscribe(
                Subjects.agentControlInventoryRequest(), this::handle);
        log.info("mesh: agent {} subscribed to {} for inventory requests",
                agent.agentId(), Subjects.agentControlInventoryRequest());
    }

    @PreDestroy
    public void stop() {
        if (sub != null) { sub.close(); sub = null; }
    }

    private void handle(byte[] bytes) {
        TableInventoryRequest req;
        try {
            req = Codecs.decode(bytes, TableInventoryRequest.class);
        } catch (Exception e) {
            log.warn("mesh: inventory-request decode failed: {}", e.toString());
            return;
        }
        // Merge boot + runtime, dedup by (name, partitionKey). Runtime
        // wins on collision (matches TaskExecutor's lookup order).
        Map<String, TableInventoryReply.Entry> merged = new LinkedHashMap<>();
        AgentConfig config = agent.config();
        for (LocalTable t : config.localTables()) {
            merged.put(key(t), new TableInventoryReply.Entry(
                    t.name(), t.partitionKey(), "boot", null));
        }
        for (LocalTable t : config.broadcastTables()) {
            merged.put(key(t), new TableInventoryReply.Entry(
                    t.name(), t.partitionKey(), "boot", null));
        }
        for (LocalTable t : agent.runtimeTables().localSnapshot()) {
            merged.put(key(t), new TableInventoryReply.Entry(
                    t.name(), t.partitionKey(), "runtime", null));
        }
        for (LocalTable t : agent.runtimeTables().broadcastSnapshot()) {
            merged.put(key(t), new TableInventoryReply.Entry(
                    t.name(), t.partitionKey(), "runtime", null));
        }

        TableInventoryReply reply = new TableInventoryReply(
                agent.agentId(), new ArrayList<>(merged.values()));
        agent.transport().publish(
                Subjects.agentInventoryReply(req.replyId(), agent.agentId()),
                Codecs.encode(reply));
    }

    private static String key(LocalTable t) {
        return t.partitionKey() == null ? t.name() : t.name() + "@" + t.partitionKey();
    }
}
