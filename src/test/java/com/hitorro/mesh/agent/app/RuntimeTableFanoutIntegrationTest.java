/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.agent.app;

import com.hitorro.jsontypesystem.JVS;
import com.hitorro.mesh.Codecs;
import com.hitorro.mesh.InMemoryMeshTransport;
import com.hitorro.mesh.MeshTransport;
import com.hitorro.mesh.RegisterTableMessage;
import com.hitorro.mesh.Subjects;
import com.hitorro.mesh.TableInventoryReply;
import com.hitorro.mesh.TableInventoryRequest;
import com.hitorro.mesh.UnregisterTableMessage;
import com.hitorro.mesh.agent.AgentConfig;
import com.hitorro.mesh.agent.LocalTable;
import com.hitorro.mesh.agent.MeshAgent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test of the runtime table fan-out using
 * {@link InMemoryMeshTransport} — no NATS, no Spring, no filesystem
 * MinIO. Exercises the same subjects, message classes, and installer
 * paths a real deployment uses.
 *
 * <p>Setup: shared transport, one MeshAgent, one manually-driven
 * {@link RuntimeTableInstaller} bound to that agent. The test itself
 * plays the "driver" role — publishing register / unregister /
 * inventory-request messages and asserting outcomes.</p>
 */
class RuntimeTableFanoutIntegrationTest {

    /** Small NDJSON dataset written under @TempDir so agents can actually
     *  load the file into memory when they build an NdjsonLocalTable. */
    private static Path writeNdjson(Path dir, String name) throws IOException {
        Path f = dir.resolve(name + ".ndjson");
        Files.writeString(f, """
                {"k":"a","n":1}
                {"k":"b","n":2}
                {"k":"c","n":3}
                """, StandardCharsets.UTF_8);
        return f;
    }

    /** Boots one MeshAgent + its installers on the shared transport. */
    private static MeshAgent bootAgent(String id, MeshTransport transport, Path journalHome) throws Exception {
        AgentConfig cfg = new AgentConfig(id, Set.of("jvssql"),
                Duration.ofSeconds(1), List.of(), List.of());
        MeshAgent agent = new MeshAgent(transport, cfg);
        agent.start();

        // Manually wire an installer that persists to the temp journal dir.
        // Skip @PostConstruct — we're not in Spring here.
        // Set the journal home so tests don't pollute the operator's real home.
        System.setProperty("hitorro.agent.home", journalHome.toString());
        RuntimeTableInstaller installer = new RuntimeTableInstaller(agent);
        installer.start();
        return agent;
    }

    @Test
    void registerFanout_agentInstallsTable_isQueryable(@TempDir Path tmp) throws Exception {
        InMemoryMeshTransport transport = new InMemoryMeshTransport();
        MeshAgent agent = bootAgent("agent-1", transport, tmp.resolve("agent-home"));
        Path ndjson = writeNdjson(tmp, "widgets");

        // Publish RegisterTableMessage — installer picks it up synchronously
        // (InMemoryMeshTransport dispatches on the publishing thread).
        RegisterTableMessage msg = new RegisterTableMessage(
                "widgets",
                "{\"name\":\"widgets\",\"fields\":[{\"name\":\"k\",\"type\":\"core_string\"},{\"name\":\"n\",\"type\":\"core_long\"}]}",
                ndjson.toUri().toString(),
                "ndjson", true, null);
        transport.publish(Subjects.agentControlRegisterTable(), Codecs.encode(msg));

        // Broadcast tables get DUAL registration (pk=null + pk="broadcast").
        LocalTable byName = agent.runtimeTables().find("widgets", null);
        LocalTable byPk   = agent.runtimeTables().find("widgets", "broadcast");
        assertThat(byName).isNotNull();
        assertThat(byPk).isNotNull();

        // Actually iterate the scan — proves the file was read + parsed.
        Iterator<JVS> rows = byName.openScan();
        int count = 0;
        while (rows.hasNext()) { rows.next(); count++; }
        assertThat(count).isEqualTo(3);

        agent.close();
        transport.close();
    }

    @Test
    void unregisterFanout_dropsBothPartitionSlots(@TempDir Path tmp) throws Exception {
        InMemoryMeshTransport transport = new InMemoryMeshTransport();
        MeshAgent agent = bootAgent("agent-1", transport, tmp.resolve("agent-home"));
        Path ndjson = writeNdjson(tmp, "gadgets");

        // Install
        transport.publish(Subjects.agentControlRegisterTable(), Codecs.encode(
                new RegisterTableMessage("gadgets",
                        "{\"name\":\"gadgets\",\"fields\":[]}",
                        ndjson.toUri().toString(), "ndjson", true, null)));
        assertThat(agent.runtimeTables().size()).isEqualTo(2);   // dual install

        // Unregister — both slots must go.
        transport.publish(Subjects.agentControlUnregisterTable(), Codecs.encode(
                new UnregisterTableMessage("gadgets", null)));
        assertThat(agent.runtimeTables().size()).isZero();
        assertThat(agent.runtimeTables().find("gadgets", null)).isNull();
        assertThat(agent.runtimeTables().find("gadgets", "broadcast")).isNull();

        agent.close();
        transport.close();
    }

    @Test
    void inventoryProbe_returnsInstalledTables(@TempDir Path tmp) throws Exception {
        InMemoryMeshTransport transport = new InMemoryMeshTransport();
        MeshAgent agent = bootAgent("agent-1", transport, tmp.resolve("agent-home"));
        Path ndjson = writeNdjson(tmp, "sprockets");

        // Boot the responder alongside the installer.
        TableInventoryResponder responder = new TableInventoryResponder(agent);
        responder.start();

        // Install a table + register.
        transport.publish(Subjects.agentControlRegisterTable(), Codecs.encode(
                new RegisterTableMessage("sprockets",
                        "{\"name\":\"sprockets\",\"fields\":[]}",
                        ndjson.toUri().toString(), "ndjson", true, null)));

        // Fire a probe and capture the reply.
        AtomicReference<TableInventoryReply> reply = new AtomicReference<>();
        String replyId = "test-probe";
        MeshTransport.Subscription sub = transport.subscribe(
                Subjects.allAgentInventoryReplies(replyId),
                bytes -> reply.set(Codecs.decode(bytes, TableInventoryReply.class)));

        transport.publish(Subjects.agentControlInventoryRequest(),
                Codecs.encode(new TableInventoryRequest(replyId)));

        assertThat(reply.get()).isNotNull();
        assertThat(reply.get().agentId()).isEqualTo("agent-1");
        // Runtime install shows up (dual-install → 2 entries).
        assertThat(reply.get().tables())
                .extracting(TableInventoryReply.Entry::name)
                .contains("sprockets");
        assertThat(reply.get().tables())
                .filteredOn(e -> "sprockets".equals(e.name()))
                .extracting(TableInventoryReply.Entry::source)
                .containsOnly("runtime");

        sub.close();
        responder.stop();
        agent.close();
        transport.close();
    }

    @Test
    void reRegister_replacesInstalledEntry(@TempDir Path tmp) throws Exception {
        InMemoryMeshTransport transport = new InMemoryMeshTransport();
        MeshAgent agent = bootAgent("agent-1", transport, tmp.resolve("agent-home"));
        Path v1 = writeNdjson(tmp, "v1");
        Path v2 = writeNdjson(tmp, "v2");

        transport.publish(Subjects.agentControlRegisterTable(), Codecs.encode(
                new RegisterTableMessage("v",
                        "{\"name\":\"v\",\"fields\":[]}",
                        v1.toUri().toString(), "ndjson", true, null)));
        assertThat(agent.runtimeTables().find("v", null).openScan().hasNext()).isTrue();

        // Re-register same name, different URI — should replace, not duplicate.
        transport.publish(Subjects.agentControlRegisterTable(), Codecs.encode(
                new RegisterTableMessage("v",
                        "{\"name\":\"v\",\"fields\":[]}",
                        v2.toUri().toString(), "ndjson", true, null)));
        assertThat(agent.runtimeTables().size()).isEqualTo(2);   // still just the dual-install

        agent.close();
        transport.close();
    }
}
