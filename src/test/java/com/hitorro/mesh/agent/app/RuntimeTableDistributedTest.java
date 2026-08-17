/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.agent.app;

import com.hitorro.mesh.Codecs;
import com.hitorro.mesh.InMemoryMeshTransport;
import com.hitorro.mesh.RegisterTableMessage;
import com.hitorro.mesh.Subjects;
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
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Focused tests for the distributed runtime table path
 * (broadcast=false + partitionKey). Distinct from the broadcast dual-
 * install pattern covered elsewhere.
 */
class RuntimeTableDistributedTest {

    private static Path writeNdjson(Path dir, String name) throws IOException {
        Path f = dir.resolve(name + ".ndjson");
        Files.writeString(f, """
                {"region":"na","country":"USA","n":1}
                {"region":"eu","country":"DEU","n":2}
                """, StandardCharsets.UTF_8);
        return f;
    }

    private static MeshAgent bootAgent(String id, com.hitorro.mesh.MeshTransport transport, Path journalHome) throws Exception {
        AgentConfig cfg = new AgentConfig(id, Set.of("jvssql"),
                Duration.ofSeconds(1), List.of(), List.of());
        MeshAgent agent = new MeshAgent(transport, cfg);
        agent.start();
        System.setProperty("hitorro.agent.home", journalHome.toString());
        RuntimeTableInstaller installer = new RuntimeTableInstaller(agent);
        installer.start();
        return agent;
    }

    @Test
    void distributedRegister_installsUnderPartitionKeyOnly(@TempDir Path tmp) throws Exception {
        InMemoryMeshTransport transport = new InMemoryMeshTransport();
        MeshAgent agent = bootAgent("agent-x", transport, tmp.resolve("agent-home"));
        Path ndjson = writeNdjson(tmp, "shards");

        // broadcast=false, partitionKey="all"
        RegisterTableMessage msg = new RegisterTableMessage(
                "shards",
                "{\"name\":\"shards\",\"fields\":[{\"name\":\"region\",\"type\":\"core_string\"}]}",
                ndjson.toUri().toString(),
                "ndjson",
                /*broadcast=*/false,
                "all");
        transport.publish(Subjects.agentControlRegisterTable(), Codecs.encode(msg));

        // Only the "all" slot should be populated — no dual-install for
        // distributed tables.
        LocalTable byAll   = agent.runtimeTables().find("shards", "all");
        LocalTable byNull  = agent.runtimeTables().find("shards", null);
        LocalTable byBcast = agent.runtimeTables().find("shards", "broadcast");
        assertThat(byAll).isNotNull();
        assertThat(byNull).isNull();
        assertThat(byBcast).isNull();
        assertThat(agent.runtimeTables().size()).isEqualTo(1);

        agent.close();
        transport.close();
    }

    @Test
    void distributedRegister_missingPartitionKey_isRejectedAtMessageConstruction() {
        // Message record validates in its compact constructor —
        // broadcast=false + null partitionKey → IllegalArgumentException
        // before anything hits NATS.
        org.junit.jupiter.api.Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> new RegisterTableMessage(
                        "t",
                        "{\"name\":\"t\",\"fields\":[]}",
                        "file:/x.ndjson",
                        "ndjson",
                        /*broadcast=*/false,
                        /*partitionKey=*/null));
    }

    @Test
    void distributedUnregister_dropsTheSinglePartitionSlot(@TempDir Path tmp) throws Exception {
        InMemoryMeshTransport transport = new InMemoryMeshTransport();
        MeshAgent agent = bootAgent("agent-x", transport, tmp.resolve("agent-home"));
        Path ndjson = writeNdjson(tmp, "shards");

        transport.publish(Subjects.agentControlRegisterTable(), Codecs.encode(
                new RegisterTableMessage("shards",
                        "{\"name\":\"shards\",\"fields\":[]}",
                        ndjson.toUri().toString(), "ndjson", false, "all")));
        assertThat(agent.runtimeTables().size()).isEqualTo(1);

        // Unregister with the same partitionKey — should clean up.
        transport.publish(Subjects.agentControlUnregisterTable(), Codecs.encode(
                new com.hitorro.mesh.UnregisterTableMessage("shards", "all")));
        assertThat(agent.runtimeTables().size()).isZero();

        agent.close();
        transport.close();
    }
}
