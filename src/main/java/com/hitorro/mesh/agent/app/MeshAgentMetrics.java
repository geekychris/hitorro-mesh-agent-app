/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.mesh.agent.app;

import com.hitorro.mesh.agent.MeshAgent;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

/**
 * Phase 7m — agent-side Prometheus meters, tagged with the agent's ID so
 * a single Prometheus scrape across a fleet distinguishes per-agent load.
 *
 * <h3>Exposed meters</h3>
 * <ul>
 *   <li>{@code mesh_agent_tasks_active} — gauge, currently in-flight
 *       tasks on this agent</li>
 *   <li>{@code mesh_agent_tasks_run_total} — gauge (monotonic counter
 *       source), total tasks accepted since startup</li>
 *   <li>{@code mesh_agent_rows_emitted_total} — gauge, total result rows
 *       this agent has published on the wire</li>
 *   <li>{@code mesh_agent_tasks_errored_total} — gauge, tasks that
 *       finished by publishing an ERROR ResultMessage</li>
 *   <li>{@code mesh_agent_watermarks_emitted_total} — gauge, WATERMARK
 *       heartbeats published by this agent's streaming scan tasks</li>
 * </ul>
 *
 * <p>All meters are tagged {@code agent="<agentId>"} so a Prometheus query
 * can filter/aggregate: e.g. {@code sum by (agent) (mesh_agent_rows_emitted_total)}.</p>
 *
 * <p>Standard JVM meters ({@code jvm_*}, {@code process_*},
 * {@code http_server_requests_seconds}) come for free via Spring Boot
 * Actuator + Micrometer, tagged the same way at the process level.</p>
 */
@Component
public class MeshAgentMetrics {

    public MeshAgentMetrics(MeterRegistry registry, MeshAgent agent) {
        String agentId = agent.agentId();
        registry.gauge("mesh.agent.tasks.active", java.util.List.of(
                io.micrometer.core.instrument.Tag.of("agent", agentId)), agent,
                MeshAgent::activeTaskCount);
        registry.gauge("mesh.agent.tasks.run.total", java.util.List.of(
                io.micrometer.core.instrument.Tag.of("agent", agentId)), agent,
                MeshAgent::tasksRunCount);
        registry.gauge("mesh.agent.rows.emitted.total", java.util.List.of(
                io.micrometer.core.instrument.Tag.of("agent", agentId)), agent,
                MeshAgent::rowsEmittedCount);
        registry.gauge("mesh.agent.tasks.errored.total", java.util.List.of(
                io.micrometer.core.instrument.Tag.of("agent", agentId)), agent,
                MeshAgent::tasksErroredCount);
        registry.gauge("mesh.agent.watermarks.emitted.total", java.util.List.of(
                io.micrometer.core.instrument.Tag.of("agent", agentId)), agent,
                MeshAgent::watermarksEmittedCount);
    }
}
