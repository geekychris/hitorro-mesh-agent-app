/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.agent.app;

import com.hitorro.mesh.Codecs;
import com.hitorro.mesh.EnableS3Message;
import com.hitorro.mesh.MeshTransport;
import com.hitorro.mesh.Subjects;
import com.hitorro.mesh.agent.MeshAgent;
import com.hitorro.util.basefile.fs.BaseFileSystem;
import com.hitorro.util.basefile.fs.s3.MinioProtocolAdapter;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Subscribes to {@code mesh.agent.control.enable-s3} and hot-installs a
 * MinIO / S3 protocol adapter at runtime — the agent-side counterpart of
 * the driver's {@code MinioLifecycleService}. Turns "one click on the
 * driver's Start MinIO button" into a mesh-wide S3 enablement: driver
 * wires its adapter, publishes {@link EnableS3Message}, every agent
 * receives it and wires its own.
 *
 * <p>Idempotent — {@code BaseFileSystem.addProtocolAdapter} overwrites
 * the previous {@code s3} entry. Safe to receive the message multiple
 * times; safe to receive it after S3 was already enabled at boot via
 * {@code HITORRO_STORAGE_S3_ENDPOINT}.</p>
 *
 * <p>Without this handler, agents that boot without S3 config can't
 * read {@code s3://} URIs at all — including tables the driver
 * runtime-registers via {@link com.hitorro.mesh.RegisterTableMessage}.
 * With it, the whole s3://-write-then-SELECT loop works out of the box
 * once you click Start MinIO on the driver UI.</p>
 */
@Component
public class S3AdapterInstaller {

    private static final Logger log = LoggerFactory.getLogger(S3AdapterInstaller.class);

    private final MeshAgent agent;
    private MeshTransport.Subscription sub;

    public S3AdapterInstaller(MeshAgent agent) {
        this.agent = agent;
    }

    @PostConstruct
    public void start() {
        sub = agent.transport().subscribe(Subjects.agentControlEnableS3(), this::handle);
        log.info("mesh: agent {} subscribed to {} for S3 hot-switch",
                agent.agentId(), Subjects.agentControlEnableS3());
    }

    @PreDestroy
    public void stop() {
        if (sub != null) { sub.close(); sub = null; }
    }

    private void handle(byte[] bytes) {
        EnableS3Message msg;
        try {
            msg = Codecs.decode(bytes, EnableS3Message.class);
        } catch (Exception e) {
            log.warn("mesh: enable-s3 decode failed: {}", e.toString());
            return;
        }
        try {
            MinioProtocolAdapter adapter = new MinioProtocolAdapter(
                    msg.endpoint(), msg.bucket(), msg.accessKey(), msg.secretKey(), msg.ssl());
            BaseFileSystem.addProtocolAdapter(adapter);
            log.info("mesh: agent {} enabled S3 (endpoint={}, bucket={}, ssl={})",
                    agent.agentId(), msg.endpoint(), msg.bucket(), msg.ssl());
        } catch (Exception e) {
            log.warn("mesh: agent {} failed to install S3 adapter: {}",
                    agent.agentId(), e.toString());
        }
    }
}
