/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.agent.app.storage;

import com.hitorro.util.basefile.fs.BaseFileSystem;
import com.hitorro.util.basefile.fs.s3.MinioProtocolAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Agent-side twin of the driver's S3StorageAutoConfig. Registers
 * {@link MinioProtocolAdapter} with {@link BaseFileSystem} at agent
 * startup so table configs with {@code s3://…} URIs load their NDJson
 * from MinIO instead of the local disk.
 *
 * <p>Activated by presence of {@code hitorro.storage.s3.endpoint} —
 * zero-config when absent. See the driver-app variant for the full
 * property surface.</p>
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "hitorro.storage.s3", name = "endpoint")
public class S3StorageAutoConfig {

    private static final Logger log = LoggerFactory.getLogger(S3StorageAutoConfig.class);

    @Bean
    public MinioProtocolAdapter s3ProtocolAdapter(
            @Value("${hitorro.storage.s3.endpoint}") String endpoint,
            @Value("${hitorro.storage.s3.bucket:hitorro}") String bucket,
            @Value("${hitorro.storage.s3.access-key:}") String accessKey,
            @Value("${hitorro.storage.s3.secret-key:}") String secretKey,
            @Value("${hitorro.storage.s3.ssl:false}") boolean sslEnabled) {

        MinioProtocolAdapter adapter = new MinioProtocolAdapter(
                endpoint, bucket, accessKey, secretKey, sslEnabled);
        BaseFileSystem.addProtocolAdapter(adapter);
        log.info("agent storage: s3:// routed to {} (bucket={}, ssl={})",
                endpoint, bucket, sslEnabled);
        return adapter;
    }
}
