/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.agent.app.storage;

import org.apache.hadoop.conf.Configuration;

/**
 * The agent's S3 settings in the form Hadoop's S3A filesystem reads them.
 *
 * <p>{@code ParquetLocalTable} reads through Hadoop ({@code s3a://}), not
 * through basefile's S3 adapter, so the endpoint and credentials the agent
 * gets from {@code hitorro.storage.s3.*} (or from the driver's enable-s3
 * message) have to be copied into each Hadoop {@link Configuration} too.
 * {@link S3StorageAutoConfig} and the S3 hot-switch call {@link #configure}
 * when they install the S3 adapter; {@link #apply} copies the latest settings
 * into a configuration. Without settings, {@code apply} only turns on
 * path-style access and leaves the rest to core-site.xml / AWS defaults.</p>
 */
public final class S3aSettings {

    /** Region used when the store doesn't care (MinIO, SeaweedFS) but the AWS SDK v2 needs one. */
    static final String DEFAULT_REGION = "us-east-1";

    private record Settings(String endpoint, String accessKey, String secretKey, boolean ssl) {}

    private static volatile Settings current;

    private S3aSettings() {}

    /** Remember the S3 endpoint and credentials for later S3A reads. */
    public static void configure(String endpoint, String accessKey, String secretKey, boolean ssl) {
        current = new Settings(endpoint, accessKey, secretKey, ssl);
    }

    /** Forget the settings (tests). */
    public static void clear() {
        current = null;
    }

    /** Copy the current settings into {@code conf} as {@code fs.s3a.*} keys. */
    public static Configuration apply(Configuration conf) {
        // MinIO / SeaweedFS serve buckets by path, not by virtual host.
        conf.set("fs.s3a.path.style.access", "true");
        Settings s = current;
        if (s == null || s.endpoint() == null || s.endpoint().isBlank()) {
            return conf;
        }
        conf.set("fs.s3a.endpoint", s.endpoint());
        conf.setBoolean("fs.s3a.connection.ssl.enabled", s.ssl());
        if (conf.get("fs.s3a.endpoint.region") == null) {
            conf.set("fs.s3a.endpoint.region", DEFAULT_REGION);
        }
        if (s.accessKey() != null && !s.accessKey().isBlank()) {
            conf.set("fs.s3a.access.key", s.accessKey());
            conf.set("fs.s3a.secret.key", s.secretKey() == null ? "" : s.secretKey());
            conf.set("fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
        }
        return conf;
    }
}
