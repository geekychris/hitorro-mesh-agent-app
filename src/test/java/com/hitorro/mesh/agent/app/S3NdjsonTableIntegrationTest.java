/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.mesh.agent.app;

import com.hitorro.jsontypesystem.JVS;
import com.hitorro.jsontypesystem.Type;
import com.hitorro.mesh.agent.app.storage.S3StorageAutoConfig;
import com.hitorro.util.json.JsonMappers;
import com.hitorro.util.json.JsonValues;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.core.checksums.ResponseChecksumValidation;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * An NDJSON table whose source is an {@code s3://} URI, read through HiTorro's own S3 file system
 * (not S3A) from SeaweedFS, as a deployment with its tables in MinIO does. This is the path that
 * failed on main with {@code NoClassDefFoundError: org/apache/log4j/Priority} (hitorro-core's
 * logger uses the log4j 1 API; the pom now adds log4j-over-slf4j), and it checks the rows' JSON.
 */
@Testcontainers(disabledWithoutDocker = true)
class S3NdjsonTableIntegrationTest {

    static final String BUCKET = "agent-ndjson-test";
    static final String KEY = "test-access";
    static final String SECRET = "test-secret";
    static final String OBJECT = "tables/docs.ndjson";
    static final String NDJSON = "{\"id\":\"a\",\"n\":1,\"s\":\"café\",\"d\":1.5,\"o\":{\"k\":[1,null]}}\n"
            + "# comment lines are skipped\n\n"
            + "{\"id\":\"b\",\"n\":12345678901,\"s\":null}\n";

    @Container
    static final GenericContainer<?> S3 = new GenericContainer<>(DockerImageName.parse("chrislusf/seaweedfs:3.99"))
            .withCopyToContainer(Transferable.of(
                    "{\"identities\":[{\"name\":\"test\",\"credentials\":[{\"accessKey\":\"" + KEY + "\",\"secretKey\":\""
                            + SECRET + "\"}],\"actions\":[\"Admin\",\"Read\",\"List\",\"Tagging\",\"Write\"]}]}"),
                    "/etc/seaweedfs/s3.json")
            .withCommand("server", "-s3", "-s3.config=/etc/seaweedfs/s3.json", "-dir=/data", "-volume.max=8")
            .withExposedPorts(8333)
            .waitingFor(Wait.forListeningPorts(8333).withStartupTimeout(Duration.ofMinutes(2)));

    static String endpoint() {
        return "http://" + S3.getHost() + ":" + S3.getMappedPort(8333);
    }

    @BeforeAll
    static void upload() throws Exception {
        try (S3Client s3 = S3Client.builder()
                .endpointOverride(URI.create(endpoint()))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(KEY, SECRET)))
                .forcePathStyle(true)
                .requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED)
                .responseChecksumValidation(ResponseChecksumValidation.WHEN_REQUIRED)
                .build()) {
            long deadline = System.currentTimeMillis() + 60_000;
            while (true) {
                try {
                    s3.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
                    s3.putObject(PutObjectRequest.builder().bucket(BUCKET).key(OBJECT).build(),
                            RequestBody.fromString(NDJSON, StandardCharsets.UTF_8));
                    break;
                } catch (Exception e) {
                    if (System.currentTimeMillis() > deadline) throw new IllegalStateException("S3 store never became ready", e);
                    Thread.sleep(500);
                }
            }
        }
        // HiTorro's S3 client takes credentials and region from the AWS default chain.
        System.setProperty("aws.accessKeyId", KEY);
        System.setProperty("aws.secretAccessKey", SECRET);
        System.setProperty("aws.region", "us-east-1");
        // The app's own S3 wiring (hitorro.storage.s3.*): registers the s3:// protocol adapter.
        new S3StorageAutoConfig().s3ProtocolAdapter(endpoint(), BUCKET, KEY, SECRET, false);
    }

    @AfterAll
    static void cleanUp() {
        com.hitorro.mesh.agent.app.storage.S3aSettings.clear();
        System.clearProperty("aws.accessKeyId");
        System.clearProperty("aws.secretAccessKey");
        System.clearProperty("aws.region");
    }

    @Test
    void readsAnS3NdjsonTable() throws Exception {
        Type type = new Type();
        type.init(JsonMappers.json().readTree("{\"name\":\"docs\",\"fields\":[{\"name\":\"id\",\"type\":\"core_string\"},"
                + "{\"name\":\"n\",\"type\":\"core_long\"}]}"));
        NdjsonLocalTable table = new NdjsonLocalTable("docs", type, "p0", URI.create("s3://" + BUCKET + "/" + OBJECT));
        List<String> rows = new ArrayList<>();
        for (Iterator<JVS> it = table.openScan(); it.hasNext(); ) {
            rows.add(JsonMappers.json().writeValueAsString(it.next().getJsonNode()));
        }
        assertThat(rows).containsExactly(
                "{\"id\":\"a\",\"n\":1,\"s\":\"café\",\"d\":1.5,\"o\":{\"k\":[1,null]}}",
                "{\"id\":\"b\",\"n\":12345678901,\"s\":null}");
        assertThat(JsonValues.asText(JsonMappers.json().readTree(rows.get(0)).get("id"))).isEqualTo("a");
    }
}
