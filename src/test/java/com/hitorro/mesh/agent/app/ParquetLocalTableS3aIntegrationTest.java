/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.agent.app;

import com.fasterxml.jackson.databind.JsonNode;
import com.hitorro.jsontypesystem.JVS;
import com.hitorro.mesh.agent.app.storage.S3StorageAutoConfig;
import com.hitorro.mesh.agent.app.storage.S3aSettings;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.LocalOutputFile;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reads Parquet from an S3-compatible store through Hadoop's S3A filesystem,
 * the way a runtime-registered {@code format: parquet} table does: SeaweedFS in
 * a container (MinIO's images are no longer published), a Parquet file uploaded
 * with the AWS SDK, then {@link ParquetLocalTable} on an {@code s3a://} (and an
 * {@code s3://}) URI. S3A gets its endpoint and credentials from the agent's S3
 * config, set here through {@link S3StorageAutoConfig} as the app does at boot.
 * Skipped when Docker isn't available; on Rancher Desktop set
 * {@code DOCKER_HOST=unix://$HOME/.rd/docker.sock}.
 */
@Testcontainers(disabledWithoutDocker = true)
class ParquetLocalTableS3aIntegrationTest {

    static final String BUCKET = "agent-parquet-test";
    static final String KEY = "test-access";
    static final String SECRET = "test-secret";
    static final String OBJECT = "tables/people.parquet";

    /** SeaweedFS with its S3 gateway and one identity holding KEY/SECRET. */
    @Container
    static final GenericContainer<?> S3 = new GenericContainer<>(DockerImageName.parse("chrislusf/seaweedfs:3.99"))
            .withCopyToContainer(Transferable.of(
                    "{\"identities\":[{\"name\":\"test\",\"credentials\":[{\"accessKey\":\"" + KEY + "\",\"secretKey\":\""
                            + SECRET + "\"}],\"actions\":[\"Admin\",\"Read\",\"List\",\"Tagging\",\"Write\"]}]}"),
                    "/etc/seaweedfs/s3.json")
            .withCommand("server", "-s3", "-s3.config=/etc/seaweedfs/s3.json", "-dir=/data", "-volume.max=8")
            .withExposedPorts(8333)
            .waitingFor(Wait.forListeningPorts(8333).withStartupTimeout(Duration.ofMinutes(2)));

    static final Schema SCHEMA = SchemaBuilder.record("Person").fields()
            .requiredLong("id")
            .requiredString("name")
            .requiredDouble("score")
            .requiredBoolean("active")
            .endRecord();

    static String endpoint() {
        return "http://" + S3.getHost() + ":" + S3.getMappedPort(8333);
    }

    @BeforeAll
    static void uploadParquet(@TempDir Path tmp) throws Exception {
        Path local = tmp.resolve("people.parquet");
        try (ParquetWriter<GenericRecord> w = AvroParquetWriter
                .<GenericRecord>builder(new LocalOutputFile(local))
                .withSchema(SCHEMA)
                .build()) {
            w.write(person(1, "ada", 9.5, true));
            w.write(person(2, "brian", 7.25, false));
            w.write(person(3, "chen", 8.0, true));
        }
        byte[] bytes = Files.readAllBytes(local);

        try (S3Client s3 = S3Client.builder()
                .endpointOverride(URI.create(endpoint()))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(KEY, SECRET)))
                .forcePathStyle(true)
                // SDK 2.30+ otherwise streams uploads as aws-chunked with a trailing
                // checksum, which SeaweedFS stores verbatim (as basefile's client does).
                .requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED)
                .responseChecksumValidation(ResponseChecksumValidation.WHEN_REQUIRED)
                .build()) {
            // the S3 port opens before the store can take writes: retry for a while
            long deadline = System.currentTimeMillis() + 60_000;
            while (true) {
                try {
                    s3.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
                    s3.putObject(PutObjectRequest.builder().bucket(BUCKET).key(OBJECT).build(),
                            RequestBody.fromBytes(bytes));
                    break;
                } catch (Exception e) {
                    if (System.currentTimeMillis() > deadline) {
                        throw new IllegalStateException("S3 store never became ready", e);
                    }
                    Thread.sleep(500);
                }
            }
        }

        // The app's own S3 wiring (hitorro.storage.s3.*), which now also feeds S3A.
        new S3StorageAutoConfig().s3ProtocolAdapter(endpoint(), BUCKET, KEY, SECRET, false);
    }

    @AfterAll
    static void cleanUp() throws Exception {
        S3aSettings.clear();
        FileSystem.closeAll();
    }

    private static GenericRecord person(long id, String name, double score, boolean active) {
        GenericRecord r = new GenericData.Record(SCHEMA);
        r.put("id", id);
        r.put("name", name);
        r.put("score", score);
        r.put("active", active);
        return r;
    }

    private static List<JsonNode> rows(ParquetLocalTable table) {
        List<JsonNode> out = new ArrayList<>();
        for (Iterator<JVS> it = table.openScan(); it.hasNext(); ) {
            out.add(it.next().getJsonNode());
        }
        return out;
    }

    @Test
    void readsParquetThroughS3a() throws Exception {
        ParquetLocalTable table = new ParquetLocalTable("people", null, "all",
                URI.create("s3a://" + BUCKET + "/" + OBJECT));

        assertThat(table.rowCount()).isEqualTo(3);
        List<JsonNode> rows = rows(table);
        assertThat(rows).extracting(n -> n.get("name").asText()).containsExactly("ada", "brian", "chen");
        JsonNode first = rows.get(0);
        assertThat(first.get("id").isIntegralNumber()).isTrue();
        assertThat(first.get("id").asLong()).isEqualTo(1L);
        assertThat(first.get("score").asDouble()).isEqualTo(9.5);
        assertThat(first.get("active").asBoolean()).isTrue();
    }

    @Test
    void s3SchemeIsReadThroughS3aToo() throws Exception {
        ParquetLocalTable table = new ParquetLocalTable("people", null, "all",
                URI.create("s3://" + BUCKET + "/" + OBJECT));

        assertThat(rows(table)).extracting(n -> n.get("id").asLong()).containsExactly(1L, 2L, 3L);
    }
}
