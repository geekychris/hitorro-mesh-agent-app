/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.mesh.agent.app;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hitorro.jsontypesystem.JVS;
import com.hitorro.jsontypesystem.Type;
import com.hitorro.mesh.agent.LocalTable;
import com.hitorro.util.basefile.fs.BaseFile;
import com.hitorro.util.basefile.fs.BaseFileSystem;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Simple {@link LocalTable} that reads its rows from an NDJSON file (or any
 * URL). Loaded once at startup and held in memory — good enough for
 * phase-1 demos and unit-of-shard datasets that fit in RAM.
 *
 * <p>Larger tables should use a {@code LocalTable} backed by a kvstore,
 * Lucene index, or basefile-shard iterator — swap the class, keep the
 * interface. Phase 2 will add these variants in {@code hitorro-mesh-agent}.</p>
 */
public final class NdjsonLocalTable implements LocalTable {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String name;
    private final Type type;
    private final String partitionKey;
    private final List<JVS> rows;

    public NdjsonLocalTable(String name, Type type, String partitionKey, URI ndjson) throws IOException {
        this.name = name;
        this.type = type;
        this.partitionKey = partitionKey;
        this.rows = load(ndjson);
    }

    private static List<JVS> load(URI ndjson) throws IOException {
        List<JVS> out = new ArrayList<>();
        // Route local files through basefile so extension-based compression
        // (.bz2, .bzip, .bzip2, .gz, .zstd) is decompressed transparently.
        // Non-file schemes (http, s3, hdfs) fall back to URL.openStream —
        // basefile has separate systems for those we haven't wired up here.
        try (InputStream raw = openStream(ndjson);
             BufferedReader r = new BufferedReader(new InputStreamReader(raw, StandardCharsets.UTF_8))) {
            String line;
            while ((line = r.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#")) continue;
                out.add(new JVS(MAPPER.readTree(line)));
            }
        }
        return out;
    }

    /**
     * Open the source stream via {@link BaseFile#getInputStream()} so
     * every filesystem basefile knows about (local, HDFS, S3 via the
     * addProtocolAdapter registry) works, AND extension-based
     * decompression (.gz / .bz2 / .bzip / .zstd) happens transparently.
     *
     * <p>The URI is passed as-is to {@code getBaseFileFromPath} so the
     * scheme drives which ProtocolAdapter picks it up:
     * {@code file:} → FileFile, {@code hdfs:} → DFS, and so on. A bare
     * absolute path with no scheme falls through to the DFS default,
     * which is not what mesh datasets want, so the config layer requires
     * a URI (all shipped configs use {@code file:...}).</p>
     */
    private static InputStream openStream(URI uri) throws IOException {
        BaseFile bf = BaseFileSystem.getBaseFileFromPath(uri.toString());
        if (bf == null) {
            throw new IOException("basefile has no protocol adapter for " + uri
                    + " — supported: file, hdfs, ftp, s3 (via addProtocolAdapter). "
                    + "Make sure the ndjson-file config value includes an explicit "
                    + "scheme like file:/path/to/data.ndjson.bz2");
        }
        return bf.getInputStream();
    }

    @Override public String name() { return name; }
    @Override public Type type() { return type; }
    @Override public String partitionKey() { return partitionKey; }
    @Override public Iterator<JVS> openScan() { return new ArrayList<>(rows).iterator(); }

    public int rowCount() { return rows.size(); }
}
