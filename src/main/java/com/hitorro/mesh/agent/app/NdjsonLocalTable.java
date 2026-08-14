/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.mesh.agent.app;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hitorro.jsontypesystem.JVS;
import com.hitorro.jsontypesystem.Type;
import com.hitorro.mesh.agent.LocalTable;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URL;
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
        URL url = ndjson.toURL();
        try (BufferedReader r = new BufferedReader(new InputStreamReader(url.openStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = r.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#")) continue;
                out.add(new JVS(MAPPER.readTree(line)));
            }
        }
        return out;
    }

    @Override public String name() { return name; }
    @Override public Type type() { return type; }
    @Override public String partitionKey() { return partitionKey; }
    @Override public Iterator<JVS> openScan() { return new ArrayList<>(rows).iterator(); }

    public int rowCount() { return rows.size(); }
}
