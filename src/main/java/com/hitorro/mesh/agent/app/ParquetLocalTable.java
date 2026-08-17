/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.agent.app;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hitorro.jsontypesystem.JVS;
import com.hitorro.jsontypesystem.Type;
import com.hitorro.jvssql.config.StreamConfig;
import com.hitorro.mesh.agent.LocalTable;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * {@link LocalTable} that reads its rows from a Parquet file, mirroring
 * {@link NdjsonLocalTable} but for Parquet. Loaded once at construction
 * and held in memory as {@code List<JVS>} — {@code openScan} returns a
 * fresh iterator each call.
 *
 * <p>Uses parquet-avro's {@link AvroParquetReader} to hydrate each row
 * into an Avro {@link GenericRecord}, then converts to
 * {@link com.fasterxml.jackson.databind.node.ObjectNode} field-by-field
 * so JVS can wrap it. Scalars keep their Parquet type (long, double,
 * boolean); byte-arrays and everything else stringify.</p>
 *
 * <p>Destination URI drives Hadoop's {@code FileSystem} — {@code file:},
 * {@code s3a:}, {@code hdfs:} all work. For {@code s3a:} the agent
 * needs {@code fs.s3a.*} config in either {@code core-site.xml} on the
 * classpath or {@code -D} JVM flags before boot; see the
 * {@code hitorro.storage.s3.*} property surface driver-side.</p>
 *
 * <p>Loaded reflectively by {@link RuntimeTableInstaller} so agents
 * that don't have parquet-avro on the classpath just don't support
 * Parquet runtime tables — they'll log a warning and skip the
 * install rather than crash at boot.</p>
 */
public final class ParquetLocalTable implements LocalTable {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String name;
    private final Type type;
    private final String partitionKey;
    private final List<JVS> rows;

    public ParquetLocalTable(String name, Type type, String partitionKey, URI parquet) throws IOException {
        this.name = name;
        this.type = type;
        this.partitionKey = partitionKey;
        this.rows = load(parquet);
    }

    private static List<JVS> load(URI parquet) throws IOException {
        // s3:// isn't recognised by Hadoop FileSystem — the driver uses s3a://
        // when writing to MinIO. Assume the URI already carries a
        // Hadoop-friendly scheme; if not, translate here defensively.
        String u = parquet.toString();
        if (u.startsWith("s3://")) u = "s3a://" + u.substring("s3://".length());

        Configuration conf = new Configuration();
        // Best-effort MinIO defaults so file: and s3a: work out of the box.
        conf.set("fs.s3a.path.style.access", "true");

        List<JVS> out = new ArrayList<>();
        try (ParquetReader<GenericRecord> reader = AvroParquetReader
                .<GenericRecord>builder(HadoopInputFile.fromPath(
                        new org.apache.hadoop.fs.Path(u), conf))
                .withConf(conf)
                .build()) {
            GenericRecord rec;
            while ((rec = reader.read()) != null) {
                out.add(new JVS(recordToJson(rec)));
            }
        }
        return out;
    }

    /**
     * Convert a GenericRecord to an ObjectNode, one field at a time. Scalars
     * preserve their JSON type (long/double/boolean); bytes and other
     * complex Avro types stringify via {@code toString()}. Nested records
     * recurse. Arrays flatten to JSON arrays.
     */
    private static JsonNode recordToJson(GenericRecord rec) {
        ObjectNode n = MAPPER.createObjectNode();
        Schema schema = rec.getSchema();
        for (Schema.Field f : schema.getFields()) {
            Object v = rec.get(f.pos());
            n.set(f.name(), toJson(v));
        }
        return n;
    }

    private static JsonNode toJson(Object v) {
        if (v == null) return MAPPER.nullNode();
        if (v instanceof CharSequence cs) return MAPPER.getNodeFactory().textNode(cs.toString());
        if (v instanceof Integer i)  return MAPPER.getNodeFactory().numberNode(i);
        if (v instanceof Long l)     return MAPPER.getNodeFactory().numberNode(l);
        if (v instanceof Double d)   return MAPPER.getNodeFactory().numberNode(d);
        if (v instanceof Float ff)   return MAPPER.getNodeFactory().numberNode(ff);
        if (v instanceof Boolean b)  return MAPPER.getNodeFactory().booleanNode(b);
        if (v instanceof GenericRecord r) return recordToJson(r);
        if (v instanceof java.util.Collection<?> c) {
            var arr = MAPPER.createArrayNode();
            for (Object e : c) arr.add(toJson(e));
            return arr;
        }
        return MAPPER.getNodeFactory().textNode(v.toString());
    }

    @Override public String name() { return name; }
    @Override public Type type() { return type; }
    @Override public String partitionKey() { return partitionKey; }
    @Override public Iterator<JVS> openScan() { return new ArrayList<>(rows).iterator(); }
    @Override public StreamConfig streamConfig() { return null; }
    public int rowCount() { return rows.size(); }
}
