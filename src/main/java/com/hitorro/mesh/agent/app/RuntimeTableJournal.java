/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.agent.app;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hitorro.mesh.RegisterTableMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * NDJSON append-only journal of runtime table registrations, so
 * {@link RuntimeTableInstaller} can rebuild the runtime registry on
 * agent restart without needing the driver to re-publish every
 * {@link RegisterTableMessage} it ever sent.
 *
 * <p>File layout — one JSON object per line:</p>
 * <pre>
 *   {"op":"register","msg":{name:…, typeJson:…, uri:…, format:…, broadcast:…, partitionKey:…}}
 *   {"op":"unregister","name":"my_langs","partitionKey":null}
 * </pre>
 *
 * <p>On boot: {@link #loadActive()} replays the journal, keeping only
 * the last message per (name, partitionKey) and dropping any that were
 * later unregistered. That deduped set is what
 * {@link RuntimeTableInstaller} replays.</p>
 *
 * <p>Path: {@code ${hitorro.agent.home}/runtime-tables/<agentId>.jsonl},
 * default {@code ~/.hitorro/agent/runtime-tables/<agentId>.jsonl}.
 * Journal files are per-agent so multiple agents on the same host
 * don't step on each other.</p>
 *
 * <p>Not full ACID — power-loss mid-append could truncate the last
 * line. On next boot the truncated line fails to parse and is
 * skipped (with a warn log); everything before it replays cleanly.</p>
 */
public final class RuntimeTableJournal {

    private static final Logger log = LoggerFactory.getLogger(RuntimeTableJournal.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Path file;

    public RuntimeTableJournal(String agentId) {
        String home = System.getProperty("hitorro.agent.home",
                System.getProperty("user.home") + "/.hitorro/agent");
        this.file = Path.of(home, "runtime-tables", agentId + ".jsonl");
    }

    /** Test-friendly constructor accepting the exact journal path. */
    RuntimeTableJournal(Path file) { this.file = file; }

    public Path path() { return file; }

    /** Append an install record. Safe to call concurrently — the write
     *  uses {@link StandardOpenOption#APPEND} which is atomic per-line
     *  on POSIX for lines under PIPE_BUF (4KB); the typical
     *  RegisterTableMessage payload fits comfortably. */
    public synchronized void appendRegister(RegisterTableMessage msg) {
        try {
            ensureDir();
            ObjectNode n = MAPPER.createObjectNode();
            n.put("op", "register");
            n.set("msg", MAPPER.valueToTree(msg));
            Files.writeString(file, MAPPER.writeValueAsString(n) + "\n",
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            log.warn("runtime-tables: journal append (register) failed: {}", e.toString());
        }
    }

    /** Append an unregister tombstone. */
    public synchronized void appendUnregister(String name, String partitionKey) {
        try {
            ensureDir();
            ObjectNode n = MAPPER.createObjectNode();
            n.put("op", "unregister");
            n.put("name", name);
            n.put("partitionKey", partitionKey);   // ObjectNode.put(String, null) → NullNode
            Files.writeString(file, MAPPER.writeValueAsString(n) + "\n",
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            log.warn("runtime-tables: journal append (unregister) failed: {}", e.toString());
        }
    }

    /**
     * Replay the journal → dedup by (name, partitionKey) → drop any
     * (name, partitionKey) with a later unregister. Result is the set
     * of tables that should be reinstalled at boot.
     */
    public synchronized List<RegisterTableMessage> loadActive() {
        if (!Files.exists(file)) return List.of();
        Map<String, RegisterTableMessage> active = new LinkedHashMap<>();
        int okLines = 0, badLines = 0;
        try (var lines = Files.lines(file, StandardCharsets.UTF_8)) {
            for (String line : (Iterable<String>) lines::iterator) {
                if (line.isBlank()) continue;
                try {
                    ObjectNode n = (ObjectNode) MAPPER.readTree(line);
                    String op = n.path("op").asText();
                    if ("register".equals(op)) {
                        RegisterTableMessage m = MAPPER.treeToValue(n.get("msg"), RegisterTableMessage.class);
                        active.put(key(m.name(), m.partitionKey()), m);
                    } else if ("unregister".equals(op)) {
                        String name = n.path("name").asText();
                        String pk = n.hasNonNull("partitionKey") ? n.get("partitionKey").asText() : null;
                        active.remove(key(name, pk));
                    }
                    okLines++;
                } catch (Exception e) {
                    badLines++;
                }
            }
        } catch (IOException e) {
            log.warn("runtime-tables: journal read failed: {}", e.toString());
            return List.of();
        }
        if (badLines > 0) {
            log.warn("runtime-tables: skipped {} malformed line(s) in {} (ok={})",
                    badLines, file, okLines);
        }
        return new ArrayList<>(active.values());
    }

    /**
     * Rewrite the journal to contain only currently-active register
     * records — drops tombstones and older shadowed entries. Intended
     * for boot-time compaction after {@link #loadActive} when the
     * on-disk file has grown significantly larger than the active set.
     *
     * <p>Atomic-rename via a sibling {@code .tmp} file so a crash
     * mid-compaction leaves either the old journal or the new one,
     * never a partial file.</p>
     *
     * @return number of lines dropped (0 if the file was already tight)
     */
    public synchronized int compact() {
        if (!Files.exists(file)) return 0;
        List<RegisterTableMessage> active = loadActive();
        long oldLines = -1;
        try (var lines = Files.lines(file, StandardCharsets.UTF_8)) {
            oldLines = lines.count();
        } catch (IOException e) {
            log.warn("runtime-tables: journal line-count failed: {}", e.toString());
            return 0;
        }
        // Only rewrite when there's actual savings — avoid churn.
        if (oldLines <= active.size() + 4) return 0;

        try {
            ensureDir();
            Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
            StringBuilder sb = new StringBuilder();
            for (RegisterTableMessage m : active) {
                var n = MAPPER.createObjectNode();
                n.put("op", "register");
                n.set("msg", MAPPER.valueToTree(m));
                sb.append(MAPPER.writeValueAsString(n)).append('\n');
            }
            Files.writeString(tmp, sb.toString(), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            Files.move(tmp, file,
                    StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            long dropped = oldLines - active.size();
            log.info("runtime-tables: compacted journal {} — {} lines → {} lines ({} dropped)",
                    file.getFileName(), oldLines, active.size(), dropped);
            return (int) dropped;
        } catch (IOException e) {
            log.warn("runtime-tables: journal compaction failed: {}", e.toString());
            return 0;
        }
    }

    private void ensureDir() throws IOException {
        Path parent = file.getParent();
        if (parent != null) Files.createDirectories(parent);
    }

    private static String key(String name, String partitionKey) {
        return partitionKey == null ? name : name + "@" + partitionKey;
    }
}
