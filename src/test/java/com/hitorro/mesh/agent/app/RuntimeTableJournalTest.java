/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.agent.app;

import com.hitorro.mesh.RegisterTableMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link RuntimeTableJournal}. Uses {@link TempDir} so
 * every test gets a fresh empty file; covers append/replay/tombstone/
 * compact/malformed-line-skip.
 */
class RuntimeTableJournalTest {

    private static RegisterTableMessage msg(String name, String uri) {
        return new RegisterTableMessage(name,
                "{\"name\":\"" + name + "\",\"fields\":[{\"name\":\"x\",\"type\":\"core_string\"}]}",
                uri, "ndjson", true, null);
    }

    @Test
    void emptyFile_replayReturnsNothing(@TempDir Path tmp) {
        RuntimeTableJournal j = new RuntimeTableJournal(tmp.resolve("j.jsonl"));
        assertThat(j.loadActive()).isEmpty();
    }

    @Test
    void appendRegister_thenReplay_returnsIt(@TempDir Path tmp) {
        RuntimeTableJournal j = new RuntimeTableJournal(tmp.resolve("j.jsonl"));
        j.appendRegister(msg("t1", "file:/a.ndjson"));
        j.appendRegister(msg("t2", "file:/b.ndjson"));

        List<RegisterTableMessage> active = j.loadActive();
        assertThat(active).extracting(RegisterTableMessage::name).containsExactly("t1", "t2");
    }

    @Test
    void appendUnregister_tombstonesTheEntry(@TempDir Path tmp) {
        RuntimeTableJournal j = new RuntimeTableJournal(tmp.resolve("j.jsonl"));
        j.appendRegister(msg("t1", "file:/a.ndjson"));
        j.appendRegister(msg("t2", "file:/b.ndjson"));
        j.appendUnregister("t1", null);

        assertThat(j.loadActive())
                .extracting(RegisterTableMessage::name)
                .containsExactly("t2");
    }

    @Test
    void reRegister_replacesEarlierEntry(@TempDir Path tmp) {
        RuntimeTableJournal j = new RuntimeTableJournal(tmp.resolve("j.jsonl"));
        j.appendRegister(msg("t1", "file:/a.ndjson"));
        j.appendRegister(msg("t1", "file:/a-v2.ndjson"));   // same name

        List<RegisterTableMessage> active = j.loadActive();
        assertThat(active).hasSize(1);
        assertThat(active.get(0).uri()).isEqualTo("file:/a-v2.ndjson");
    }

    @Test
    void compact_dropsTombstonesAndReplacedEntries(@TempDir Path tmp) {
        Path file = tmp.resolve("j.jsonl");
        RuntimeTableJournal j = new RuntimeTableJournal(file);
        j.appendRegister(msg("t1", "file:/a.ndjson"));
        j.appendRegister(msg("t2", "file:/b.ndjson"));
        j.appendRegister(msg("t3", "file:/c.ndjson"));
        j.appendUnregister("t1", null);              // tombstone
        j.appendRegister(msg("t2", "file:/b-v2.ndjson"));  // replacement
        // File has 5 lines; active is {t2, t3} = 2 entries.

        long before = countLines(file);
        assertThat(before).isEqualTo(5);

        int dropped = j.compact();

        assertThat(dropped).isGreaterThan(0);
        assertThat(countLines(file)).isEqualTo(2);
        // Post-compaction replay still returns the same active set.
        assertThat(j.loadActive()).extracting(RegisterTableMessage::name)
                .containsExactlyInAnyOrder("t2", "t3");
    }

    @Test
    void compact_isNoopWhenSavingsAreSmall(@TempDir Path tmp) {
        RuntimeTableJournal j = new RuntimeTableJournal(tmp.resolve("j.jsonl"));
        // Just 2 entries, no tombstones — nothing to save.
        j.appendRegister(msg("t1", "file:/a.ndjson"));
        j.appendRegister(msg("t2", "file:/b.ndjson"));
        assertThat(j.compact()).isZero();
    }

    @Test
    void malformedLine_isSkippedNotFatal(@TempDir Path tmp) throws IOException {
        Path file = tmp.resolve("j.jsonl");
        RuntimeTableJournal j = new RuntimeTableJournal(file);
        j.appendRegister(msg("t1", "file:/a.ndjson"));
        // Simulate a truncated / garbled line (e.g. from crash mid-write).
        Files.writeString(file, "{not-json\n",
                StandardCharsets.UTF_8, StandardOpenOption.APPEND);
        j.appendRegister(msg("t2", "file:/b.ndjson"));

        // Both good entries survive; the bad one is skipped silently.
        assertThat(j.loadActive()).extracting(RegisterTableMessage::name)
                .containsExactly("t1", "t2");
    }

    @Test
    void unregisterOfNonExistent_leavesFileConsistent(@TempDir Path tmp) {
        RuntimeTableJournal j = new RuntimeTableJournal(tmp.resolve("j.jsonl"));
        j.appendRegister(msg("t1", "file:/a.ndjson"));
        j.appendUnregister("never-registered", null);     // tombstone with no matching register
        j.appendRegister(msg("t2", "file:/b.ndjson"));

        assertThat(j.loadActive()).extracting(RegisterTableMessage::name)
                .containsExactly("t1", "t2");
    }

    private static long countLines(Path file) {
        try (var s = Files.lines(file)) { return s.count(); }
        catch (IOException e) { throw new RuntimeException(e); }
    }
}
