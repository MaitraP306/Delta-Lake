package com.delta.deltalake.table;

import com.delta.deltalake.data.Row;
import com.delta.deltalake.storage.LocalStorage;
import com.delta.deltalake.log.*;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class OptimisticTransactionTest {
    @Test
    void transactionAbortsWhenReadFileChanges() throws Exception {
        Path root = Files.createTempDirectory("delta-occ-");
        try {
            DeltaTable first = DeltaTable.open(new LocalStorage(root));
            first.appendRows(List.of(Row.infer(Map.of("id", 1L, "name", "a"))));
            Snapshot snapshot = first.snapshot();

            OptimisticTransaction tx = first.beginTransaction();
            tx.readPaths(snapshot.activeFiles().stream().map(AddFile::path).collect(java.util.stream.Collectors.toSet()));

            DeltaTable concurrent = DeltaTable.open(new LocalStorage(root));
            String observedPath = snapshot.activeFiles().iterator().next().path();
            List<LogRecord> concurrentActions = List.of(ActionCodec.encode(new RemoveFile(observedPath, System.currentTimeMillis(), true)));
            OptimisticTransaction concurrentTx = concurrent.beginTransaction();
            concurrentTx.readPath(observedPath);
            assertTrue(concurrentTx.commit(concurrentActions));

            List<LogRecord> actions = List.of(ActionCodec.encode(new CommitInfo(System.currentTimeMillis(), "TEST", Map.of(), null)));
            assertFalse(tx.commit(actions));
        } finally {
            delete(root);
        }
    }

    @Test
    void transactionCanCommitWhenObservedSnapshotIsUnchanged() throws Exception {
        Path root = Files.createTempDirectory("delta-occ-success-");
        try {
            DeltaTable table = DeltaTable.open(new LocalStorage(root));
            table.appendRows(List.of(Row.infer(Map.of("id", 1L, "name", "a"))));
            Snapshot snapshot = table.snapshot();
            OptimisticTransaction tx = table.beginTransaction();
            tx.readPaths(snapshot.activeFiles().stream().map(AddFile::path).collect(java.util.stream.Collectors.toSet()));

            List<LogRecord> actions = List.of(ActionCodec.encode(new CommitInfo(System.currentTimeMillis(), "TEST", Map.of(), null)));
            assertTrue(tx.commit(actions));
            assertEquals(1, table.version());
        } finally {
            delete(root);
        }
    }


    @Test
    void transactionCanCommitOnTopOfDisjointConcurrentAppend() throws Exception {
        Path root = Files.createTempDirectory("delta-occ-disjoint-");
        try {
            DeltaTable first = DeltaTable.open(new LocalStorage(root));
            first.appendRows(List.of(Row.infer(Map.of("id", 1L, "name", "a"))));
            OptimisticTransaction tx = first.beginTransaction();
            tx.readPath(first.snapshot().activeFiles().iterator().next().path());

            DeltaTable concurrent = DeltaTable.open(new LocalStorage(root));
            concurrent.appendRows(List.of(Row.infer(Map.of("id", 2L, "name", "b"))));

            List<LogRecord> actions = List.of(ActionCodec.encode(new CommitInfo(System.currentTimeMillis(), "TEST", Map.of(), null)));
            assertTrue(tx.commit(actions));
            assertEquals(2, first.version());
        } finally {
            delete(root);
        }
    }


    @Test
    void metadataSensitiveTransactionRejectsConcurrentMetadataChange() throws Exception {
        Path root = Files.createTempDirectory("delta-occ-metadata-");
        try {
            DeltaTable first = DeltaTable.open(new LocalStorage(root));
            first.appendRows(List.of(Row.infer(Map.of("id", 1L, "name", "a"))));
            OptimisticTransaction tx = first.beginTransaction();
            tx.failOnMetadataChanges();

            DeltaTable concurrent = DeltaTable.open(new LocalStorage(root));
            concurrent.setAutoOptimize(true);

            List<LogRecord> actions = List.of(ActionCodec.encode(new CommitInfo(System.currentTimeMillis(), "TEST", Map.of(), null)));
            assertFalse(tx.commit(actions));
        } finally {
            delete(root);
        }
    }

    @Test
    void transactionRejectsAConcurrentNewMatchingFile() throws Exception {
        Path root = Files.createTempDirectory("delta-occ-new-file-");
        try {
            DeltaTable first = DeltaTable.open(new LocalStorage(root));
            first.appendRows(List.of(Row.infer(Map.of("id", 10L, "name", "a"))));
            Snapshot base = first.snapshot();

            OptimisticTransaction tx = first.beginTransaction();
            tx.failIfNewFileMatches(file -> {
                FileStats.ColumnStats stats = file.stats().columns().get("id");
                return stats != null && stats.min() instanceof Number min && stats.max() instanceof Number max && min.longValue() <= 20L && max.longValue() >= 20L;
            });

            DeltaTable concurrent = DeltaTable.open(new LocalStorage(root));
            concurrent.appendRows(List.of(Row.infer(Map.of("id", 20L, "name", "b"))));

            List<LogRecord> actions = List.of(ActionCodec.encode(new CommitInfo(System.currentTimeMillis(), "TEST", Map.of(), null)));
            assertFalse(tx.commit(actions));
            assertEquals(base.version() + 1, concurrent.version());
        } finally {
            delete(root);
        }
    }

    private static void delete(Path root) throws Exception {
        try (var stream = Files.walk(root)) {
            stream.sorted(java.util.Comparator.reverseOrder()).forEach(path -> {
                try { Files.deleteIfExists(path); } catch (Exception e) { throw new RuntimeException(e); }
            });
        }
    }
}
