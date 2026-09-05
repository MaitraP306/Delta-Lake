package com.delta.deltalake.table;

import com.delta.deltalake.log.*;
import com.delta.deltalake.storage.LocalStorage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class SnapshotVerificationTest {

    @TempDir
    Path tempDir;

    private CommitInfo commitInfo(String operation) {
        return new CommitInfo(
                System.currentTimeMillis(),
                operation,
                Map.of(),
                null
        );
    }

    private LogRecord add(String path) {
        return ActionCodec.encode(
                new AddFile(
                        path,
                        100L,
                        1700000000000L,
                        true
                )
        );
    }

    private LogRecord remove(String path) {
        return ActionCodec.encode(
                new RemoveFile(
                        path,
                        1700000000000L,
                        true
                )
        );
    }

    @Test
    void snapshotReconstructsAddedFiles() throws Exception {

        LocalStorage storage = new LocalStorage(tempDir);
        TransactionLog log = new TransactionLog(storage);

        log.append(
                0,
                List.of(
                        ActionCodec.encode(
                                new Protocol(1, 1)
                        ),
                        ActionCodec.encode(
                                new Metadata(
                                        "table-1",
                                        "parquet",
                                        "{\"schema\":\"test\"}",
                                        List.of(),
                                        Map.of()
                                )
                        ),
                        add("data/file-a.parquet"),
                        ActionCodec.encode(
                                commitInfo("WRITE")
                        )
                )
        );

        SnapshotManager manager = new SnapshotManager(log);

        Snapshot snapshot = manager.loadSnapshot(0);

        assertEquals(0, snapshot.version());
        assertEquals(1, snapshot.fileCount());

        assertTrue(
                snapshot.contains("data/file-a.parquet")
        );

        AddFile file = snapshot.activeFiles()
                .iterator()
                .next();

        assertEquals(
                "data/file-a.parquet",
                file.path()
        );

        assertEquals(100L, file.size());

        assertEquals(
                1,
                snapshot.protocol().minReaderVersion()
        );

        assertEquals(
                1,
                snapshot.protocol().minWriterVersion()
        );

        assertEquals(
                "table-1",
                snapshot.metadata().id()
        );
    }

    @Test
    void snapshotAppliesAddFilesAcrossVersions() throws Exception {

        LocalStorage storage = new LocalStorage(tempDir);
        TransactionLog log = new TransactionLog(storage);

        log.append(
                0,
                List.of(add("data/file-a.parquet"))
        );

        log.append(
                1,
                List.of(add("data/file-b.parquet"))
        );

        log.append(
                2,
                List.of(add("data/file-c.parquet"))
        );

        SnapshotManager manager = new SnapshotManager(log);

        Snapshot snapshot = manager.loadSnapshot(2);

        assertEquals(2, snapshot.version());
        assertEquals(3, snapshot.fileCount());

        assertTrue(snapshot.contains("data/file-a.parquet"));
        assertTrue(snapshot.contains("data/file-b.parquet"));
        assertTrue(snapshot.contains("data/file-c.parquet"));
    }

    @Test
    void removeFileRemovesItFromActiveSnapshot() throws Exception {

        LocalStorage storage = new LocalStorage(tempDir);
        TransactionLog log = new TransactionLog(storage);

        log.append(
                0,
                List.of(
                        add("data/file-a.parquet"),
                        add("data/file-b.parquet")
                )
        );

        log.append(
                1,
                List.of(
                        remove("data/file-a.parquet")
                )
        );

        SnapshotManager manager = new SnapshotManager(log);

        Snapshot snapshot = manager.loadSnapshot(1);

        assertEquals(1, snapshot.fileCount());

        assertFalse(
                snapshot.contains("data/file-a.parquet")
        );

        assertTrue(
                snapshot.contains("data/file-b.parquet")
        );

        assertEquals(
                1,
                snapshot.tombstones().size()
        );

        RemoveFile tombstone =
                snapshot.tombstones()
                        .iterator()
                        .next();

        assertEquals(
                "data/file-a.parquet",
                tombstone.path()
        );
    }

    @Test
    void addingSamePathAgainRemovesItsTombstone() throws Exception {

        LocalStorage storage = new LocalStorage(tempDir);
        TransactionLog log = new TransactionLog(storage);

        log.append(
                0,
                List.of(add("data/file-a.parquet"))
        );

        log.append(
                1,
                List.of(remove("data/file-a.parquet"))
        );

        log.append(
                2,
                List.of(add("data/file-a.parquet"))
        );

        SnapshotManager manager = new SnapshotManager(log);

        Snapshot snapshot = manager.loadSnapshot(2);

        assertEquals(1, snapshot.fileCount());

        assertTrue(
                snapshot.contains("data/file-a.parquet")
        );

        assertEquals(
                0,
                snapshot.tombstones().size()
        );
    }

    @Test
    void timeTravelReconstructsHistoricalSnapshots() throws Exception {

        LocalStorage storage = new LocalStorage(tempDir);
        TransactionLog log = new TransactionLog(storage);

        log.append(
                0,
                List.of(add("data/file-a.parquet"))
        );

        log.append(
                1,
                List.of(add("data/file-b.parquet"))
        );

        log.append(
                2,
                List.of(remove("data/file-a.parquet"))
        );

        SnapshotManager manager = new SnapshotManager(log);

        Snapshot version0 = manager.loadSnapshot(0);
        Snapshot version1 = manager.loadSnapshot(1);
        Snapshot version2 = manager.loadSnapshot(2);

        // Version 0
        assertEquals(1, version0.fileCount());
        assertTrue(
                version0.contains("data/file-a.parquet")
        );
        assertFalse(
                version0.contains("data/file-b.parquet")
        );

        // Version 1
        assertEquals(2, version1.fileCount());
        assertTrue(
                version1.contains("data/file-a.parquet")
        );
        assertTrue(
                version1.contains("data/file-b.parquet")
        );

        // Version 2
        assertEquals(1, version2.fileCount());
        assertFalse(
                version2.contains("data/file-a.parquet")
        );
        assertTrue(
                version2.contains("data/file-b.parquet")
        );
    }

    @Test
    void metadataAndProtocolAreUpdatedByLaterCommits() throws Exception {

        LocalStorage storage = new LocalStorage(tempDir);
        TransactionLog log = new TransactionLog(storage);

        log.append(
                0,
                List.of(
                        ActionCodec.encode(
                                new Protocol(1, 1)
                        ),
                        ActionCodec.encode(
                                new Metadata(
                                        "table-1",
                                        "parquet",
                                        "schema-v1",
                                        List.of(),
                                        Map.of()
                                )
                        )
                )
        );

        log.append(
                1,
                List.of(
                        ActionCodec.encode(
                                new Protocol(2, 3)
                        ),
                        ActionCodec.encode(
                                new Metadata(
                                        "table-1",
                                        "parquet",
                                        "schema-v2",
                                        List.of("date"),
                                        Map.of(
                                                "key",
                                                "value"
                                        )
                                )
                        )
                )
        );

        SnapshotManager manager = new SnapshotManager(log);

        Snapshot snapshot = manager.loadSnapshot(1);

        assertEquals(
                2,
                snapshot.protocol().minReaderVersion()
        );

        assertEquals(
                3,
                snapshot.protocol().minWriterVersion()
        );

        assertEquals(
                "schema-v2",
                snapshot.metadata().schemaString()
        );

        assertEquals(
                List.of("date"),
                snapshot.metadata().partitionColumns()
        );

        assertEquals(
                "value",
                snapshot.metadata().configuration().get("key")
        );
    }

    @Test
    void transactionsKeepLatestVersionPerApplication() throws Exception {

        LocalStorage storage = new LocalStorage(tempDir);
        TransactionLog log = new TransactionLog(storage);

        log.append(
                0,
                List.of(
                        ActionCodec.encode(
                                new Txn(
                                        "app-1",
                                        1L,
                                        1000L
                                )
                        )
                )
        );

        log.append(
                1,
                List.of(
                        ActionCodec.encode(
                                new Txn(
                                        "app-1",
                                        3L,
                                        3000L
                                )
                        )
                )
        );

        log.append(
                2,
                List.of(
                        ActionCodec.encode(
                                new Txn(
                                        "app-1",
                                        2L,
                                        2000L
                                )
                        )
                )
        );

        SnapshotManager manager = new SnapshotManager(log);

        Snapshot snapshot = manager.loadSnapshot(2);

        assertEquals(1, snapshot.transactions().size());

        Txn txn = snapshot.transactions().get("app-1");

        assertNotNull(txn);
        assertEquals(3L, txn.version());
        assertEquals(3000L, txn.lastUpdated());
    }

    @Test
    void commitInfoDoesNotBecomeSnapshotState() throws Exception {

        LocalStorage storage = new LocalStorage(tempDir);
        TransactionLog log = new TransactionLog(storage);

        log.append(
                0,
                List.of(
                        ActionCodec.encode(
                                commitInfo("WRITE")
                        )
                )
        );

        SnapshotManager manager = new SnapshotManager(log);

        Snapshot snapshot = manager.loadSnapshot(0);

        assertEquals(0, snapshot.fileCount());
        assertTrue(snapshot.tombstones().isEmpty());
        assertTrue(snapshot.transactions().isEmpty());
    }

    @Test
    void requestingVersionBeyondLatestFails() throws Exception {

        LocalStorage storage = new LocalStorage(tempDir);
        TransactionLog log = new TransactionLog(storage);

        log.append(
                0,
                List.of(add("data/file-a.parquet"))
        );

        SnapshotManager manager = new SnapshotManager(log);

        assertThrows(
                IllegalArgumentException.class,
                () -> manager.loadSnapshot(1)
        );
    }
}