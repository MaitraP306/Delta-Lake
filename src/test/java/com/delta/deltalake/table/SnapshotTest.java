package com.delta.deltalake.table;

import com.delta.deltalake.log.AddFile;
import com.delta.deltalake.log.LogRecord;
import com.delta.deltalake.log.RemoveFile;
import com.delta.deltalake.log.TransactionLog;
import com.delta.deltalake.storage.LocalStorage;
import com.delta.deltalake.storage.Storage;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SnapshotTest {

    @Test
    void shouldReconstructSnapshots() throws Exception {
        Path root = Files.createTempDirectory("delta-snapshot-test");

        Storage storage = new LocalStorage(root);
        TransactionLog log = new TransactionLog(storage);

        AddFile fileA = new AddFile(
                "data/a.parquet",
                100,
                1000,
                true
        );

        AddFile fileB = new AddFile(
                "data/b.parquet",
                200,
                2000,
                true
        );

        RemoveFile removeA = new RemoveFile(
                "data/a.parquet",
                3000,
                true
        );

        assertTrue(
                log.append(
                        0,
                        List.of(
                                new LogRecord("add", fileA)
                        )
                )
        );

        assertTrue(
                log.append(
                        1,
                        List.of(
                                new LogRecord("add", fileB)
                        )
                )
        );

        assertTrue(
                log.append(
                        2,
                        List.of(
                                new LogRecord("remove", removeA)
                        )
                )
        );

        Snapshot version0 = log.loadSnapshot(0);
        Snapshot version1 = log.loadSnapshot(1);
        Snapshot version2 = log.loadSnapshot(2);

        assertEquals(1, version0.fileCount());
        assertTrue(version0.contains("data/a.parquet"));

        assertEquals(2, version1.fileCount());
        assertTrue(version1.contains("data/a.parquet"));
        assertTrue(version1.contains("data/b.parquet"));

        assertEquals(1, version2.fileCount());
        assertFalse(version2.contains("data/a.parquet"));
        assertTrue(version2.contains("data/b.parquet"));
    }
}