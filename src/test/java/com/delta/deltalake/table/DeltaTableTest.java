package com.delta.deltalake.table;

import com.delta.deltalake.log.AddFile;
import com.delta.deltalake.log.LogRecord;
import com.delta.deltalake.log.TransactionLog;
import com.delta.deltalake.storage.LocalStorage;
import com.delta.deltalake.storage.Storage;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DeltaTableTest {

    @Test
    void shouldOpenAndReadTableSnapshot() throws Exception {
        Path root = Files.createTempDirectory("delta-table-test");

        Storage storage = new LocalStorage(root);
        TransactionLog log = new TransactionLog(storage);

        AddFile file = new AddFile(
                "data/a.parquet",
                100,
                1000,
                true
        );

        assertTrue(
                log.append(
                        0,
                        List.of(new LogRecord("add", file))
                )
        );

        DeltaTable table = DeltaTable.open(storage);

        Snapshot snapshot = table.snapshot();

        assertEquals(0, snapshot.version());
        assertEquals(1, snapshot.fileCount());
        assertTrue(snapshot.contains("data/a.parquet"));
    }
}