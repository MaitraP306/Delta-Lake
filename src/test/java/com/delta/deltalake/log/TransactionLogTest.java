package com.delta.deltalake.log;

import com.delta.deltalake.storage.LocalStorage;
import com.delta.deltalake.storage.Storage;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TransactionLogTest {

    @Test
    void shouldCommitTransaction() throws Exception {
        Path root = Files.createTempDirectory("delta-log-test");

        Storage storage = new LocalStorage(root);
        TransactionLog log = new TransactionLog(storage);

        AddFile addFile = new AddFile(
                "data/a.parquet",
                100,
                123456789L,
                true
        );

        boolean committed = log.append(
                0,
                List.of(
                        new LogRecord("add", addFile)
                )
        );

        assertTrue(committed);
        assertEquals(0, log.latestVersion());
        assertTrue(
                storage.exists("_delta_log/00000000000000000000.json")
        );
    }

    @Test
    void shouldRejectDuplicateVersion() throws Exception {
        Path root = Files.createTempDirectory("delta-log-test");

        Storage storage = new LocalStorage(root);
        TransactionLog log = new TransactionLog(storage);

        LogRecord record = new LogRecord(
                "add",
                new AddFile(
                        "data/a.parquet",
                        100,
                        123456789L,
                        true
                )
        );

        assertTrue(log.append(0, List.of(record)));
        assertFalse(log.append(0, List.of(record)));

        assertEquals(0, log.latestVersion());
    }
}