package com.delta.deltalake.table;

import com.delta.deltalake.data.Record;
import com.delta.deltalake.storage.LocalStorage;
import com.delta.deltalake.storage.Storage;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DeltaTableAppendTest {

    @Test
    void shouldAppendRecordsToTable() throws Exception {
        Path root = Files.createTempDirectory("delta-append-test");

        Storage storage = new LocalStorage(root);
        DeltaTable table = DeltaTable.open(storage);

        List<Record> records = List.of(
                new Record(1, "Alice", 25),
                new Record(2, "Bob", 31),
                new Record(3, "Charlie", 28)
        );

        table.append(records);

        Snapshot snapshot = table.snapshot();

        assertEquals(0, snapshot.version());
        assertEquals(1, snapshot.fileCount());

        assertTrue(
                snapshot.activeFiles().stream()
                        .anyMatch(file -> file.path().startsWith("data/"))
        );
    }
}