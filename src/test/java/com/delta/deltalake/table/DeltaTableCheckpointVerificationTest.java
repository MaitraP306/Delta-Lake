package com.delta.deltalake.table;

import com.delta.deltalake.data.Row;
import com.delta.deltalake.data.TableSchema;
import com.delta.deltalake.log.AddFile;
import com.delta.deltalake.log.LogAction;
import com.delta.deltalake.log.TransactionLog;
import com.delta.deltalake.storage.LocalStorage;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DeltaTableCheckpointVerificationTest {

    @TempDir
    Path tempDir;

    private TableSchema schema() {
        String schemaJson = """
                {
                  "type": "record",
                  "name": "TestRow",
                  "fields": [
                    {
                      "name": "id",
                      "type": "long"
                    },
                    {
                      "name": "value",
                      "type": "string"
                    }
                  ]
                }
                """;

        return new TableSchema(
                new Schema.Parser().parse(schemaJson)
        );
    }

    private Row row(
            TableSchema schema,
            long id) {

        return Row.of(
                schema,
                Map.of(
                        "id", id,
                        "value", "row-" + id
                )
        );
    }

    private DeltaTable createTable(
            LocalStorage storage,
            TableSchema schema,
            int checkpointInterval)
            throws Exception {

        return DeltaTable.open(
                storage,
                checkpointInterval
        );
    }

    @Test
    void checkpointIntervalMustBePositive() {

        LocalStorage storage =
                new LocalStorage(tempDir);

        assertThrows(
                IllegalArgumentException.class,
                () -> DeltaTable.open(
                        storage,
                        0
                )
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> DeltaTable.open(
                        storage,
                        -1
                )
        );
    }

    @Test
    void explicitCheckpointCreatesCheckpointFile()
            throws Exception {

        LocalStorage storage =
                new LocalStorage(tempDir);

        TableSchema schema =
                schema();

        DeltaTable table =
                createTable(
                        storage,
                        schema,
                        100
                );

        table.appendRows(
                List.of(
                        row(schema, 1),
                        row(schema, 2)
                )
        );

        assertEquals(
                0,
                table.version()
        );

        String checkpointPath =
                "_delta_log/00000000000000000000.checkpoint.parquet";

        assertFalse(
                storage.exists(checkpointPath)
        );

        table.checkpoint();

        assertTrue(
                storage.exists(checkpointPath)
        );
    }

    @Test
    void explicitCheckpointCreatesLastCheckpointPointer()
            throws Exception {

        LocalStorage storage =
                new LocalStorage(tempDir);

        TableSchema schema =
                schema();

        DeltaTable table =
                createTable(
                        storage,
                        schema,
                        100
                );

        table.appendRows(
                List.of(row(schema, 1))
        );

        table.checkpoint();

        assertTrue(
                storage.exists(
                        TransactionLog.LAST_CHECKPOINT
                )
        );

        String contents =
                new String(
                        storage.read(
                                TransactionLog.LAST_CHECKPOINT
                        ),
                        StandardCharsets.UTF_8
                );

        assertTrue(
                contents.contains("0")
        );
    }

    @Test
    void explicitCheckpointOnNonexistentTableDoesNothing()
            throws Exception {

        LocalStorage storage =
                new LocalStorage(tempDir);

        DeltaTable table =
                createTable(
                        storage,
                        schema(),
                        100
                );

        assertFalse(table.exists());

        table.checkpoint();

        assertFalse(
                storage.exists(
                        TransactionLog.LAST_CHECKPOINT
                )
        );
    }

    @Test
    void automaticCheckpointUsesConfiguredInterval()
            throws Exception {

        LocalStorage storage =
                new LocalStorage(tempDir);

        TableSchema schema =
                schema();

        /*
         * interval = 3
         *
         * version 0 -> no checkpoint
         * version 1 -> no checkpoint
         * version 2 -> checkpoint
         */
        DeltaTable table =
                createTable(
                        storage,
                        schema,
                        3
                );

        table.appendRows(
                List.of(row(schema, 0))
        );

        assertEquals(
                0,
                table.version()
        );

        assertFalse(
                storage.exists(
                        "_delta_log/00000000000000000000.checkpoint.parquet"
                )
        );

        table.appendRows(
                List.of(row(schema, 1))
        );

        assertEquals(
                1,
                table.version()
        );

        assertFalse(
                storage.exists(
                        "_delta_log/00000000000000000001.checkpoint.parquet"
                )
        );

        table.appendRows(
                List.of(row(schema, 2))
        );

        assertEquals(
                2,
                table.version()
        );

        assertTrue(
                storage.exists(
                        "_delta_log/00000000000000000002.checkpoint.parquet"
                )
        );
    }

    @Test
    void automaticCheckpointPublishesLatestCheckpoint()
            throws Exception {

        LocalStorage storage =
                new LocalStorage(tempDir);

        TableSchema schema =
                schema();

        DeltaTable table =
                createTable(
                        storage,
                        schema,
                        2
                );

        table.appendRows(
                List.of(row(schema, 1))
        );

        /*
         * version 0: no checkpoint
         */

        assertFalse(
                storage.exists(
                        TransactionLog.LAST_CHECKPOINT
                )
        );

        table.appendRows(
                List.of(row(schema, 2))
        );

        /*
         * version 1 -> checkpoint because
         * (1 + 1) % 2 == 0
         */
        assertTrue(
                storage.exists(
                        TransactionLog.LAST_CHECKPOINT
                )
        );

        String contents =
                new String(
                        storage.read(
                                TransactionLog.LAST_CHECKPOINT
                        ),
                        StandardCharsets.UTF_8
                );

        assertTrue(
                contents.contains("1")
        );
    }

    @Test
    void checkpointContainsActiveFiles()
            throws Exception {

        LocalStorage storage =
                new LocalStorage(tempDir);

        TableSchema schema =
                schema();

        DeltaTable table =
                createTable(
                        storage,
                        schema,
                        100
                );

        table.appendRows(
                List.of(
                        row(schema, 1),
                        row(schema, 2)
                )
        );

        Snapshot before =
                table.snapshot();

        table.checkpoint();

        CheckpointManager manager =
                new CheckpointManager(
                        storage,
                        new TransactionLog(storage)
                );

        List<LogAction> actions =
                manager.load(0);

        long addFileCount =
                actions.stream()
                        .filter(
                                action ->
                                        action instanceof AddFile
                        )
                        .count();

        assertEquals(
                before.fileCount(),
                addFileCount
        );
    }

    @Test
    void checkpointContainsProtocolAndMetadata()
            throws Exception {

        LocalStorage storage =
                new LocalStorage(tempDir);

        TableSchema schema =
                schema();

        DeltaTable table =
                createTable(
                        storage,
                        schema,
                        100
                );

        table.appendRows(
                List.of(row(schema, 1))
        );

        table.checkpoint();

        CheckpointManager manager =
                new CheckpointManager(
                        storage,
                        new TransactionLog(storage)
                );

        List<LogAction> actions =
                manager.load(0);

        assertTrue(
                actions.stream()
                        .anyMatch(
                                action ->
                                        action instanceof
                                                com.delta.deltalake.log.Protocol
                        )
        );

        assertTrue(
                actions.stream()
                        .anyMatch(
                                action ->
                                        action instanceof
                                                com.delta.deltalake.log.Metadata
                        )
        );
    }

    @Test
    void checkpointCanBeLoadedBack()
            throws Exception {

        LocalStorage storage =
                new LocalStorage(tempDir);

        TableSchema schema =
                schema();

        DeltaTable table =
                createTable(
                        storage,
                        schema,
                        100
                );

        table.appendRows(
                List.of(
                        row(schema, 1),
                        row(schema, 2),
                        row(schema, 3)
                )
        );

        table.checkpoint();

        CheckpointManager manager =
                new CheckpointManager(
                        storage,
                        new TransactionLog(storage)
                );

        List<LogAction> actions =
                manager.load(0);

        assertFalse(
                actions.isEmpty()
        );
    }

    @Test
    void latestCheckpointAtOrBeforeFindsCheckpoint()
            throws Exception {

        LocalStorage storage =
                new LocalStorage(tempDir);

        TableSchema schema =
                schema();

        DeltaTable table =
                createTable(
                        storage,
                        schema,
                        100
                );

        for (int i = 0; i < 5; i++) {
            table.appendRows(
                    List.of(row(schema, i))
            );
        }

        /*
         * Current version = 4.
         */
        assertEquals(
                4,
                table.version()
        );

        table.checkpoint();

        CheckpointManager manager =
                new CheckpointManager(
                        storage,
                        new TransactionLog(storage)
                );

        assertEquals(
                4,
                manager.latestCheckpointAtOrBefore(4)
        );

        assertEquals(
                4,
                manager.latestCheckpointAtOrBefore(10)
        );

        assertEquals(
                -1,
                manager.latestCheckpointAtOrBefore(3)
        );
    }

    @Test
    void checkpointDoesNotChangeTableVersion()
            throws Exception {

        LocalStorage storage =
                new LocalStorage(tempDir);

        TableSchema schema =
                schema();

        DeltaTable table =
                createTable(
                        storage,
                        schema,
                        100
                );

        table.appendRows(
                List.of(row(schema, 1))
        );

        long before =
                table.version();

        table.checkpoint();

        assertEquals(
                before,
                table.version()
        );
    }

    @Test
    void checkpointDoesNotChangeTableData()
            throws Exception {

        LocalStorage storage =
                new LocalStorage(tempDir);

        TableSchema schema =
                schema();

        DeltaTable table =
                createTable(
                        storage,
                        schema,
                        100
                );

        table.appendRows(
                List.of(
                        row(schema, 1),
                        row(schema, 2),
                        row(schema, 3)
                )
        );

        List<Long> before =
                table.readRows()
                        .stream()
                        .map(
                                row ->
                                        ((Number) row.get("id"))
                                                .longValue()
                        )
                        .sorted()
                        .toList();

        table.checkpoint();

        List<Long> after =
                table.readRows()
                        .stream()
                        .map(
                                row ->
                                        ((Number) row.get("id"))
                                                .longValue()
                        )
                        .sorted()
                        .toList();

        assertEquals(
                before,
                after
        );
    }

    @Test
    void snapshotRemainsCorrectAfterCheckpoint()
            throws Exception {

        LocalStorage storage =
                new LocalStorage(tempDir);

        TableSchema schema =
                schema();

        DeltaTable table =
                createTable(
                        storage,
                        schema,
                        100
                );

        for (int i = 0; i < 5; i++) {
            table.appendRows(
                    List.of(
                            row(schema, i * 2L),
                            row(schema, i * 2L + 1)
                    )
            );
        }

        Snapshot before =
                table.snapshot();

        table.checkpoint();

        Snapshot after =
                table.snapshot();

        assertEquals(
                before.version(),
                after.version()
        );

        assertEquals(
                before.fileCount(),
                after.fileCount()
        );

        assertEquals(
                before.activeFiles()
                        .stream()
                        .map(AddFile::path)
                        .sorted()
                        .toList(),
                after.activeFiles()
                        .stream()
                        .map(AddFile::path)
                        .sorted()
                        .toList()
        );

        assertEquals(
                10,
                after.fileCount() * 2
        );
    }

    @Test
    void checkpointCanBeCreatedMultipleTimes()
            throws Exception {

        LocalStorage storage =
                new LocalStorage(tempDir);

        TableSchema schema =
                schema();

        DeltaTable table =
                createTable(
                        storage,
                        schema,
                        100
                );

        table.appendRows(
                List.of(row(schema, 1))
        );

        table.checkpoint();

        assertTrue(
                storage.exists(
                        "_delta_log/00000000000000000000.checkpoint.parquet"
                )
        );

        /*
         * Creating the same checkpoint again should
         * remain successful because CheckpointManager
         * handles an already-existing checkpoint.
         */
        table.checkpoint();

        assertTrue(
                storage.exists(
                        "_delta_log/00000000000000000000.checkpoint.parquet"
                )
        );
    }
}