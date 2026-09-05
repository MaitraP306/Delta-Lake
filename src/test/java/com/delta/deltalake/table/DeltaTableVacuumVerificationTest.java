package com.delta.deltalake.table;

import com.delta.deltalake.data.Row;
import com.delta.deltalake.data.TableSchema;
import com.delta.deltalake.log.AddFile;
import com.delta.deltalake.storage.LocalStorage;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DeltaTableVacuumVerificationTest {

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

    private DeltaTable createMultiFileTable(
            LocalStorage storage,
            TableSchema schema) throws Exception {

        DeltaTable table =
                DeltaTable.open(storage);

        for (int i = 0; i < 5; i++) {

            long id = i * 10L;

            table.appendRows(
                    List.of(
                            row(schema, id),
                            row(schema, id + 1)
                    )
            );
        }

        return table;
    }

    @Test
    void vacuumRejectsNegativeRetention()
            throws Exception {

        LocalStorage storage =
                new LocalStorage(tempDir);

        TableSchema schema =
                schema();

        DeltaTable table =
                DeltaTable.open(storage);

        table.appendRows(
                List.of(row(schema, 1))
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> table.vacuum(Duration.ofMillis(-1))
        );
    }

    @Test
    void vacuumRequiresExistingTable()
            throws Exception {

        LocalStorage storage =
                new LocalStorage(tempDir);

        DeltaTable table =
                DeltaTable.open(storage);

        assertFalse(table.exists());

        assertThrows(
                IllegalStateException.class,
                () -> table.vacuum(Duration.ZERO)
        );
    }

    @Test
    void defaultRetentionIsSevenDays()
            throws Exception {

        LocalStorage storage =
                new LocalStorage(tempDir);

        TableSchema schema =
                schema();

        DeltaTable table =
                DeltaTable.open(storage);

        table.appendRows(
                List.of(row(schema, 1))
        );

        assertEquals(
                Duration.ofDays(7),
                table.retention()
        );
    }

    @Test
    void vacuumDoesNothingWhenThereAreNoTombstones()
            throws Exception {

        LocalStorage storage =
                new LocalStorage(tempDir);

        TableSchema schema =
                schema();

        DeltaTable table =
                DeltaTable.open(storage);

        table.appendRows(
                List.of(row(schema, 1))
        );

        assertEquals(
                0,
                table.snapshot().tombstones().size()
        );

        int deleted =
                table.vacuum(Duration.ZERO);

        assertEquals(
                0,
                deleted
        );

        assertEquals(
                1,
                table.snapshot().fileCount()
        );

        assertEquals(
                1,
                table.readRows().size()
        );
    }

    @Test
    void vacuumDeletesOldTombstonedFiles()
            throws Exception {

        LocalStorage storage =
                new LocalStorage(tempDir);

        TableSchema schema =
                schema();

        DeltaTable table =
                createMultiFileTable(
                        storage,
                        schema
                );

        Snapshot beforeOptimize =
                table.snapshot();

        List<String> oldPaths =
                beforeOptimize.activeFiles()
                        .stream()
                        .map(AddFile::path)
                        .toList();

        assertEquals(
                5,
                oldPaths.size()
        );

        /*
         * OPTIMIZE logically removes the old files.
         */
        table.optimize();

        Snapshot afterOptimize =
                table.snapshot();

        for (String path : oldPaths) {
            assertTrue(
                    afterOptimize.tombstones()
                            .stream()
                            .anyMatch(
                                    tombstone ->
                                            tombstone.path()
                                                    .equals(path)
                            )
            );

            /*
             * Physical files still exist.
             */
            assertTrue(
                    Files.exists(
                            tempDir.resolve(path)
                    )
            );
        }

        /*
         * Zero retention makes every tombstone
         * whose deletion timestamp is <= now
         * eligible for vacuum.
         */
        int deleted =
                table.vacuum(Duration.ZERO);

        assertEquals(
                oldPaths.size(),
                deleted
        );

        /*
         * The physical files are now gone.
         */
        for (String path : oldPaths) {
            assertFalse(
                    Files.exists(
                            tempDir.resolve(path)
                    )
            );
        }
    }

    @Test
    void vacuumDoesNotDeleteActiveFiles()
            throws Exception {

        LocalStorage storage =
                new LocalStorage(tempDir);

        TableSchema schema =
                schema();

        DeltaTable table =
                DeltaTable.open(storage);

        table.appendRows(
                List.of(
                        row(schema, 1),
                        row(schema, 2)
                )
        );

        String activePath =
                table.snapshot()
                        .activeFiles()
                        .iterator()
                        .next()
                        .path();

        assertTrue(
                Files.exists(
                        tempDir.resolve(activePath)
                )
        );

        int deleted =
                table.vacuum(Duration.ZERO);

        assertEquals(
                0,
                deleted
        );

        assertTrue(
                Files.exists(
                        tempDir.resolve(activePath)
                )
        );
    }

    @Test
    void vacuumDoesNotDeleteRecentlyDeletedFileWithLongRetention()
            throws Exception {

        LocalStorage storage =
                new LocalStorage(tempDir);

        TableSchema schema =
                schema();

        DeltaTable table =
                createMultiFileTable(
                        storage,
                        schema
                );

        List<String> oldPaths =
                table.snapshot()
                        .activeFiles()
                        .stream()
                        .map(AddFile::path)
                        .toList();

        table.optimize();

        /*
         * A one-day retention window should protect
         * tombstones created just now.
         */
        int deleted =
                table.vacuum(Duration.ofDays(1));

        assertEquals(
                0,
                deleted
        );

        for (String path : oldPaths) {
            assertTrue(
                    Files.exists(
                            tempDir.resolve(path)
                    )
            );
        }
    }

    @Test
    void vacuumKeepsTombstonesInSnapshot()
            throws Exception {

        LocalStorage storage =
                new LocalStorage(tempDir);

        TableSchema schema =
                schema();

        DeltaTable table =
                createMultiFileTable(
                        storage,
                        schema
                );

        List<String> oldPaths =
                table.snapshot()
                        .activeFiles()
                        .stream()
                        .map(AddFile::path)
                        .toList();

        table.optimize();

        int tombstonesBefore =
                table.snapshot()
                        .tombstones()
                        .size();

        assertEquals(
                oldPaths.size(),
                tombstonesBefore
        );

        table.vacuum(Duration.ZERO);

        /*
         * VACUUM physically deletes the data files.
         * It does not remove the RemoveFile actions
         * from the transaction log.
         */
        int tombstonesAfter =
                table.snapshot()
                        .tombstones()
                        .size();

        assertEquals(
                tombstonesBefore,
                tombstonesAfter
        );
    }

    @Test
    void vacuumPreservesCurrentTableData()
            throws Exception {

        LocalStorage storage =
                new LocalStorage(tempDir);

        TableSchema schema =
                schema();

        DeltaTable table =
                createMultiFileTable(
                        storage,
                        schema
                );

        List<Row> before =
                table.readRows();

        table.optimize();

        table.vacuum(Duration.ZERO);

        List<Row> after =
                table.readRows();

        assertEquals(
                before.size(),
                after.size()
        );

        assertEquals(
                before.stream()
                        .map(row -> row.get("id"))
                        .sorted()
                        .toList(),
                after.stream()
                        .map(row -> row.get("id"))
                        .sorted()
                        .toList()
        );
    }

    @Test
    void setRetentionChangesConfiguredRetention()
            throws Exception {

        LocalStorage storage =
                new LocalStorage(tempDir);

        TableSchema schema =
                schema();

        DeltaTable table =
                DeltaTable.open(storage);

        table.appendRows(
                List.of(row(schema, 1))
        );

        assertEquals(
                Duration.ofDays(7),
                table.retention()
        );

        long version =
                table.setRetention(
                        Duration.ofHours(12)
                );

        assertEquals(
                1,
                version
        );

        assertEquals(
                Duration.ofHours(12),
                table.retention()
        );

        assertEquals(
                "43200000",
                table.snapshot()
                        .metadata()
                        .configuration()
                        .get(
                                "delta.deletedFileRetentionMillis"
                        )
        );
    }

    @Test
    void setRetentionRejectsNegativeDuration()
            throws Exception {

        LocalStorage storage =
                new LocalStorage(tempDir);

        TableSchema schema =
                schema();

        DeltaTable table =
                DeltaTable.open(storage);

        table.appendRows(
                List.of(row(schema, 1))
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> table.setRetention(
                        Duration.ofMillis(-1)
                )
        );
    }

    @Test
    void configuredRetentionIsUsedByVacuum()
            throws Exception {

        LocalStorage storage =
                new LocalStorage(tempDir);

        TableSchema schema =
                schema();

        DeltaTable table =
                createMultiFileTable(
                        storage,
                        schema
                );

        /*
         * Set a long retention period.
         */
        table.setRetention(
                Duration.ofDays(10)
        );

        List<String> oldPaths =
                table.snapshot()
                        .activeFiles()
                        .stream()
                        .map(AddFile::path)
                        .toList();

        table.optimize();

        /*
         * vacuum() without an argument uses
         * table.retention().
         */
        int deleted =
                table.vacuum();

        assertEquals(
                0,
                deleted
        );

        for (String path : oldPaths) {
            assertTrue(
                    Files.exists(
                            tempDir.resolve(path)
                    )
            );
        }
    }

    @Test
    void nonexistentTableUsesDefaultRetention()
            throws Exception {

        LocalStorage storage =
                new LocalStorage(tempDir);

        DeltaTable table =
                DeltaTable.open(storage);

        assertFalse(table.exists());

        assertEquals(
                Duration.ofDays(7),
                table.retention()
        );
    }
}