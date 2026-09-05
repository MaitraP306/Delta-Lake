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
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DeltaTableMutationVerificationTest {

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
                      "name": "name",
                      "type": "string"
                    },
                    {
                      "name": "age",
                      "type": "int"
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
            long id,
            String name,
            int age) {

        return Row.of(
                schema,
                Map.of(
                        "id", id,
                        "name", name,
                        "age", age
                )
        );
    }

    private DeltaTable createTable(
            LocalStorage storage,
            TableSchema schema) throws Exception {

        DeltaTable table = DeltaTable.open(storage);

        table.appendRows(
                List.of(
                        row(schema, 1L, "Alice", 20),
                        row(schema, 2L, "Bob", 25),
                        row(schema, 3L, "Charlie", 30)
                )
        );

        return table;
    }

    // -------------------------------------------------------------------------
    // DELETE
    // -------------------------------------------------------------------------

    @Test
    void deleteRemovesMatchingRows() throws Exception {
        LocalStorage storage = new LocalStorage(tempDir);

        TableSchema schema = schema();

        DeltaTable table = createTable(storage, schema);

        assertEquals(3, table.readRows().size());
        assertEquals(0, table.version());

        table.deleteRows(
                row -> ((Number) row.get("id")).longValue() == 2L
        );

        assertEquals(1, table.version());

        List<Row> rows = table.readRows();

        assertEquals(2, rows.size());

        assertEquals(1L, rows.get(0).get("id"));
        assertEquals(3L, rows.get(1).get("id"));
    }

    @Test
    void deleteLeavesNonMatchingRowsUntouched() throws Exception {
        LocalStorage storage = new LocalStorage(tempDir);

        TableSchema schema = schema();

        DeltaTable table = createTable(storage, schema);

        table.deleteRows(
                row -> ((Number) row.get("id")).longValue() == 999L
        );

        /*
         * No row matched, so there should be no new version.
         */
        assertEquals(0, table.version());

        List<Row> rows = table.readRows();

        assertEquals(3, rows.size());
    }

    @Test
    void deleteProducesNewSnapshotWithoutDeletedFile() throws Exception {
        LocalStorage storage = new LocalStorage(tempDir);

        TableSchema schema = schema();

        DeltaTable table = createTable(storage, schema);

        Snapshot before = table.snapshot();

        assertEquals(1, before.fileCount());

        AddFile originalFile =
                before.activeFiles()
                        .iterator()
                        .next();

        String originalPath = originalFile.path();

        table.deleteRows(
                row -> ((Number) row.get("id")).longValue() == 2L
        );

        Snapshot after = table.snapshot();

        assertEquals(1, after.fileCount());

        /*
         * The original physical file should no longer
         * be an active file in the new snapshot.
         */
        assertFalse(
                after.activeFiles()
                        .stream()
                        .anyMatch(
                                file -> file.path().equals(originalPath)
                        )
        );
    }

    @Test
    void deleteCreatesTombstoneForRemovedFile() throws Exception {
        LocalStorage storage = new LocalStorage(tempDir);

        TableSchema schema = schema();

        DeltaTable table = createTable(storage, schema);

        Snapshot before = table.snapshot();

        AddFile originalFile =
                before.activeFiles()
                        .iterator()
                        .next();

        String originalPath = originalFile.path();

        table.deleteRows(
                row -> ((Number) row.get("id")).longValue() == 2L
        );

        Snapshot after = table.snapshot();

        assertTrue(
                after.tombstones()
                        .stream()
                        .anyMatch(
                                remove ->
                                        remove.path().equals(originalPath)
                        )
        );
    }

    // -------------------------------------------------------------------------
    // UPSERT
    // -------------------------------------------------------------------------

    @Test
    void upsertInsertsNewRows() throws Exception {
        LocalStorage storage = new LocalStorage(tempDir);

        TableSchema schema = schema();

        DeltaTable table = createTable(storage, schema);

        table.upsertRows(
                List.of(
                        row(schema, 4L, "David", 35)
                ),
                "id"
        );

        List<Row> rows = table.readRows();

        assertEquals(4, rows.size());

        assertTrue(
                rows.stream()
                        .anyMatch(
                                r -> ((Number) r.get("id")).longValue() == 4L
                        )
        );
    }

    @Test
    void upsertUpdatesExistingRow() throws Exception {
        LocalStorage storage = new LocalStorage(tempDir);

        TableSchema schema = schema();

        DeltaTable table = createTable(storage, schema);

        table.upsertRows(
                List.of(
                        row(schema, 2L, "Bob Updated", 99)
                ),
                "id"
        );

        List<Row> rows = table.readRows();

        assertEquals(3, rows.size());

        Row updated = rows.stream()
                .filter(
                        r -> ((Number) r.get("id")).longValue() == 2L
                )
                .findFirst()
                .orElseThrow();

        assertEquals(
                "Bob Updated",
                updated.get("name")
        );

        assertEquals(
                99,
                updated.get("age")
        );
    }

    @Test
    void upsertDoesNotDuplicateExistingKey() throws Exception {
        LocalStorage storage = new LocalStorage(tempDir);

        TableSchema schema = schema();

        DeltaTable table = createTable(storage, schema);

        table.upsertRows(
                List.of(
                        row(schema, 2L, "Bob Updated", 99)
                ),
                "id"
        );

        List<Row> rows = table.readRows();

        long count =
                rows.stream()
                        .filter(
                                r -> ((Number) r.get("id")).longValue() == 2L
                        )
                        .count();

        assertEquals(1, count);
    }

    @Test
    void upsertCanUpdateAndInsertInOneOperation()
            throws Exception {

        LocalStorage storage = new LocalStorage(tempDir);

        TableSchema schema = schema();

        DeltaTable table = createTable(storage, schema);

        table.upsertRows(
                List.of(
                        row(schema, 2L, "Bob Updated", 99),
                        row(schema, 4L, "David", 35)
                ),
                "id"
        );

        List<Row> rows = table.readRows();

        assertEquals(4, rows.size());

        Row updated =
                rows.stream()
                        .filter(
                                r -> ((Number) r.get("id")).longValue() == 2L
                        )
                        .findFirst()
                        .orElseThrow();

        assertEquals(
                "Bob Updated",
                updated.get("name")
        );

        Row inserted =
                rows.stream()
                        .filter(
                                r -> ((Number) r.get("id")).longValue() == 4L
                        )
                        .findFirst()
                        .orElseThrow();

        assertEquals(
                "David",
                inserted.get("name")
        );
    }

    // -------------------------------------------------------------------------
    // MERGE
    // -------------------------------------------------------------------------

    @Test
    void mergeUpdatesMatchingRows() throws Exception {
        LocalStorage storage = new LocalStorage(tempDir);

        TableSchema schema = schema();

        DeltaTable table = createTable(storage, schema);

        table.mergeRows(
                List.of(
                        row(schema, 2L, "Bob Merged", 100)
                ),
                "id"
        );

        List<Row> rows = table.readRows();

        assertEquals(3, rows.size());

        Row updated =
                rows.stream()
                        .filter(
                                r -> ((Number) r.get("id")).longValue() == 2L
                        )
                        .findFirst()
                        .orElseThrow();

        assertEquals(
                "Bob Merged",
                updated.get("name")
        );

        assertEquals(
                100,
                updated.get("age")
        );
    }

    @Test
    void mergeInsertsNonMatchingRows() throws Exception {
        LocalStorage storage = new LocalStorage(tempDir);

        TableSchema schema = schema();

        DeltaTable table = createTable(storage, schema);

        table.mergeRows(
                List.of(
                        row(schema, 4L, "David", 35)
                ),
                "id"
        );

        List<Row> rows = table.readRows();

        assertEquals(4, rows.size());

        assertTrue(
                rows.stream()
                        .anyMatch(
                                r -> ((Number) r.get("id")).longValue() == 4L
                        )
        );
    }

    @Test
    void mergeUpdatesAndInsertsInOneOperation()
            throws Exception {

        LocalStorage storage = new LocalStorage(tempDir);

        TableSchema schema = schema();

        DeltaTable table = createTable(storage, schema);

        table.mergeRows(
                List.of(
                        row(schema, 2L, "Bob Merged", 100),
                        row(schema, 4L, "David", 35)
                ),
                "id"
        );

        List<Row> rows = table.readRows();

        assertEquals(4, rows.size());

        Row updated =
                rows.stream()
                        .filter(
                                r -> ((Number) r.get("id")).longValue() == 2L
                        )
                        .findFirst()
                        .orElseThrow();

        assertEquals(
                "Bob Merged",
                updated.get("name")
        );

        Row inserted =
                rows.stream()
                        .filter(
                                r -> ((Number) r.get("id")).longValue() == 4L
                        )
                        .findFirst()
                        .orElseThrow();

        assertEquals(
                "David",
                inserted.get("name")
        );
    }

    // -------------------------------------------------------------------------
    // VERSION / SNAPSHOT BEHAVIOR
    // -------------------------------------------------------------------------

    @Test
    void mutationCreatesNewDeltaVersion() throws Exception {
        LocalStorage storage = new LocalStorage(tempDir);

        TableSchema schema = schema();

        DeltaTable table = createTable(storage, schema);

        assertEquals(0, table.version());

        table.deleteRows(
                row -> ((Number) row.get("id")).longValue() == 1L
        );

        assertEquals(1, table.version());

        table.upsertRows(
                List.of(
                        row(schema, 4L, "David", 35)
                ),
                "id"
        );

        assertEquals(2, table.version());

        assertEquals(3, table.readRows().size());
    }

    @Test
    void historicalSnapshotRemainsReadableAfterMutation()
            throws Exception {

        LocalStorage storage = new LocalStorage(tempDir);

        TableSchema schema = schema();

        DeltaTable table = createTable(storage, schema);

        Snapshot version0 =
                table.snapshot(0);

        assertEquals(3, table.readRows(0).size());

        table.deleteRows(
                row -> ((Number) row.get("id")).longValue() == 2L
        );

        assertEquals(1, table.version());

        /*
         * Current snapshot has two rows.
         */
        assertEquals(2, table.readRows().size());

        /*
         * Historical version still has all three rows.
         */
        List<Row> historical =
                table.readRows(version0.version());

        assertEquals(3, historical.size());

        assertTrue(
                historical.stream()
                        .anyMatch(
                                r -> ((Number) r.get("id")).longValue() == 2L
                        )
        );
    }

    @Test
    void mutationProducesRemoveAndAddActions()
            throws Exception {

        LocalStorage storage = new LocalStorage(tempDir);

        TableSchema schema = schema();

        DeltaTable table = createTable(storage, schema);

        Snapshot before =
                table.snapshot();

        assertEquals(1, before.fileCount());

        String originalPath =
                before.activeFiles()
                        .iterator()
                        .next()
                        .path();

        table.deleteRows(
                row -> ((Number) row.get("id")).longValue() == 2L
        );

        /*
         * The new snapshot should have a replacement file.
         */
        Snapshot after =
                table.snapshot();

        assertEquals(1, after.fileCount());

        String replacementPath =
                after.activeFiles()
                        .iterator()
                        .next()
                        .path();

        assertNotEquals(
                originalPath,
                replacementPath
        );

        /*
         * The old file must be a tombstone.
         */
        assertTrue(
                after.tombstones()
                        .stream()
                        .anyMatch(
                                remove ->
                                        remove.path()
                                                .equals(originalPath)
                        )
        );

        /*
         * Both physical files can still exist because
         * Delta does not immediately delete old files.
         */
        assertTrue(
                Files.exists(
                        tempDir.resolve(originalPath)
                )
        );

        assertTrue(
                Files.exists(
                        tempDir.resolve(replacementPath)
                )
        );
    }
}