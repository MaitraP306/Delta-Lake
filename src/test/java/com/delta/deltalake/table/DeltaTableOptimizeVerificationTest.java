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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DeltaTableOptimizeVerificationTest {

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

    private DeltaTable createMultiFileTable(
            LocalStorage storage,
            TableSchema schema) throws Exception {

        DeltaTable table =
                DeltaTable.open(storage);

        /*
         * Each append creates a separate active file.
         */
        for (int i = 0; i < 5; i++) {

            long base = i * 10L + 1;

            table.appendRows(
                    List.of(
                            row(
                                    schema,
                                    base,
                                    "row-" + base,
                                    (int) base
                            ),
                            row(
                                    schema,
                                    base + 1,
                                    "row-" + (base + 1),
                                    (int) (base + 1)
                            )
                    )
            );
        }

        return table;
    }

    @Test
    void optimizeRejectsInvalidTargetSize()
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

        assertThrows(
                IllegalArgumentException.class,
                () -> table.optimize(0)
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> table.optimize(-1)
        );
    }

    @Test
    void optimizeDoesNothingForSingleFileTable()
            throws Exception {

        LocalStorage storage =
                new LocalStorage(tempDir);

        TableSchema schema =
                schema();

        DeltaTable table =
                DeltaTable.open(storage);

        table.appendRows(
                List.of(
                        row(schema, 1L, "Alice", 20),
                        row(schema, 2L, "Bob", 25)
                )
        );

        assertEquals(
                1,
                table.snapshot().fileCount()
        );

        long before =
                table.version();

        long result =
                table.optimize();

        /*
         * Implementation explicitly returns the
         * current version when there is <= 1 file.
         */
        assertEquals(
                before,
                result
        );

        assertEquals(
                before,
                table.version()
        );

        assertEquals(
                1,
                table.snapshot().fileCount()
        );
    }

    @Test
    void optimizeCreatesNewVersion()
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

        assertEquals(
                4,
                table.version()
        );

        assertEquals(
                5,
                table.snapshot().fileCount()
        );

        long optimizedVersion =
                table.optimize();

        assertEquals(
                5,
                optimizedVersion
        );

        assertEquals(
                5,
                table.version()
        );
    }

    @Test
    void optimizeReducesNumberOfFiles()
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

        Snapshot before =
                table.snapshot();

        assertEquals(
                5,
                before.fileCount()
        );

        table.optimize();

        Snapshot after =
                table.snapshot();

        /*
         * Default target size is large enough that
         * these small files should be compacted.
         */
        assertTrue(
                after.fileCount() < before.fileCount()
        );
    }

    @Test
    void optimizePreservesAllRows()
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

        List<Long> beforeIds =
                table.readRows()
                        .stream()
                        .map(row ->
                                ((Number) row.get("id"))
                                        .longValue()
                        )
                        .sorted()
                        .toList();

        assertEquals(
                10,
                beforeIds.size()
        );

        table.optimize();

        List<Long> afterIds =
                table.readRows()
                        .stream()
                        .map(row ->
                                ((Number) row.get("id"))
                                        .longValue()
                        )
                        .sorted()
                        .toList();

        assertEquals(
                beforeIds,
                afterIds
        );
    }

    @Test
    void optimizePreservesRowValues()
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

        List<Row> after =
                table.readRows();

        assertEquals(
                before.size(),
                after.size()
        );

        Map<Long, Row> beforeById =
                before.stream()
                        .collect(
                                java.util.stream.Collectors.toMap(
                                        row ->
                                                ((Number) row.get("id"))
                                                        .longValue(),
                                        row -> row
                                )
                        );

        Map<Long, Row> afterById =
                after.stream()
                        .collect(
                                java.util.stream.Collectors.toMap(
                                        row ->
                                                ((Number) row.get("id"))
                                                        .longValue(),
                                        row -> row
                                )
                        );

        assertEquals(
                beforeById.keySet(),
                afterById.keySet()
        );

        for (Long id : beforeById.keySet()) {

            Row oldRow =
                    beforeById.get(id);

            Row newRow =
                    afterById.get(id);

            assertEquals(
                    oldRow.get("name"),
                    newRow.get("name")
            );

            assertEquals(
                    oldRow.get("age"),
                    newRow.get("age")
            );
        }
    }

    @Test
    void optimizeMakesOldFilesInactive()
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

        Snapshot before =
                table.snapshot();

        List<String> oldPaths =
                before.activeFiles()
                        .stream()
                        .map(AddFile::path)
                        .toList();

        table.optimize();

        Snapshot after =
                table.snapshot();

        List<String> newPaths =
                after.activeFiles()
                        .stream()
                        .map(AddFile::path)
                        .toList();

        for (String oldPath : oldPaths) {
            assertFalse(
                    newPaths.contains(oldPath)
            );
        }
    }

    @Test
    void optimizeCreatesTombstonesForOldFiles()
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

        Snapshot before =
                table.snapshot();

        List<String> oldPaths =
                before.activeFiles()
                        .stream()
                        .map(AddFile::path)
                        .toList();

        table.optimize();

        Snapshot after =
                table.snapshot();

        for (String oldPath : oldPaths) {

            assertTrue(
                    after.tombstones()
                            .stream()
                            .anyMatch(
                                    remove ->
                                            remove.path()
                                                    .equals(oldPath)
                            )
            );
        }
    }

    @Test
    void optimizeKeepsOldPhysicalFiles()
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
         * Optimize creates RemoveFile actions.
         * It does not physically delete the old files.
         */
        for (String path : oldPaths) {

            assertTrue(
                    Files.exists(
                            tempDir.resolve(path)
                    )
            );
        }
    }

    @Test
    void optimizeNewFilesHaveDataChangeFalse()
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

        table.optimize();

        Snapshot snapshot =
                table.snapshot();

        assertFalse(
                snapshot.activeFiles()
                        .isEmpty()
        );

        for (AddFile file :
                snapshot.activeFiles()) {

            assertFalse(
                    file.dataChange()
            );
        }
    }

    @Test
    void optimizeDoesNotChangeQueryResults()
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

        List<Long> before =
                table.queryRows(
                        Map.of(
                                "id",
                                new DeltaTable.QueryRange(
                                        12L,
                                        14L
                                )
                        )
                )
                .stream()
                .map(row ->
                        ((Number) row.get("id"))
                                .longValue()
                )
                .sorted()
                .toList();

        table.optimize();

        List<Long> after =
                table.queryRows(
                        Map.of(
                                "id",
                                new DeltaTable.QueryRange(
                                        12L,
                                        14L
                                )
                        )
                )
                .stream()
                .map(row ->
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
    void optimizeCanCreateMultipleTargetFiles()
            throws Exception {

        LocalStorage storage =
                new LocalStorage(tempDir);

        TableSchema schema =
                schema();

        DeltaTable table =
                DeltaTable.open(storage);

        /*
         * Create several reasonably sized files.
         */
        for (int i = 0; i < 10; i++) {

            List<Row> rows =
                    new ArrayList<>();

            for (int j = 0; j < 10; j++) {

                long id =
                        i * 10L + j;

                rows.add(
                        row(
                                schema,
                                id,
                                "row-" + id,
                                (int) id
                        )
                );
            }

            table.appendRows(rows);
        }

        assertEquals(
                10,
                table.snapshot().fileCount()
        );

        /*
         * Very small target means optimize should
         * split the combined data into multiple
         * output files.
         */
        table.optimize(1);

        assertTrue(
                table.snapshot().fileCount() > 1
        );

        assertEquals(
                100,
                table.readRows().size()
        );
    }
}