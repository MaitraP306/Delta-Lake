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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DeltaTableZOrderVerificationTest {

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
                      "name": "x",
                      "type": "int"
                    },
                    {
                      "name": "y",
                      "type": "int"
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
            long id,
            int x,
            int y) {

        return Row.of(
                schema,
                Map.of(
                        "id", id,
                        "x", x,
                        "y", y,
                        "value", "row-" + id
                )
        );
    }

    private DeltaTable createMultiFileTable(
            LocalStorage storage,
            TableSchema schema) throws Exception {

        DeltaTable table =
                DeltaTable.open(storage);

        /*
         * Five independent appends -> five active files.
         */
        for (int i = 0; i < 5; i++) {
            table.appendRows(
                    List.of(
                            row(
                                    schema,
                                    i * 2L,
                                    i,
                                    4 - i
                            ),
                            row(
                                    schema,
                                    i * 2L + 1,
                                    i,
                                    i
                            )
                    )
            );
        }

        return table;
    }

    @Test
    void zOrderRejectsNoColumns()
            throws Exception {

        LocalStorage storage =
                new LocalStorage(tempDir);

        TableSchema schema =
                schema();

        DeltaTable table =
                DeltaTable.open(storage);

        table.appendRows(
                List.of(
                        row(schema, 1, 1, 1)
                )
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> table.optimizeZOrder()
        );
    }

    @Test
    void zOrderRejectsUnknownColumn()
            throws Exception {

        LocalStorage storage =
                new LocalStorage(tempDir);

        TableSchema schema =
                schema();

        DeltaTable table =
                DeltaTable.open(storage);

        table.appendRows(
                List.of(
                        row(schema, 1, 1, 1)
                )
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> table.optimizeZOrder("does_not_exist")
        );
    }

    @Test
    void zOrderRejectsDuplicateColumn()
            throws Exception {

        LocalStorage storage =
                new LocalStorage(tempDir);

        TableSchema schema =
                schema();

        DeltaTable table =
                DeltaTable.open(storage);

        table.appendRows(
                List.of(
                        row(schema, 1, 1, 1)
                )
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> table.optimizeZOrder("x", "x")
        );
    }

    @Test
    void zOrderOnNonexistentTableThrows()
            throws Exception {

        LocalStorage storage =
                new LocalStorage(tempDir);

        DeltaTable table =
                DeltaTable.open(storage);

        assertFalse(table.exists());

        assertThrows(
                IllegalStateException.class,
                () -> table.optimizeZOrder("x")
        );

        assertFalse(table.exists());
    }

    @Test
    void zOrderCreatesNewVersion()
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

        long result =
                table.optimizeZOrder("x", "y");

        assertEquals(
                5,
                result
        );

        assertEquals(
                5,
                table.version()
        );
    }

    @Test
    void zOrderPreservesAllRows()
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

        List<String> before =
                table.readRows()
                        .stream()
                        .map(this::rowIdentity)
                        .sorted()
                        .toList();

        table.optimizeZOrder("x", "y");

        List<String> after =
                table.readRows()
                        .stream()
                        .map(this::rowIdentity)
                        .sorted()
                        .toList();

        assertEquals(
                before,
                after
        );

        assertEquals(
                10,
                after.size()
        );
    }

    @Test
    void zOrderPreservesValues()
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

        Map<Long, String> before =
                new HashMap<>();

        for (Row row : table.readRows()) {
            before.put(
                    ((Number) row.get("id")).longValue(),
                    rowIdentity(row)
            );
        }

        table.optimizeZOrder("x", "y");

        Map<Long, String> after =
                new HashMap<>();

        for (Row row : table.readRows()) {
            after.put(
                    ((Number) row.get("id")).longValue(),
                    rowIdentity(row)
            );
        }

        assertEquals(
                before,
                after
        );
    }

    @Test
    void zOrderMakesOldFilesInactive()
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

        table.optimizeZOrder("x", "y");

        List<String> newPaths =
                table.snapshot()
                        .activeFiles()
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
    void zOrderCreatesTombstonesForOldFiles()
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

        table.optimizeZOrder("x", "y");

        for (String oldPath : oldPaths) {

            assertTrue(
                    table.snapshot()
                            .tombstones()
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
    void zOrderDoesNotPhysicallyDeleteOldFiles()
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

        table.optimizeZOrder("x", "y");

        for (String path : oldPaths) {
            assertTrue(
                    Files.exists(
                            tempDir.resolve(path)
                    )
            );
        }
    }

    @Test
    void zOrderOutputFilesHaveDataChangeFalse()
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

        table.optimizeZOrder("x", "y");

        assertFalse(
                table.snapshot()
                        .activeFiles()
                        .isEmpty()
        );

        for (AddFile file :
                table.snapshot().activeFiles()) {

            assertFalse(
                    file.dataChange()
            );
        }
    }

    @Test
    void zOrderPreservesQueryResults()
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

        Map<String, DeltaTable.QueryRange> predicate =
                Map.of(
                        "x",
                        new DeltaTable.QueryRange(1, 3),
                        "y",
                        new DeltaTable.QueryRange(1, 3)
                );

        List<String> before =
                table.queryRows(predicate)
                        .stream()
                        .map(this::rowIdentity)
                        .sorted()
                        .toList();

        table.optimizeZOrder(
                predicate,
                "x",
                "y"
        );

        List<String> after =
                table.queryRows(predicate)
                        .stream()
                        .map(this::rowIdentity)
                        .sorted()
                        .toList();

        assertEquals(
                before,
                after
        );
    }

    @Test
    void zOrderCanUseSingleColumn()
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

        List<String> before =
                table.readRows()
                        .stream()
                        .map(this::rowIdentity)
                        .sorted()
                        .toList();

        table.optimizeZOrder("x");

        List<String> after =
                table.readRows()
                        .stream()
                        .map(this::rowIdentity)
                        .sorted()
                        .toList();

        assertEquals(
                before,
                after
        );
    }

    @Test
    void zOrderSupportsMultipleDimensions()
            throws Exception {

        LocalStorage storage =
                new LocalStorage(tempDir);

        TableSchema schema =
                schema();

        DeltaTable table =
                DeltaTable.open(storage);

        List<Row> rows =
                new ArrayList<>();

        int id = 0;

        /*
         * Create a two-dimensional grid.
         */
        for (int x = 0; x < 4; x++) {
            for (int y = 0; y < 4; y++) {
                rows.add(
                        row(
                                schema,
                                id++,
                                x,
                                y
                        )
                );
            }
        }

        /*
         * Put the rows into multiple physical files.
         */
        for (int i = 0; i < rows.size(); i += 4) {

            table.appendRows(
                    rows.subList(
                            i,
                            Math.min(
                                    rows.size(),
                                    i + 4
                            )
                    )
            );
        }

        assertEquals(
                4,
                table.snapshot().fileCount()
        );

        table.optimizeZOrder("x", "y");

        assertEquals(
                16,
                table.readRows().size()
        );
    }

    @Test
    void scopedZOrderOnlyRewritesMatchingFiles()
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

        /*
         * x=0..4 exists in the five files.
         *
         * Scope x=[0,1] should identify files whose
         * statistics overlap that range.
         */
        Map<String, DeltaTable.QueryRange> scope =
                Map.of(
                        "x",
                        new DeltaTable.QueryRange(0, 1)
                );

        List<String> affected =
                before.activeFiles()
                        .stream()
                        .filter(
                                file ->
                                        file.stats() != null
                                                && file.stats()
                                                .columns()
                                                .get("x") != null
                                                && file.stats()
                                                .columns()
                                                .get("x")
                                                .min() != null
                                                && file.stats()
                                                .columns()
                                                .get("x")
                                                .max() != null
                                                && compare(
                                                        file.stats()
                                                                .columns()
                                                                .get("x")
                                                                .max(),
                                                        0
                                                ) >= 0
                                                && compare(
                                                        file.stats()
                                                                .columns()
                                                                .get("x")
                                                                .min(),
                                                        1
                                                ) <= 0
                        )
                        .map(AddFile::path)
                        .toList();

        table.optimizeZOrder(
                scope,
                "x",
                "y"
        );

        Snapshot after =
                table.snapshot();

        /*
         * Files selected by the scope should no longer
         * be active.
         */
        for (String path : affected) {
            assertFalse(
                    after.activeFiles()
                            .stream()
                            .anyMatch(
                                    file ->
                                            file.path()
                                                    .equals(path)
                            )
            );
        }

        /*
         * Files outside the scope should remain active.
         */
        for (AddFile file : before.activeFiles()) {

            if (!affected.contains(file.path())) {

                assertTrue(
                        after.activeFiles()
                                .stream()
                                .anyMatch(
                                        current ->
                                                current.path()
                                                        .equals(file.path())
                                )
                );
            }
        }
    }

    private String rowIdentity(Row row) {
        return row.get("id") + "|"
                + row.get("x") + "|"
                + row.get("y") + "|"
                + row.get("value");
    }

    private static int compare(
            Object left,
            Object right) {

        if (left instanceof Number l
                && right instanceof Number r) {

            return new java.math.BigDecimal(
                    l.toString()
            ).compareTo(
                    new java.math.BigDecimal(
                            r.toString()
                    )
            );
        }

        return ((Comparable<Object>) left)
                .compareTo(right);
    }
}