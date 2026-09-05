package com.delta.deltalake.table;

import com.delta.deltalake.data.Row;
import com.delta.deltalake.data.TableSchema;
import com.delta.deltalake.log.VersionedLogRecord;
import com.delta.deltalake.storage.LocalStorage;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class DeltaTableArchitectureVerificationTest {

    // -------------------------------------------------------------------------
    // Complete architecture lifecycle
    // -------------------------------------------------------------------------

    @Test
    void completeTableLifecyclePreservesDeltaSemantics() throws Exception {
        Path root = Files.createTempDirectory("delta-architecture-");

        try {
            DeltaTable table = DeltaTable.open(
                    new LocalStorage(root),
                    2
            );

            TableSchema original = schema();

            // -----------------------------------------------------------------
            // 1. Initial write
            // -----------------------------------------------------------------

            long v0 = table.appendRows(List.of(
                    row(original, 1L, "Alice", 25),
                    row(original, 2L, "Bob", 30),
                    row(original, 3L, "Carol", 35)
            ));

            assertEquals(0, v0);
            assertEquals(0, table.version());

            assertEquals(
                    Set.of(1L, 2L, 3L),
                    ids(table.readRows())
            );

            // -----------------------------------------------------------------
            // 2. Append
            // -----------------------------------------------------------------

            long v1 = table.appendRows(List.of(
                    row(original, 4L, "David", 40),
                    row(original, 5L, "Eve", 45)
            ));

            assertEquals(1, v1);

            assertEquals(
                    Set.of(1L, 2L, 3L, 4L, 5L),
                    ids(table.readRows())
            );

            // -----------------------------------------------------------------
            // 3. Data skipping
            // -----------------------------------------------------------------

            List<Row> ageRows = table.queryRows(
                    Map.of(
                            "age",
                            new DeltaTable.QueryRange(40, 45)
                    )
            );

            assertEquals(
                    Set.of(4L, 5L),
                    ids(ageRows)
            );

            // -----------------------------------------------------------------
            // 4. Delete
            // -----------------------------------------------------------------

            long v2 = table.deleteRows(
                    row -> ((Number) row.get("id")).longValue() == 2L
            );

            assertEquals(2, v2);

            assertEquals(
                    Set.of(1L, 3L, 4L, 5L),
                    ids(table.readRows())
            );

            // -----------------------------------------------------------------
            // 5. Upsert
            // -----------------------------------------------------------------

            long v3 = table.upsertRows(
                    List.of(
                            row(original, 3L, "Carol-updated", 36),
                            row(original, 6L, "Frank", 50)
                    ),
                    "id"
            );

            assertEquals(3, v3);

            List<Row> afterUpsert = table.readRows();

            assertEquals(
                    Set.of(1L, 3L, 4L, 5L, 6L),
                    ids(afterUpsert)
            );

            assertEquals(
                    "Carol-updated",
                    findById(afterUpsert, 3L).get("name")
            );

            // -----------------------------------------------------------------
            // 6. Time travel
            // -----------------------------------------------------------------

            Snapshot historical = table.snapshot(1);

            assertEquals(1, historical.version());

            List<Row> historicalRows = table.readRows(1);

            assertEquals(
                    Set.of(1L, 2L, 3L, 4L, 5L),
                    ids(historicalRows)
            );

            // -----------------------------------------------------------------
            // 7. Schema evolution
            // -----------------------------------------------------------------

            TableSchema evolved = schemaWithCity();

            long evolutionVersion = table.evolveSchema(
                    evolved,
                    "architecture verification"
            );

            assertEquals(4, evolutionVersion);

            assertEquals(
                    evolved.json(),
                    table.currentSchema().json()
            );

            // Existing rows must expose the new nullable field as null.

            List<Row> afterEvolution = table.readRows();

            assertEquals(
                    Set.of(1L, 3L, 4L, 5L, 6L),
                    ids(afterEvolution)
            );

            for (Row row : afterEvolution) {
                assertNull(row.get("city"));
            }

            // -----------------------------------------------------------------
            // 8. Append using evolved schema
            // -----------------------------------------------------------------

            long v5 = table.appendRows(
                    List.of(
                            rowWithCity(
                                    evolved,
                                    7L,
                                    "Grace",
                                    55,
                                    "Toronto"
                            )
                    )
            );

            assertEquals(5, v5);

            List<Row> afterEvolvedAppend = table.readRows();

            assertEquals(
                    Set.of(1L, 3L, 4L, 5L, 6L, 7L),
                    ids(afterEvolvedAppend)
            );

            assertEquals(
                    "Toronto",
                    findById(afterEvolvedAppend, 7L).get("city")
            );

            // -----------------------------------------------------------------
            // 9. Checkpoint
            // -----------------------------------------------------------------

            long versionBeforeCheckpoint = table.version();

            table.checkpoint();

            assertEquals(
                    versionBeforeCheckpoint,
                    table.version()
            );

            assertTrue(
                    new LocalStorage(root).exists(
                            "_delta_log/_last_checkpoint"
                    )
            );

            assertEquals(
                    Set.of(1L, 3L, 4L, 5L, 6L, 7L),
                    ids(table.readRows())
            );

            assertEquals(
                    evolved.json(),
                    table.currentSchema().json()
            );

            // -----------------------------------------------------------------
            // 10. Optimize
            // -----------------------------------------------------------------

            long beforeOptimizeVersion = table.version();

            long optimizeVersion = table.optimize();

            assertTrue(
                    optimizeVersion >= beforeOptimizeVersion
            );

            assertEquals(
                    Set.of(1L, 3L, 4L, 5L, 6L, 7L),
                    ids(table.readRows())
            );

            // -----------------------------------------------------------------
            // 11. Transaction log
            // -----------------------------------------------------------------

            List<VersionedLogRecord> records = table.tail(-1);

            assertFalse(records.isEmpty());

            Set<Long> versions = new HashSet<>();

            for (VersionedLogRecord record : records) {
                versions.add(record.version());
            }

            for (long version = 0; version <= table.version(); version++) {
                assertTrue(
                        versions.contains(version),
                        "Missing transaction-log version " + version
                );
            }

            // -----------------------------------------------------------------
            // 12. Final state
            // -----------------------------------------------------------------

            List<Row> finalRows = table.readRows();

            assertEquals(
                    Set.of(1L, 3L, 4L, 5L, 6L, 7L),
                    ids(finalRows)
            );

            assertEquals(
                    "Carol-updated",
                    findById(finalRows, 3L).get("name")
            );

            assertEquals(
                    "Toronto",
                    findById(finalRows, 7L).get("city")
            );

        } finally {
            deleteRecursively(root);
        }
    }

    // -------------------------------------------------------------------------
    // Historical snapshots remain stable after maintenance
    // -------------------------------------------------------------------------

    @Test
    void historicalSnapshotsRemainStableAfterLaterMaintenance()
            throws Exception {

        Path root = Files.createTempDirectory(
                "delta-architecture-history-"
        );

        try {
            DeltaTable table = DeltaTable.open(
                    new LocalStorage(root),
                    1000
            );

            TableSchema schema = schema();

            table.appendRows(List.of(
                    row(schema, 1L, "Alice", 25),
                    row(schema, 2L, "Bob", 30)
            ));

            long v0 = table.version();

            table.appendRows(List.of(
                    row(schema, 3L, "Carol", 35)
            ));

            // Version 0 must remain unchanged after a later append.

            assertEquals(
                    Set.of(1L, 2L),
                    ids(table.readRows(v0))
            );

            table.optimize();

            // Maintenance must not alter the historical snapshot.

            assertEquals(
                    Set.of(1L, 2L),
                    ids(table.readRows(v0))
            );

            // Current table must contain all rows.

            assertEquals(
                    Set.of(1L, 2L, 3L),
                    ids(table.readRows())
            );

        } finally {
            deleteRecursively(root);
        }
    }

    // -------------------------------------------------------------------------
    // Checkpoint does not change semantics
    // -------------------------------------------------------------------------

    @Test
    void checkpointDoesNotChangeTableSemantics() throws Exception {
        Path root = Files.createTempDirectory(
                "delta-architecture-checkpoint-"
        );

        try {
            DeltaTable table = DeltaTable.open(
                    new LocalStorage(root),
                    2
            );

            TableSchema schema = schema();

            table.appendRows(List.of(
                    row(schema, 1L, "Alice", 25)
            ));

            table.appendRows(List.of(
                    row(schema, 2L, "Bob", 30)
            ));

            Set<Long> before = ids(table.readRows());

            long versionBefore = table.version();

            table.checkpoint();

            assertEquals(
                    versionBefore,
                    table.version()
            );

            Set<Long> after = ids(table.readRows());

            assertEquals(before, after);

        } finally {
            deleteRecursively(root);
        }
    }

    // -------------------------------------------------------------------------
    // Schema definitions
    // -------------------------------------------------------------------------

    private static TableSchema schema() {
        return TableSchema.fromJson("""
                {
                  "type": "record",
                  "name": "TestRecord",
                  "fields": [
                    {"name": "id", "type": "long"},
                    {"name": "name", "type": "string"},
                    {"name": "age", "type": "int"}
                  ]
                }
                """);
    }

    private static TableSchema schemaWithCity() {
        return TableSchema.fromJson("""
                {
                  "type": "record",
                  "name": "TestRecord",
                  "fields": [
                    {"name": "id", "type": "long"},
                    {"name": "name", "type": "string"},
                    {"name": "age", "type": "int"},
                    {"name": "city", "type": ["null", "string"], "default": null}
                  ]
                }
                """);
    }

    // -------------------------------------------------------------------------
    // Row helpers
    // -------------------------------------------------------------------------

    private static Row row(
            TableSchema schema,
            long id,
            String name,
            int age
    ) {
        return Row.of(schema, Map.of(
                "id", id,
                "name", name,
                "age", age
        ));
    }

    private static Row rowWithCity(
            TableSchema schema,
            long id,
            String name,
            int age,
            String city
    ) {
        Map<String, Object> values = new HashMap<>();

        values.put("id", id);
        values.put("name", name);
        values.put("age", age);
        values.put("city", city);

        return Row.of(schema, values);
    }

    // -------------------------------------------------------------------------
    // Assertions / utilities
    // -------------------------------------------------------------------------

    private static Set<Long> ids(List<Row> rows) {
        Set<Long> result = new HashSet<>();

        for (Row row : rows) {
            result.add(
                    ((Number) row.get("id")).longValue()
            );
        }

        return result;
    }

    private static Row findById(
            List<Row> rows,
            long id
    ) {
        return rows.stream()
                .filter(row ->
                        ((Number) row.get("id")).longValue() == id
                )
                .findFirst()
                .orElseThrow();
    }

    private static void deleteRecursively(Path root)
            throws Exception {

        if (!Files.exists(root)) {
            return;
        }

        try (var stream = Files.walk(root)) {
            stream
                    .sorted(Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (Exception ignored) {
                        }
                    });
        }
    }
}