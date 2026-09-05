package com.delta.deltalake.table;

import com.delta.deltalake.data.Row;
import com.delta.deltalake.data.TableSchema;
import com.delta.deltalake.log.AddFile;
import com.delta.deltalake.log.VersionedLogRecord;
import com.delta.deltalake.storage.LocalStorage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DeltaTableSchemaEvolutionVerificationTest {

    @TempDir
    Path tempDir;

    private DeltaTable newTable() {
        return DeltaTable.open(new LocalStorage(tempDir));
    }

    private TableSchema schema(String json) {
        return TableSchema.fromJson(json);
    }

    private TableSchema baseSchema() {
        return schema("""
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

    private TableSchema schemaWithCity() {
        return schema("""
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

    private TableSchema schemaWithRequiredCity() {
        return schema("""
                {
                  "type": "record",
                  "name": "TestRecord",
                  "fields": [
                    {"name": "id", "type": "long"},
                    {"name": "name", "type": "string"},
                    {"name": "age", "type": "int"},
                    {"name": "city", "type": "string"}
                  ]
                }
                """);
    }

    private TableSchema renamedNameSchema() {
        return schema("""
                {
                  "type": "record",
                  "name": "TestRecord",
                  "fields": [
                    {"name": "id", "type": "long"},
                    {
                      "name": "full_name",
                      "type": "string",
                      "aliases": ["name"]
                    },
                    {"name": "age", "type": "int"}
                  ]
                }
                """);
    }

    private TableSchema intToLongSchema() {
        return schema("""
                {
                  "type": "record",
                  "name": "TestRecord",
                  "fields": [
                    {"name": "id", "type": "long"},
                    {"name": "name", "type": "string"},
                    {"name": "age", "type": "long"}
                  ]
                }
                """);
    }

    private TableSchema longToDoubleSchema() {
        return schema("""
                {
                  "type": "record",
                  "name": "TestRecord",
                  "fields": [
                    {"name": "id", "type": "double"},
                    {"name": "name", "type": "string"},
                    {"name": "age", "type": "int"}
                  ]
                }
                """);
    }

    private TableSchema longToIntSchema() {
        return schema("""
                {
                  "type": "record",
                  "name": "TestRecord",
                  "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "name", "type": "string"},
                    {"name": "age", "type": "int"}
                  ]
                }
                """);
    }

    private TableSchema incompatibleSchema() {
        return schema("""
                {
                  "type": "record",
                  "name": "TestRecord",
                  "fields": [
                    {"name": "id", "type": "long"},
                    {"name": "name", "type": "int"},
                    {"name": "age", "type": "int"}
                  ]
                }
                """);
    }

    private TableSchema schemaWithoutAge() {
        return schema("""
                {
                  "type": "record",
                  "name": "TestRecord",
                  "fields": [
                    {"name": "id", "type": "long"},
                    {"name": "name", "type": "string"}
                  ]
                }
                """);
    }

    private Row row(TableSchema schema, long id, String name, int age) {
        return Row.of(schema, Map.of(
                "id", id,
                "name", name,
                "age", age
        ));
    }

    private Row rowWithCity(
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

    private List<AddFile> activeFiles(DeltaTable table) throws IOException {
        return table.snapshot().activeFiles().stream().toList();
    }

    private List<String> activePaths(DeltaTable table) throws IOException {
        return activeFiles(table).stream()
                .map(AddFile::path)
                .toList();
    }

//     private boolean containsAction(
//             List<VersionedLogRecord> records,
//             Class<? extends LogAction> actionClass
//     ) {
//         for (VersionedLogRecord versioned : records) {
//             Object action = versioned.record().action();

//             if (actionClass.isInstance(action)) {
//                 return true;
//             }
//         }

//         return false;
//     }

    // -------------------------------------------------------------------------
    // Basic additive evolution
    // -------------------------------------------------------------------------

    @Test
    void addNullableColumn() throws Exception {
        DeltaTable table = newTable();

        TableSchema original = baseSchema();
        TableSchema evolved = schemaWithCity();

        table.appendRows(List.of(
                row(original, 1, "Alice", 25)
        ));

        assertEquals(0, table.version());
        assertEquals(original.json(), table.currentSchema().json());

        long version = table.evolveSchema(evolved);

        assertEquals(1, version);
        assertEquals(evolved.json(), table.currentSchema().json());
    }

    @Test
    void additiveEvolutionDoesNotRewriteExistingDataFiles() throws Exception {
        DeltaTable table = newTable();

        TableSchema original = baseSchema();
        TableSchema evolved = schemaWithCity();

        table.appendRows(List.of(
                row(original, 1, "Alice", 25),
                row(original, 2, "Bob", 30)
        ));

        List<String> before = activePaths(table);

        assertEquals(1, before.size());

        table.evolveSchema(evolved);

        List<String> after = activePaths(table);

        assertEquals(before, after);
    }

    @Test
    void oldRowsExposeNewNullableColumnAsNull() throws Exception {
        DeltaTable table = newTable();

        TableSchema original = baseSchema();
        TableSchema evolved = schemaWithCity();

        table.appendRows(List.of(
                row(original, 1, "Alice", 25)
        ));

        table.evolveSchema(evolved);

        List<Row> rows = table.readRows();

        assertEquals(1, rows.size());

        Row result = rows.get(0);

        assertEquals(1L, result.get("id"));
        assertEquals("Alice", result.get("name"));
        assertEquals(25, result.get("age"));
        assertNull(result.get("city"));
    }

    @Test
    void newRowsCanPopulateNewColumn() throws Exception {
        DeltaTable table = newTable();

        TableSchema original = baseSchema();
        TableSchema evolved = schemaWithCity();

        table.appendRows(List.of(
                row(original, 1, "Alice", 25)
        ));

        table.evolveSchema(evolved);

        table.appendRows(List.of(
                rowWithCity(evolved, 2, "Bob", 30, "Toronto")
        ));

        List<Row> rows = table.readRows();

        assertEquals(2, rows.size());

        Map<Long, Row> byId = new HashMap<>();

        for (Row row : rows) {
            byId.put((Long) row.get("id"), row);
        }

        assertNull(byId.get(1L).get("city"));
        assertEquals("Toronto", byId.get(2L).get("city"));
    }

    @Test
    void historicalSnapshotRetainsOldSchema() throws Exception {
        DeltaTable table = newTable();

        TableSchema original = baseSchema();
        TableSchema evolved = schemaWithCity();

        table.appendRows(List.of(
                row(original, 1, "Alice", 25)
        ));

        assertEquals(0, table.version());

        table.evolveSchema(evolved);

        assertEquals(1, table.version());

        assertEquals(
                original.json(),
                table.snapshot(0).metadata().schemaString()
        );

        assertEquals(
                evolved.json(),
                table.snapshot(1).metadata().schemaString()
        );
    }

    // -------------------------------------------------------------------------
    // Required fields
    // -------------------------------------------------------------------------

    @Test
    void requiredNewColumnWithoutDefaultIsRejected() throws Exception {
        DeltaTable table = newTable();

        TableSchema original = baseSchema();
        TableSchema evolved = schemaWithRequiredCity();

        table.appendRows(List.of(
                row(original, 1, "Alice", 25)
        ));

        assertThrows(
                IllegalArgumentException.class,
                () -> table.evolveSchema(evolved)
        );

        assertEquals(0, table.version());
        assertEquals(original.json(), table.currentSchema().json());
    }

    // -------------------------------------------------------------------------
    // Rename / Avro alias
    // -------------------------------------------------------------------------

    @Test
    void renameUsingAliasIsAccepted() throws Exception {
        DeltaTable table = newTable();

        TableSchema original = baseSchema();
        TableSchema renamed = renamedNameSchema();

        table.appendRows(List.of(
                row(original, 1, "Alice", 25)
        ));

        long version = table.evolveSchema(renamed);

        assertEquals(1, version);
        assertEquals(renamed.json(), table.currentSchema().json());
    }

    @Test
    void renamedColumnCanReadOldPhysicalData() throws Exception {
        DeltaTable table = newTable();

        TableSchema original = baseSchema();
        TableSchema renamed = renamedNameSchema();

        table.appendRows(List.of(
                row(original, 1, "Alice", 25)
        ));

        table.evolveSchema(renamed);

        List<Row> rows = table.readRows();

        assertEquals(1, rows.size());

        Row result = rows.get(0);

        assertEquals(1L, result.get("id"));
        assertEquals("Alice", result.get("full_name"));
        assertEquals(25, result.get("age"));
    }

    @Test
    void renameDoesNotLoseExistingRows() throws Exception {
        DeltaTable table = newTable();

        TableSchema original = baseSchema();
        TableSchema renamed = renamedNameSchema();

        table.appendRows(List.of(
                row(original, 1, "Alice", 25),
                row(original, 2, "Bob", 30)
        ));

        table.evolveSchema(renamed);

        List<Row> rows = table.readRows();

        assertEquals(2, rows.size());

        assertTrue(
                rows.stream().anyMatch(
                        r -> "Alice".equals(r.get("full_name"))
                )
        );

        assertTrue(
                rows.stream().anyMatch(
                        r -> "Bob".equals(r.get("full_name"))
                )
        );
    }

    // -------------------------------------------------------------------------
    // Numeric widening / narrowing
    // -------------------------------------------------------------------------

    @Test
    void intToLongWideningIsAccepted() throws Exception {
        DeltaTable table = newTable();

        TableSchema original = baseSchema();
        TableSchema evolved = intToLongSchema();

        table.appendRows(List.of(
                row(original, 1, "Alice", 25)
        ));

        long version = table.evolveSchema(evolved);

        assertEquals(1, version);
        assertEquals(evolved.json(), table.currentSchema().json());

        Row result = table.readRows().get(0);

        assertEquals(25L, result.get("age"));
    }

    @Test
    void longToDoubleWideningIsAccepted() throws Exception {
        DeltaTable table = newTable();

        TableSchema original = baseSchema();
        TableSchema evolved = longToDoubleSchema();

        table.appendRows(List.of(
                row(original, 1, "Alice", 25)
        ));

        long version = table.evolveSchema(evolved);

        assertEquals(1, version);
        assertEquals(evolved.json(), table.currentSchema().json());

        Row result = table.readRows().get(0);

        assertEquals(1.0d, result.get("id"));
    }

    @Test
    void narrowingLongToIntIsRejected() throws Exception {
        DeltaTable table = newTable();

        TableSchema original = baseSchema();
        TableSchema narrowed = longToIntSchema();

        table.appendRows(List.of(
                row(original, 1, "Alice", 25)
        ));

        assertThrows(
                IllegalArgumentException.class,
                () -> table.evolveSchema(narrowed)
        );

        assertEquals(0, table.version());
        assertEquals(original.json(), table.currentSchema().json());
    }

    @Test
    void incompatibleTypeChangeIsRejected() throws Exception {
        DeltaTable table = newTable();

        TableSchema original = baseSchema();
        TableSchema incompatible = incompatibleSchema();

        table.appendRows(List.of(
                row(original, 1, "Alice", 25)
        ));

        assertThrows(
                IllegalArgumentException.class,
                () -> table.evolveSchema(incompatible)
        );

        assertEquals(0, table.version());
        assertEquals(original.json(), table.currentSchema().json());
    }

    // -------------------------------------------------------------------------
    // Drop column / physical rewrite
    // -------------------------------------------------------------------------

    @Test
    void droppingColumnIsAcceptedAndCreatesNewVersion() throws Exception {
        DeltaTable table = newTable();

        TableSchema original = baseSchema();
        TableSchema dropped = schemaWithoutAge();

        table.appendRows(List.of(
                row(original, 1, "Alice", 25),
                row(original, 2, "Bob", 30)
        ));

        long version = table.evolveSchema(dropped);

        assertEquals(1, version);
        assertEquals(dropped.json(), table.currentSchema().json());
    }

    @Test
    void droppingColumnRewritesPhysicalData() throws Exception {
        DeltaTable table = newTable();

        TableSchema original = baseSchema();
        TableSchema dropped = schemaWithoutAge();

        table.appendRows(List.of(
                row(original, 1, "Alice", 25),
                row(original, 2, "Bob", 30)
        ));

        List<String> before = activePaths(table);

        assertEquals(1, before.size());

        table.evolveSchema(dropped);

        List<String> after = activePaths(table);

        assertEquals(1, after.size());
        assertNotEquals(before.get(0), after.get(0));
    }

    @Test
    void droppingColumnProducesRemoveAndAddActions() throws Exception {
        DeltaTable table = newTable();

        TableSchema original = baseSchema();
        TableSchema dropped = schemaWithoutAge();

        table.appendRows(List.of(
                row(original, 1, "Alice", 25)
        ));

        long version = table.evolveSchema(dropped);

        assertEquals(1, version);

        List<VersionedLogRecord> records = table.tail(0);

        System.out.println("=== SCHEMA EVOLUTION LOG ===");

        for (VersionedLogRecord record : records) {
            System.out.println(
                    "version=" + record.version()
                            + ", type=" + record.record().type()
                            + ", actionClass="
                            + record.record().action().getClass().getName()
                            + ", action="
                            + record.record().action()
            );
        }

        System.out.println("============================");

        assertFalse(records.isEmpty());

        assertTrue(
                records.stream()
                        .anyMatch(r -> "remove".equalsIgnoreCase(r.record().type())),
                "Expected remove action"
        );

        assertTrue(
                records.stream()
                        .anyMatch(r -> "add".equalsIgnoreCase(r.record().type())),
                "Expected add action"
        );
    }

    @Test
    void droppingColumnPreservesRemainingRows() throws Exception {
        DeltaTable table = newTable();

        TableSchema original = baseSchema();
        TableSchema dropped = schemaWithoutAge();

        table.appendRows(List.of(
                row(original, 1, "Alice", 25),
                row(original, 2, "Bob", 30)
        ));

        table.evolveSchema(dropped);

        List<Row> rows = table.readRows();

        assertEquals(2, rows.size());

        assertTrue(
                rows.stream().anyMatch(
                        r -> ((Long) r.get("id")) == 1L
                                && "Alice".equals(r.get("name"))
                )
        );

        assertTrue(
                rows.stream().anyMatch(
                        r -> ((Long) r.get("id")) == 2L
                                && "Bob".equals(r.get("name"))
                )
        );
    }

    // -------------------------------------------------------------------------
    // Idempotence
    // -------------------------------------------------------------------------

    @Test
    void identicalSchemaEvolutionIsNoOp() throws Exception {
        DeltaTable table = newTable();

        TableSchema original = baseSchema();

        table.appendRows(List.of(
                row(original, 1, "Alice", 25)
        ));

        assertEquals(0, table.version());

        long version = table.evolveSchema(original);

        assertEquals(0, version);
        assertEquals(0, table.version());
        assertEquals(original.json(), table.currentSchema().json());
    }

    // -------------------------------------------------------------------------
    // Checkpoint interaction
    // -------------------------------------------------------------------------

    @Test
    void checkpointPreservesEvolvedSchema() throws Exception {
        DeltaTable table = newTable();

        TableSchema original = baseSchema();
        TableSchema evolved = schemaWithCity();

        table.appendRows(List.of(
                row(original, 1, "Alice", 25)
        ));

        table.evolveSchema(evolved);

        assertEquals(1, table.version());

        table.checkpoint();

        DeltaTable reopened = newTable();

        assertEquals(1, reopened.version());
        assertEquals(
                evolved.json(),
                reopened.currentSchema().json()
        );

        List<Row> rows = reopened.readRows();

        assertEquals(1, rows.size());
        assertNull(rows.get(0).get("city"));
    }

    // -------------------------------------------------------------------------
    // User metadata
    // -------------------------------------------------------------------------

    @Test
    void evolutionCanRecordUserMetadata() throws Exception {
        DeltaTable table = newTable();

        TableSchema original = baseSchema();
        TableSchema evolved = schemaWithCity();

        table.appendRows(List.of(
                row(original, 1, "Alice", 25)
        ));

        long version = table.evolveSchema(
                evolved,
                "add city column"
        );

        assertEquals(1, version);

        List<VersionedLogRecord> records = table.tail(0);

        System.out.println("=== SCHEMA EVOLUTION METADATA ===");

        for (VersionedLogRecord record : records) {
            System.out.println(
                    "version=" + record.version()
                            + ", type=" + record.record().type()
                            + ", action=" + record.record().action()
            );
        }

        System.out.println("=================================");

        assertTrue(
                records.stream().anyMatch(
                        r -> "commitInfo".equalsIgnoreCase(r.record().type())
                ),
                "Expected CommitInfo action"
        );
    }

    // -------------------------------------------------------------------------
    // Failed evolution must not modify table
    // -------------------------------------------------------------------------

    @Test
    void failedEvolutionDoesNotChangeTableState() throws Exception {
        DeltaTable table = newTable();

        TableSchema original = baseSchema();
        TableSchema incompatible = incompatibleSchema();

        table.appendRows(List.of(
                row(original, 1, "Alice", 25)
        ));

        List<String> pathsBefore = activePaths(table);

        assertThrows(
                IllegalArgumentException.class,
                () -> table.evolveSchema(incompatible)
        );

        assertEquals(0, table.version());
        assertEquals(original.json(), table.currentSchema().json());
        assertEquals(pathsBefore, activePaths(table));

        List<Row> rows = table.readRows();

        assertEquals(1, rows.size());
        assertEquals("Alice", rows.get(0).get("name"));
    }
}