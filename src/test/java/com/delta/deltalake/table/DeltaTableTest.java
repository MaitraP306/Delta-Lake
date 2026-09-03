package com.delta.deltalake.table;

import com.delta.deltalake.data.CheckpointCodec;
import com.delta.deltalake.data.CheckpointParquetReader;
import com.delta.deltalake.data.Record;
import com.delta.deltalake.data.Row;
import com.delta.deltalake.data.TableSchema;
import com.delta.deltalake.log.AddFile;
import com.delta.deltalake.log.FileStats;
import com.delta.deltalake.log.LogAction;
import com.delta.deltalake.log.LastCheckpoint;
import com.delta.deltalake.log.Metadata;
import com.delta.deltalake.log.Protocol;
import com.delta.deltalake.storage.LocalStorage;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DeltaTableTest {
    @Test
    void appendInitializesTableAndSupportsSnapshots() throws Exception {
        DeltaTable table = DeltaTable.open(new LocalStorage(Files.createTempDirectory("delta-table")));
        long v0 = table.append(List.of(new Record(1, "Alice", 25), new Record(2, "Bob", 31)));
        long v1 = table.append(List.of(new Record(3, "Charlie", 28)));
        assertEquals(0, v0);
        assertEquals(1, v1);
        assertEquals(3, table.readAll().size());
        assertEquals(2, table.read(0).size());
        assertEquals("parquet", table.snapshot().metadata().format());
    }

    @Test
    void idRangeQueryUsesFileStatisticsWithoutChangingResults() throws Exception {
        DeltaTable table = DeltaTable.open(new LocalStorage(Files.createTempDirectory("delta-stats")));
        table.append(List.of(new Record(1, "A", 20)));
        table.append(List.of(new Record(100, "B", 21)));
        assertEquals(List.of(new Record(1, "A", 20)), table.queryIdRange(0, 10));
    }

    @Test
    void deleteIsTransactionalAndSupportsTimeTravel() throws Exception {
        DeltaTable table = DeltaTable.open(new LocalStorage(Files.createTempDirectory("delta-delete")));
        table.append(List.of(new Record(1, "Alice", 25), new Record(2, "Bob", 31)));
        long v1 = table.delete(r -> r.id() == 1);
        assertEquals(1, v1);
        assertEquals(List.of(new Record(2, "Bob", 31)), table.readAll());
        assertEquals(2, table.read(0).size());
        assertTrue(table.history().stream().anyMatch(h -> h.operation().equals("DELETE")));
    }

    @Test
    void upsertReplaceAndInsertById() throws Exception {
        DeltaTable table = DeltaTable.open(new LocalStorage(Files.createTempDirectory("delta-upsert")));
        table.append(List.of(new Record(1, "Alice", 25), new Record(2, "Bob", 31)));
        table.upsert(List.of(new Record(2, "Robert", 32), new Record(3, "Cara", 22)));
        assertEquals(List.of(
                new Record(1, "Alice", 25),
                new Record(2, "Robert", 32),
                new Record(3, "Cara", 22)), table.readAll());
    }

    @Test
    void checkpointCanBeUsedForSnapshotReconstruction() throws Exception {
        LocalStorage storage = new LocalStorage(Files.createTempDirectory("delta-checkpoint"));
        DeltaTable table = DeltaTable.open(storage, 2);
        table.append(List.of(new Record(1, "A", 20)));
        table.append(List.of(new Record(2, "B", 21)));
        assertTrue(storage.exists("_delta_log/00000000000000000001.checkpoint.parquet"));
        table.append(List.of(new Record(3, "C", 22)));
        assertEquals(3, table.snapshot().fileCount());
        assertEquals(List.of(new Record(1, "A", 20),new Record(2, "B", 21),new Record(3, "C", 22)),table.readAll());
    }

    @Test
    void optimizeAndVacuumPreserveCurrentState() throws Exception {
        LocalStorage storage = new LocalStorage(Files.createTempDirectory("delta-maintenance"));
        DeltaTable table = DeltaTable.open(storage, 100);
        table.append(List.of(new Record(1, "A", 20)));
        table.append(List.of(new Record(2, "B", 21)));
        assertEquals(2, table.snapshot().fileCount());
        table.optimize();
        assertEquals(1, table.snapshot().fileCount());
        assertEquals(2, table.readAll().size());
        assertTrue(table.vacuum(Duration.ZERO) >= 1);
    }

    @Test
    void txnActionMakesApplicationWritesIdempotent() throws Exception {
        DeltaTable table = DeltaTable.open(new LocalStorage(Files.createTempDirectory("delta-txn")));
        long first = table.append(List.of(new Record(1, "A", 20)), "app", 7L);
        long second = table.append(List.of(new Record(1, "A", 20)), "app", 7L);
        assertEquals(first, second);
        assertEquals(1, table.readAll().size());
    }

    @Test
    void tableSchemaIsStoredInMetadata() throws Exception {
        LocalStorage storage =new LocalStorage(Files.createTempDirectory("delta-schema"));
        DeltaTable table = DeltaTable.open(storage);
        table.append(List.of(new Record(1L, "Alice", 25)));
        String schemaJson = table.snapshot().metadata().schemaString();
        TableSchema schema =TableSchema.fromJson(schemaJson);
        assertEquals("Record", schema.avroSchema().getName());
        assertNotNull(schema.avroSchema().getField("id"));
        assertNotNull(schema.avroSchema().getField("name"));
        assertNotNull(schema.avroSchema().getField("age"));
    }
    @Test
    void structuredCheckpointRoundTripsActions() throws Exception {
        LocalStorage storage = new LocalStorage(Files.createTempDirectory("delta-checkpoint-structured"));
        DeltaTable table = DeltaTable.open(storage, 2);
        table.append(List.of(new Record(1, "A", 20)));
        table.append(List.of(new Record(2, "B", 21)));
        assertTrue(storage.exists("_delta_log/00000000000000000001.checkpoint.parquet"));
        table.append(List.of(new Record(3, "C", 22)));
        Snapshot snapshot = table.snapshot();
        assertEquals(3, snapshot.fileCount());
        assertEquals(List.of(new Record(1, "A", 20),new Record(2, "B", 21),new Record(3, "C", 22)),table.readAll());
    }

    @Test
    void checkpointIsStructuredRatherThanPayloadJson() throws Exception {
        LocalStorage storage = new LocalStorage(Files.createTempDirectory("delta-checkpoint-structured"));

        DeltaTable table = DeltaTable.open(storage, 2);
        table.append(List.of(new Record(1, "A", 20)));
        table.append(List.of(new Record(2, "B", 21)));

        Path checkpoint = Files.createTempFile("checkpoint-inspect-", ".parquet");

        try {
            Files.write(checkpoint, storage.read("_delta_log/00000000000000000001.checkpoint.parquet"));
            List<LogAction> actions = CheckpointParquetReader.read(checkpoint).stream().map(CheckpointCodec::decode).toList();
            assertFalse(actions.isEmpty());
            assertTrue(actions.stream().anyMatch(action -> action instanceof Protocol));

            assertTrue(actions.stream().anyMatch(action -> action instanceof Metadata));

            assertTrue(actions.stream().anyMatch(action -> action instanceof AddFile));
        } finally {
            Files.deleteIfExists(checkpoint);
        }
    }


    @Test
    void checkpointPreservesStructuredFileStats() throws Exception {
        LocalStorage storage = new LocalStorage(Files.createTempDirectory("delta-checkpoint-stats"));

        DeltaTable table = DeltaTable.open(storage, 2);

        table.append(List.of(new Record(1L, "A", 20)));

        table.append(List.of(new Record(2L, "B", 30)));

        Path checkpoint = Files.createTempFile("checkpoint-stats-", ".parquet");

        try {
            Files.write(checkpoint, storage.read("_delta_log/00000000000000000001.checkpoint.parquet"));
            List<LogAction> actions = CheckpointParquetReader.read(checkpoint).stream().map(CheckpointCodec::decode).toList();
            AddFile add = actions.stream().filter(action -> action instanceof AddFile).map(action -> (AddFile) action).findFirst().orElseThrow();
            assertNotNull(add.stats());
            FileStats stats = add.stats();
            assertEquals(1L, stats.numRecords());

            FileStats.ColumnStats idStats = stats.columns().get("id");
            assertNotNull(idStats);
            assertEquals(1, idStats.min());
            assertEquals(1, idStats.max());
            assertEquals(0L, idStats.nullCount());
            FileStats.ColumnStats ageStats = stats.columns().get("age");
            assertNotNull(ageStats);
            assertEquals(20, ageStats.min());
            assertEquals(20, ageStats.max());
            assertEquals(0L, ageStats.nullCount());
        } finally {
            Files.deleteIfExists(checkpoint);
        }
    }
    @Test
    void genericRangeQueryAndStatisticsCoverMultipleColumns() throws Exception {
        DeltaTable table = DeltaTable.open(new LocalStorage(Files.createTempDirectory("delta-generic-query")));
        table.append(List.of(new Record(1, "Alice", 20)));
        table.append(List.of(new Record(2, "Bob", 40)));
        assertEquals(List.of(new Record(1, "Alice", 20)), table.query(Map.of(
                "name", new DeltaTable.QueryRange("A", "Azz"),
                "age", new DeltaTable.QueryRange(18, 30))));
    }

    @Test
    void schemaEvolutionAddsNullableColumnsWithoutRewritingData() throws Exception {
        DeltaTable table = DeltaTable.open(new LocalStorage(Files.createTempDirectory("delta-schema-evolution")));
        table.append(List.of(new Record(1, "Alice", 20)));
        TableSchema evolved = TableSchema.fromJson("""
                {
                  "type":"record", "name":"Record",
                  "fields":[
                    {"name":"id","type":"long"},
                    {"name":"name","type":"string"},
                    {"name":"age","type":"int"},
                    {"name":"city","type":["null","string"],"default":null}
                  ]
                }
                """);
        long version = table.evolveSchema(evolved);
        assertEquals(1, version);
        assertNotNull(table.snapshot().metadata().toString());
        assertNotNull(table.snapshot().metadata().schemaString().contains("city") ? evolved.avroSchema().getField("city") : null);
    }

    @Test
    void partitionedWritesAndStreamingSkipLayoutOnlyChanges() throws Exception {
        LocalStorage storage = new LocalStorage(Files.createTempDirectory("delta-partition-stream"));
        DeltaTable table = DeltaTable.open(storage, List.of("age"));
        DeltaTable.StreamingConsumer consumer = table.streamingConsumer();
        table.append(List.of(new Record(1, "A", 20)));
        assertTrue(storage.exists("data/age=20"));
        assertFalse(consumer.poll().isEmpty());

        table.append(List.of(new Record(2, "B", 30)));
        assertTrue(storage.exists("data/age=30"));
        assertFalse(consumer.poll().isEmpty());

        table.optimize();
        assertTrue(consumer.poll().isEmpty());
    }

    @Test
    void zOrderingAndAutoOptimizePreserveRows() throws Exception {
        DeltaTable table = DeltaTable.open(new LocalStorage(Files.createTempDirectory("delta-zorder")));
        table.append(List.of(new Record(1, "A", 40)));
        table.append(List.of(new Record(2, "B", 20)));
        long zVersion = table.optimizeZOrder("id", "age");
        assertEquals(2, table.readAll().size());
        assertEquals(zVersion, table.version());

        DeltaTable auto = DeltaTable.open(new LocalStorage(Files.createTempDirectory("delta-auto-optimize")));
        auto.append(List.of(new Record(1, "A", 20)));
        auto.setAutoOptimize(true);
        auto.append(List.of(new Record(2, "B", 21)));
        auto.append(List.of(new Record(3, "C", 22)));
        auto.append(List.of(new Record(4, "D", 23)));
        assertEquals(1, auto.snapshot().fileCount());
    }

    @Test
    void genericRowsSupportArbitrarySchemaPartitioningStatisticsAndMutations() throws Exception {
        LocalStorage storage = new LocalStorage(Files.createTempDirectory("delta-generic-row"));
        TableSchema schema = TableSchema.fromJson("""
                {
                  "type":"record", "name":"Product",
                  "fields":[
                    {"name":"sku","type":"string"},
                    {"name":"price","type":"double"},
                    {"name":"city","type":"string"},
                    {"name":"rating","type":["null","double"],"default":null}
                  ]
                }
                """);
        DeltaTable table = DeltaTable.open(storage, List.of("city"));
        Row a = Row.of(schema, Map.of("sku", "A", "price", 10.5d, "city", "Toronto", "rating", 4.5d));
        Map<String, Object> bValues = new LinkedHashMap<>();
        bValues.put("sku", "B");
        bValues.put("price", 20.0d);
        bValues.put("city", "Calgary");
        bValues.put("rating", null);
        Row b = Row.of(schema, bValues);
        table.appendRows(List.of(a, b));
        assertTrue(storage.exists("data/city=Toronto"));
        assertEquals(1, table.queryRows(Map.of("price", new DeltaTable.QueryRange(10d, 15d))).size());

        Row updated = Row.of(schema, Map.of("sku", "A", "price", 12.5d, "city", "Toronto", "rating", 4.8d));
        Row inserted = Row.of(schema, Map.of("sku", "C", "price", 7.0d, "city", "Vancouver", "rating", 4.0d));
        table.upsertRows(List.of(updated, inserted), "sku");
        assertEquals(3, table.readRows().size());
        assertEquals(1, table.queryRows(Map.of("price", new DeltaTable.QueryRange(12d, 13d))).size());

        table.optimizeZOrder("price", "city");
        assertEquals(3, table.readRows().size());
    }

    @Test
    void genericSchemaEvolutionProjectsOldFiles() throws Exception {
        DeltaTable table = DeltaTable.open(new LocalStorage(Files.createTempDirectory("delta-schema-projection")));
        TableSchema schema = TableSchema.fromJson("""
                {"type":"record","name":"Customer","fields":[{"name":"id","type":"long"},{"name":"name","type":"string"}]}
                """);
        Map<String, Object> values = new LinkedHashMap<>(); values.put("id", 1L); values.put("name", "Alice");
        table.appendRows(List.of(Row.of(schema, values)));
        TableSchema evolved = TableSchema.fromJson("""
                {"type":"record","name":"Customer","fields":[{"name":"id","type":"long"},{"name":"name","type":"string"},{"name":"country","type":["null","string"],"default":null}]}
                """);
        table.evolveSchema(evolved);
        Row projected = table.readRows().getFirst();
        assertEquals(evolved.json(), projected.schema().json());
        assertTrue(projected.contains("country"));
        assertNull(projected.get("country"));
    }

    @Test
    void rollbackRestoresHistoricalSnapshot() throws Exception {
        DeltaTable table = DeltaTable.open(new LocalStorage(Files.createTempDirectory("delta-rollback")));
        table.append(List.of(new Record(1, "A", 20)));
        table.append(List.of(new Record(2, "B", 30)));
        long rollbackVersion = table.rollbackToVersion(0);
        assertEquals(rollbackVersion, table.version());
        assertEquals(List.of(new Record(1, "A", 20)), table.readAll());
        assertEquals(1, table.read(0).size());
        assertTrue(table.history().stream().anyMatch(h -> h.operation().equals("ROLLBACK")));
    }


@Test
void schemaEvolutionSupportsDropRenameAndTypeWidening() throws Exception {
    LocalStorage storage = new LocalStorage(Files.createTempDirectory("delta-schema-general"));
    DeltaTable table = DeltaTable.open(storage);
    TableSchema original = TableSchema.fromJson("""
            {"type":"record","name":"Metric","fields":[
              {"name":"id","type":"int"},
              {"name":"value","type":"int"},
              {"name":"obsolete","type":"string"}]}
            """);
    Map<String,Object> row = new LinkedHashMap<>(); row.put("id", 1); row.put("value", 7); row.put("obsolete", "x");
    table.appendRows(List.of(Row.of(original, row)));
    String oldPath = table.snapshot().activeFiles().iterator().next().path();
    TableSchema evolved = TableSchema.fromJson("""
            {"type":"record","name":"Metric","fields":[
              {"name":"id","type":"long"},
              {"name":"value2","type":"long","aliases":["value"]}]}
            """);
    table.evolveSchema(evolved);
    Row projected = table.readRows().getFirst();
    assertEquals(1L, projected.get("id"));
    assertEquals(7L, projected.get("value2"));
    assertFalse(projected.contains("obsolete"));

    Snapshot evolvedSnapshot = table.snapshot();
    assertEquals(1, evolvedSnapshot.activeFiles().size());
    assertTrue(evolvedSnapshot.activeFiles().iterator().next().path().contains(".parquet"));
    assertTrue(storage.exists(oldPath));

    Row next = Row.of(evolved, Map.of("id", 2L, "value2", 11L));
    table.appendRows(List.of(next));
    assertEquals(2, table.readRows().size());
}

@Test
void mergeSupportsConditionalUpdateDeleteAndInsert() throws Exception {
    DeltaTable table = DeltaTable.open(new LocalStorage(Files.createTempDirectory("delta-merge-clauses")));
    TableSchema schema = TableSchema.fromJson("""
            {"type":"record","name":"Account","fields":[
              {"name":"id","type":"long"},
              {"name":"balance","type":"double"},
              {"name":"active","type":"boolean"}]}
            """);
    table.appendRows(List.of(Row.of(schema, Map.of("id",1L,"balance",100.0,"active",true)), Row.of(schema, Map.of("id",2L,"balance",20.0,"active",true))));

    Row source1 = Row.of(schema, Map.of("id",1L,"balance",150.0,"active",true));
    Row source2 = Row.of(schema, Map.of("id",2L,"balance",0.0,"active",false));
    Row source3 = Row.of(schema, Map.of("id",3L,"balance",30.0,"active",true));
    DeltaTable.MergeSpec spec = DeltaTable.MergeSpec.builder().whenMatchedDelete(ctx -> !ctx.source().get("active").equals(true)).whenMatchedUpdate((target, source) -> source).whenNotMatchedInsert(source -> (Boolean) source.get("active"), source -> source).build();
    table.mergeRows(List.of(source1, source2, source3), "id", spec);

    List<Row> rows = table.readRows();
    assertEquals(2, rows.size());
    assertEquals(150.0, rows.stream().filter(r -> r.get("id").equals(1L)).findFirst().orElseThrow().get("balance"));
    assertTrue(rows.stream().anyMatch(r -> r.get("id").equals(3L)));
    assertTrue(rows.stream().noneMatch(r -> r.get("id").equals(2L)));
}
@Test
void mergeAcceptsDifferentSourceSchema() throws Exception {
    DeltaTable table = DeltaTable.open(new LocalStorage(Files.createTempDirectory("delta-merge-different-schema")));
    TableSchema targetSchema = TableSchema.fromJson("""
            {"type":"record","name":"Target","fields":[
              {"name":"id","type":"long"},
              {"name":"name","type":"string"},
              {"name":"active","type":"boolean"}]}
            """);
    table.appendRows(List.of(Row.of(targetSchema, Map.of("id",1L,"name","Alice","active",true))));
    TableSchema sourceSchema = TableSchema.fromJson("""
            {"type":"record","name":"Source","fields":[
              {"name":"id","type":"long"},
              {"name":"enabled","type":"boolean"}]}
            """);
    Row update = Row.of(sourceSchema, Map.of("id",1L,"enabled",false));
    Row insert = Row.of(sourceSchema, Map.of("id",2L,"enabled",true));
    DeltaTable.MergeSpec spec = DeltaTable.MergeSpec.builder().whenMatchedUpdate((target, source) -> target.with("active", source.get("enabled"))).whenNotMatchedInsert(source -> Row.of(targetSchema, Map.of("id", source.get("id"), "name", "generated", "active", source.get("enabled")))).build();
    table.mergeRows(List.of(update, insert), "id", spec);
    assertFalse((Boolean) table.readRows().stream().filter(r -> r.get("id").equals(1L)).findFirst().orElseThrow().get("active"));
    assertTrue(table.readRows().stream().anyMatch(r -> r.get("id").equals(2L) && r.get("name").equals("generated")));
}



    @Test
    void perTableRetentionIsPersistedAndHonoredByVacuum() throws Exception {
        LocalStorage storage = new LocalStorage(Files.createTempDirectory("delta-retention"));
        DeltaTable table = DeltaTable.open(storage);
        table.append(List.of(new Record(1, "A", 20)));
        assertEquals(Duration.ofDays(7), table.retention());
        long configured = table.setRetention(Duration.ZERO);
        assertEquals(configured, table.version());
        assertEquals(Duration.ZERO, table.retention());
        String dataPath = table.snapshot().activeFiles().iterator().next().path();
        table.delete(r -> r.id() == 1);
        table.vacuum();
        assertFalse(storage.exists(dataPath));
    }

    @Test
    void olderCheckpointCannotRegressLastCheckpointPointer() throws Exception {
        LocalStorage storage = new LocalStorage(Files.createTempDirectory("delta-checkpoint-publication"));
        DeltaTable table = DeltaTable.open(storage, 100);
        table.append(List.of(new Record(1, "A", 20)));
        table.append(List.of(new Record(2, "B", 21)));
        CheckpointManager manager = new CheckpointManager(storage, new com.delta.deltalake.log.TransactionLog(storage));
        manager.create(1);
        manager.create(0);
        LastCheckpoint pointer = new com.delta.deltalake.log.TransactionLog(storage).deserialize(storage.read("_delta_log/_last_checkpoint"), LastCheckpoint.class);
        assertEquals(1, pointer.version());
    }
}
