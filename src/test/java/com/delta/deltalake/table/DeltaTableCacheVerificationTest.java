package com.delta.deltalake.table;

import com.delta.deltalake.data.Row;
import com.delta.deltalake.data.TableSchema;
import com.delta.deltalake.storage.LocalStorage;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DeltaTableCacheVerificationTest {

    @TempDir
    Path tempDir;

    private DeltaTable newTable() {
        return DeltaTable.open(new LocalStorage(tempDir));
    }

    private TableSchema schema() {
        String json = """
                {
                  "type": "record",
                  "name": "TestRecord",
                  "namespace": "com.delta.test",
                  "fields": [
                    {"name": "id", "type": "long"},
                    {"name": "value", "type": "double"}
                  ]
                }
                """;

        return new TableSchema(new Schema.Parser().parse(json));
    }

    private Row row(TableSchema schema, long id, double value) {
        return Row.of(
                schema,
                Map.of(
                        "id", id,
                        "value", value
                )
        );
    }

    private List<Row> sortedById(List<Row> rows) {
        return rows.stream()
                .sorted(Comparator.comparingLong(
                        r -> ((Number) r.get("id")).longValue()
                ))
                .toList();
    }

    @Test
    void repeatedReadsReturnSameData() throws Exception {
        DeltaTable table = newTable();
        TableSchema schema = schema();

        List<Row> input = List.of(
                row(schema, 1, 10.0),
                row(schema, 2, 20.0),
                row(schema, 3, 30.0)
        );

        long version = table.appendRows(input);

        assertEquals(0, version);

        List<Row> firstRead = sortedById(table.readRows());
        List<Row> secondRead = sortedById(table.readRows());
        List<Row> thirdRead = sortedById(table.readRows());

        assertEquals(3, firstRead.size());
        assertEquals(firstRead, secondRead);
        assertEquals(secondRead, thirdRead);
    }

    @Test
    void appendAfterCachedReadReturnsNewData() throws Exception {
        DeltaTable table = newTable();
        TableSchema schema = schema();

        table.appendRows(List.of(
                row(schema, 1, 10.0),
                row(schema, 2, 20.0)
        ));

        // Populate the caches.
        List<Row> beforeAppend = sortedById(table.readRows());

        assertEquals(2, beforeAppend.size());

        table.appendRows(List.of(
                row(schema, 3, 30.0),
                row(schema, 4, 40.0)
        ));

        List<Row> afterAppend = sortedById(table.readRows());

        assertEquals(4, afterAppend.size());

        assertEquals(
                List.of(1L, 2L, 3L, 4L),
                afterAppend.stream()
                        .map(r -> ((Number) r.get("id")).longValue())
                        .toList()
        );
    }

    @Test
    void cachedHistoricalSnapshotDoesNotChangeAfterNewAppend() throws Exception {
        DeltaTable table = newTable();
        TableSchema schema = schema();

        table.appendRows(List.of(
                row(schema, 1, 10.0),
                row(schema, 2, 20.0)
        ));

        Snapshot versionZeroFirst = table.snapshot(0);

        assertEquals(0, versionZeroFirst.version());
        assertEquals(1, versionZeroFirst.fileCount());

        // Same version requested again.
        Snapshot versionZeroSecond = table.snapshot(0);

        assertEquals(0, versionZeroSecond.version());
        assertEquals(
                versionZeroFirst.fileCount(),
                versionZeroSecond.fileCount()
        );

        table.appendRows(List.of(
                row(schema, 3, 30.0)
        ));

        assertEquals(1, table.version());

        // Historical snapshot must remain unchanged.
        Snapshot historicalAgain = table.snapshot(0);

        assertEquals(0, historicalAgain.version());
        assertEquals(1, historicalAgain.fileCount());

        // Current snapshot contains both files.
        Snapshot current = table.snapshot();

        assertEquals(1, current.version());
        assertEquals(2, current.fileCount());
    }

    @Test
    void timeTravelRemainsCorrectAcrossMultipleCachedVersions() throws Exception {
        DeltaTable table = newTable();
        TableSchema schema = schema();

        table.appendRows(List.of(
                row(schema, 1, 10.0)
        ));

        table.appendRows(List.of(
                row(schema, 2, 20.0)
        ));

        table.appendRows(List.of(
                row(schema, 3, 30.0)
        ));

        assertEquals(2, table.version());

        Snapshot v0a = table.snapshot(0);
        Snapshot v1a = table.snapshot(1);
        Snapshot v2a = table.snapshot(2);

        // Request them again in a different order.
        Snapshot v1b = table.snapshot(1);
        Snapshot v0b = table.snapshot(0);
        Snapshot v2b = table.snapshot(2);

        assertEquals(0, v0a.version());
        assertEquals(1, v1a.version());
        assertEquals(2, v2a.version());

        assertEquals(0, v0b.version());
        assertEquals(1, v1b.version());
        assertEquals(2, v2b.version());

        assertEquals(v0a.fileCount(), v0b.fileCount());
        assertEquals(v1a.fileCount(), v1b.fileCount());
        assertEquals(v2a.fileCount(), v2b.fileCount());

        assertEquals(1, v0a.fileCount());
        assertEquals(2, v1a.fileCount());
        assertEquals(3, v2a.fileCount());
    }

    @Test
    void mutationAfterCachedReadDoesNotReturnStaleRows() throws Exception {
        DeltaTable table = newTable();
        TableSchema schema = schema();

        table.appendRows(List.of(
                row(schema, 1, 10.0),
                row(schema, 2, 20.0),
                row(schema, 3, 30.0)
        ));

        // Populate data cache.
        List<Row> beforeDelete = sortedById(table.readRows());

        assertEquals(3, beforeDelete.size());

        table.deleteRows(r ->
                ((Number) r.get("id")).longValue() == 2L
        );

        List<Row> afterDelete = sortedById(table.readRows());

        assertEquals(2, afterDelete.size());

        assertEquals(
                List.of(1L, 3L),
                afterDelete.stream()
                        .map(r -> ((Number) r.get("id")).longValue())
                        .toList()
        );
    }

    @Test
    void upsertAfterCachedReadReturnsUpdatedData() throws Exception {
        DeltaTable table = newTable();
        TableSchema schema = schema();

        table.appendRows(List.of(
                row(schema, 1, 10.0),
                row(schema, 2, 20.0)
        ));

        // Populate caches.
        List<Row> beforeUpsert = sortedById(table.readRows());

        assertEquals(2, beforeUpsert.size());

        table.upsertRows(
                List.of(
                        row(schema, 2, 200.0),
                        row(schema, 3, 30.0)
                ),
                "id"
        );

        List<Row> afterUpsert = sortedById(table.readRows());

        assertEquals(3, afterUpsert.size());

        Row id2 = afterUpsert.stream()
                .filter(r ->
                        ((Number) r.get("id")).longValue() == 2L
                )
                .findFirst()
                .orElseThrow();

        assertEquals(
                200.0,
                ((Number) id2.get("value")).doubleValue()
        );

        assertEquals(
                List.of(1L, 2L, 3L),
                afterUpsert.stream()
                        .map(r -> ((Number) r.get("id")).longValue())
                        .toList()
        );
    }

    @Test
    void optimizeAfterCachedReadPreservesRows() throws Exception {
        DeltaTable table = newTable();
        TableSchema schema = schema();

        table.appendRows(List.of(
                row(schema, 1, 10.0)
        ));

        table.appendRows(List.of(
                row(schema, 2, 20.0)
        ));

        table.appendRows(List.of(
                row(schema, 3, 30.0)
        ));

        // Populate data cache before optimization.
        List<Row> beforeOptimize = sortedById(table.readRows());

        assertEquals(3, beforeOptimize.size());

        long optimizeVersion = table.optimize();

        assertTrue(optimizeVersion > 2);

        List<Row> afterOptimize = sortedById(table.readRows());

        assertEquals(3, afterOptimize.size());
        assertEquals(beforeOptimize, afterOptimize);

        assertEquals(
                List.of(1L, 2L, 3L),
                afterOptimize.stream()
                        .map(r -> ((Number) r.get("id")).longValue())
                        .toList()
        );
    }

    @Test
    void checkpointAfterCachedReadsPreservesSnapshots() throws Exception {
        DeltaTable table = newTable();
        TableSchema schema = schema();

        table.appendRows(List.of(
                row(schema, 1, 10.0)
        ));

        table.appendRows(List.of(
                row(schema, 2, 20.0)
        ));

        // Populate caches.
        List<Row> beforeCheckpoint = sortedById(table.readRows());
        Snapshot beforeCheckpointSnapshot = table.snapshot();

        assertEquals(2, beforeCheckpoint.size());
        assertEquals(1, beforeCheckpointSnapshot.version());

        table.checkpoint();

        Snapshot afterCheckpointSnapshot = table.snapshot();

        assertEquals(
                beforeCheckpointSnapshot.version(),
                afterCheckpointSnapshot.version()
        );

        assertEquals(
                beforeCheckpointSnapshot.fileCount(),
                afterCheckpointSnapshot.fileCount()
        );

        List<Row> afterCheckpoint = sortedById(table.readRows());

        assertEquals(beforeCheckpoint, afterCheckpoint);
    }

    @Test
    void repeatedCheckpointBackedSnapshotsRemainCorrect() throws Exception {
        DeltaTable table = DeltaTable.open(
                new LocalStorage(tempDir),
                2
        );

        TableSchema schema = schema();

        table.appendRows(List.of(
                row(schema, 1, 10.0)
        ));

        table.appendRows(List.of(
                row(schema, 2, 20.0)
        ));

        // With interval 2, version 1 should have an automatic checkpoint.
        assertEquals(1, table.version());

        Snapshot first = table.snapshot(1);
        Snapshot second = table.snapshot(1);
        Snapshot third = table.snapshot(1);

        assertEquals(1, first.version());
        assertEquals(1, second.version());
        assertEquals(1, third.version());

        assertEquals(first.fileCount(), second.fileCount());
        assertEquals(second.fileCount(), third.fileCount());

        assertEquals(2, first.fileCount());
    }

    @Test
    void cacheDoesNotChangeCurrentVersionSemantics() throws Exception {
        DeltaTable table = newTable();
        TableSchema schema = schema();

        table.appendRows(List.of(
                row(schema, 1, 10.0)
        ));

        // Repeated operations should exercise the caches.
        for (int i = 0; i < 5; i++) {
            assertEquals(0, table.version());
            assertEquals(1, table.readRows().size());
            assertEquals(0, table.snapshot().version());
        }

        table.appendRows(List.of(
                row(schema, 2, 20.0)
        ));

        for (int i = 0; i < 5; i++) {
            assertEquals(1, table.version());
            assertEquals(2, table.readRows().size());
            assertEquals(1, table.snapshot().version());
        }
    }
}