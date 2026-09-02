package com.delta.deltalake.table;

import com.delta.deltalake.data.CheckpointCodec;
import com.delta.deltalake.data.CheckpointParquetReader;
import com.delta.deltalake.data.Record;
import com.delta.deltalake.data.TableSchema;
import com.delta.deltalake.log.AddFile;
import com.delta.deltalake.log.FileStats;
import com.delta.deltalake.log.LogAction;
import com.delta.deltalake.log.Metadata;
import com.delta.deltalake.log.Protocol;
import com.delta.deltalake.storage.LocalStorage;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

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
}
