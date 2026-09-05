package com.delta.deltalake.table;

import com.delta.deltalake.data.Row;
import com.delta.deltalake.data.TableSchema;
import com.delta.deltalake.log.AddFile;
import com.delta.deltalake.log.VersionedLogRecord;
import com.delta.deltalake.storage.S3Storage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class DeltaTableS3IntegrationVerificationTest {

    private static final String BUCKET =
            "delta-lake-experiments-263242277349";

    private static final String PREFIX =
            "phase12/" + UUID.randomUUID();

    private S3Storage storage;
    private DeltaTable table;

    @BeforeEach
    void setUp() {
        storage = new S3Storage(
                BUCKET,
                PREFIX,
                Region.US_EAST_2
        );

        table = DeltaTable.open(storage, 10);
    }

    @AfterEach
    void tearDown() throws Exception {
        cleanupS3Prefix();
        storage.close();
    }

    @Test
    void appendCreatesCompleteDeltaTableInS3() throws Exception {
        TableSchema schema = schema();

        long version = table.appendRows(List.of(
                row(schema, 1, "Alice", 25),
                row(schema, 2, "Bob", 30)
        ));

        assertEquals(0, version);
        assertEquals(0, table.version());
        assertTrue(table.exists());

        assertTrue(storage.exists("_delta_log/00000000000000000000.json"));

        List<String> dataFiles = storage.list("data");

        assertEquals(1, dataFiles.size());
        assertTrue(dataFiles.get(0).endsWith(".parquet"));

        assertEquals(2, table.readRows().size());
    }

    @Test
    void appendAndReadRoundTripThroughS3() throws Exception {
        TableSchema schema = schema();

        table.appendRows(List.of(
                row(schema, 1, "Alice", 25),
                row(schema, 2, "Bob", 30),
                row(schema, 3, "Charlie", 35)
        ));

        List<Row> rows = table.readRows();

        assertEquals(3, rows.size());

        Set<Long> ids = rows.stream()
                .map(row -> ((Number) row.get("id")).longValue())
                .collect(Collectors.toSet());

        assertEquals(Set.of(1L, 2L, 3L), ids);
    }

    @Test
    void multipleAppendsCreateContiguousS3TransactionLog() throws Exception {
        TableSchema schema = schema();

        assertEquals(
                0,
                table.appendRows(List.of(
                        row(schema, 1, "Alice", 25)
                ))
        );

        assertEquals(
                1,
                table.appendRows(List.of(
                        row(schema, 2, "Bob", 30)
                ))
        );

        assertEquals(
                2,
                table.appendRows(List.of(
                        row(schema, 3, "Charlie", 35)
                ))
        );

        assertEquals(2, table.version());

        assertTrue(storage.exists(
                "_delta_log/00000000000000000000.json"
        ));

        assertTrue(storage.exists(
                "_delta_log/00000000000000000001.json"
        ));

        assertTrue(storage.exists(
                "_delta_log/00000000000000000002.json"
        ));

        List<Row> rows = table.readRows();

        assertEquals(3, rows.size());
    }

    @Test
    void snapshotAndTimeTravelWorkThroughS3() throws Exception {
        TableSchema schema = schema();

        long v0 = table.appendRows(List.of(
                row(schema, 1, "Alice", 25)
        ));

        long v1 = table.appendRows(List.of(
                row(schema, 2, "Bob", 30)
        ));

        assertEquals(0, v0);
        assertEquals(1, v1);

        assertEquals(
                2,
                table.readRows().size()
        );

        assertEquals(
                1,
                table.readRows(0).size()
        );

        assertEquals(
                2,
                table.readRows(1).size()
        );

        assertEquals(
                1L,
                ((Number) table.readRows(0)
                        .get(0)
                        .get("id"))
                        .longValue()
        );
    }

    @Test
    void queryAndDataSkippingWorkThroughS3() throws Exception {
        TableSchema schema = schema();

        table.appendRows(List.of(
                row(schema, 1, "Alice", 25),
                row(schema, 2, "Bob", 30),
                row(schema, 3, "Charlie", 35)
        ));

        List<Row> result = table.queryRows(
                Map.of(
                        "id",
                        new DeltaTable.QueryRange(2, 2)
                )
        );

        assertEquals(1, result.size());

        assertEquals(
                2L,
                ((Number) result.get(0)
                        .get("id"))
                        .longValue()
        );
    }

    @Test
    void deleteWorksThroughS3() throws Exception {
        TableSchema schema = schema();

        table.appendRows(List.of(
                row(schema, 1, "Alice", 25),
                row(schema, 2, "Bob", 30),
                row(schema, 3, "Charlie", 35)
        ));

        long deleteVersion = table.deleteRows(
                row -> ((Number) row.get("id")).longValue() == 2
        );

        assertEquals(1, deleteVersion);

        List<Row> rows = table.readRows();

        assertEquals(2, rows.size());

        Set<Long> ids = rows.stream()
                .map(row -> ((Number) row.get("id")).longValue())
                .collect(Collectors.toSet());

        assertEquals(Set.of(1L, 3L), ids);

        assertEquals(
                3,
                table.readRows(0).size()
        );
    }

    @Test
    void upsertWorksThroughS3() throws Exception {
        TableSchema schema = schema();

        table.appendRows(List.of(
                row(schema, 1, "Alice", 25),
                row(schema, 2, "Bob", 30)
        ));

        table.upsertRows(
                List.of(
                        row(schema, 2, "Robert", 31),
                        row(schema, 3, "Charlie", 35)
                ),
                "id"
        );

        List<Row> rows = table.readRows();

        assertEquals(3, rows.size());

        Map<Long, Row> byId = rows.stream()
                .collect(Collectors.toMap(
                        row -> ((Number) row.get("id")).longValue(),
                        row -> row
                ));

        assertEquals("Alice", byId.get(1L).get("name"));
        assertEquals("Robert", byId.get(2L).get("name"));
        assertEquals(31, ((Number) byId.get(2L).get("age")).intValue());
        assertEquals("Charlie", byId.get(3L).get("name"));
    }

    @Test
    void checkpointWorksThroughS3() throws Exception {
        S3Storage checkpointStorage = storage;

        DeltaTable checkpointedTable =
                DeltaTable.open(checkpointStorage, 2);

        TableSchema schema = schema();

        checkpointedTable.appendRows(List.of(
                row(schema, 1, "Alice", 25)
        ));

        checkpointedTable.appendRows(List.of(
                row(schema, 2, "Bob", 30)
        ));

        assertEquals(1, checkpointedTable.version());

        assertTrue(
                checkpointStorage.exists(
                        "_delta_log/_last_checkpoint"
                )
        );

        assertTrue(
                checkpointStorage.exists(
                        "_delta_log/00000000000000000001.checkpoint.parquet"
                )
        );

        assertEquals(
                2,
                checkpointedTable.readRows().size()
        );
    }

    @Test
    void optimizeWorksThroughS3() throws Exception {
        TableSchema schema = schema();

        table.appendRows(List.of(
                row(schema, 1, "Alice", 25)
        ));

        table.appendRows(List.of(
                row(schema, 2, "Bob", 30)
        ));

        table.appendRows(List.of(
                row(schema, 3, "Charlie", 35)
        ));

        int filesBefore = table.snapshot()
                .activeFiles()
                .size();

        assertEquals(3, filesBefore);

        long optimizeVersion = table.optimize(
                1024 * 1024
        );

        assertTrue(optimizeVersion >= 3);

        int filesAfter = table.snapshot()
                .activeFiles()
                .size();

        assertEquals(1, filesAfter);

        assertEquals(
                3,
                table.readRows().size()
        );
    }

    @Test
    void physicalS3ObjectsMatchActiveSnapshot() throws Exception {
        TableSchema schema = schema();

        table.appendRows(List.of(
                row(schema, 1, "Alice", 25)
        ));

        table.appendRows(List.of(
                row(schema, 2, "Bob", 30)
        ));

        Snapshot snapshot = table.snapshot();

        Set<String> activeFiles = snapshot.activeFiles()
                .stream()
                .map(AddFile::path)
                .collect(Collectors.toSet());

        List<String> physicalFiles = storage.list("data")
                .stream()
                .filter(path -> path.endsWith(".parquet"))
                .toList();

        assertEquals(
                activeFiles,
                new HashSet<>(physicalFiles)
        );
    }

    @Test
    void transactionLogTailWorksThroughS3() throws Exception {
        TableSchema schema = schema();

        table.appendRows(List.of(
                row(schema, 1, "Alice", 25)
        ));

        table.appendRows(List.of(
                row(schema, 2, "Bob", 30)
        ));

        List<VersionedLogRecord> records =
                table.tail(-1);

        assertFalse(records.isEmpty());

        Set<Long> versions = records.stream()
                .map(VersionedLogRecord::version)
                .collect(Collectors.toSet());

        assertEquals(Set.of(0L, 1L), versions);
    }

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

    private static Row row(
            TableSchema schema,
            long id,
            String name,
            int age
    ) {
        return Row.of(
                schema,
                Map.of(
                        "id", id,
                        "name", name,
                        "age", age
                )
        );
    }

    private void cleanupS3Prefix() throws Exception {
        for (String key : storage.list("data")) {
                storage.delete(key);
        }

        for (String key : storage.list("_delta_log")) {
                storage.delete(key);
        }
        }
}