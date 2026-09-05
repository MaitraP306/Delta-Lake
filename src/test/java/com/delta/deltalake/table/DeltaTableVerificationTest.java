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

class DeltaTableVerificationTest {

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

    @Test
    void newTableDoesNotExist() throws Exception {
        LocalStorage storage = new LocalStorage(tempDir);

            DeltaTable table = DeltaTable.open(storage);

            assertFalse(table.exists());
            assertEquals(-1, table.version());
    }

    @Test
    void firstAppendCreatesTable() throws Exception {
        LocalStorage storage = new LocalStorage(tempDir);

            DeltaTable table = DeltaTable.open(storage);

            TableSchema schema = schema();

            long version = table.appendRows(
                    List.of(
                            row(schema, 1L, "Alice", 20)
                    )
            );

            assertEquals(0, version);
            assertTrue(table.exists());
            assertEquals(0, table.version());
    }

    @Test
    void firstAppendCreatesProtocolMetadataLogAndDataFile()
            throws Exception {

        LocalStorage storage = new LocalStorage(tempDir);

            DeltaTable table = DeltaTable.open(storage);

            TableSchema schema = schema();

            table.appendRows(
                    List.of(
                            row(schema, 1L, "Alice", 20),
                            row(schema, 2L, "Bob", 25)
                    )
            );

            assertEquals(0, table.version());

            Snapshot snapshot = table.snapshot();

            /*
             * The first commit must contain protocol information.
             */
            assertNotNull(snapshot.protocol());

            assertEquals(
                    1,
                    snapshot.protocol().minReaderVersion()
            );

            assertEquals(
                    1,
                    snapshot.protocol().minWriterVersion()
            );

            /*
             * The first commit must contain table metadata.
             */
            assertNotNull(snapshot.metadata());

            assertEquals(
                    schema.json(),
                    snapshot.metadata().schemaString()
            );

            /*
             * Two rows written in a single append should result
             * in one data file when the table is not partitioned.
             */
            assertEquals(
                    1,
                    snapshot.fileCount()
            );

            AddFile file =
                    snapshot.activeFiles()
                            .iterator()
                            .next();

            assertNotNull(file);

            assertTrue(
                    file.path().startsWith("data/")
            );

            assertTrue(
                    file.path().endsWith(".parquet")
            );

            assertTrue(
                    storage.exists(file.path())
            );

            assertTrue(
                    storage.size(file.path()) > 0
            );
    }

    @Test
    void appendAndReadRowsRoundTrip() throws Exception {
        LocalStorage storage = new LocalStorage(tempDir);

            DeltaTable table = DeltaTable.open(storage);

            TableSchema schema = schema();

            table.appendRows(
                    List.of(
                            row(schema, 1L, "Alice", 20),
                            row(schema, 2L, "Bob", 25),
                            row(schema, 3L, "Charlie", 30)
                    )
            );

            List<Row> rows = table.readRows();

            assertEquals(3, rows.size());

            assertEquals(
                    1L,
                    rows.get(0).get("id")
            );

            assertEquals(
                    "Alice",
                    rows.get(0).get("name")
            );

            assertEquals(
                    20,
                    rows.get(0).get("age")
            );

            assertEquals(
                    2L,
                    rows.get(1).get("id")
            );

            assertEquals(
                    "Bob",
                    rows.get(1).get("name")
            );

            assertEquals(
                    25,
                    rows.get(1).get("age")
            );

            assertEquals(
                    3L,
                    rows.get(2).get("id")
            );

            assertEquals(
                    "Charlie",
                    rows.get(2).get("name")
            );

            assertEquals(
                    30,
                    rows.get(2).get("age")
            );
    }

    @Test
    void multipleAppendsAccumulateRows() throws Exception {
        LocalStorage storage = new LocalStorage(tempDir);

            DeltaTable table = DeltaTable.open(storage);

            TableSchema schema = schema();

            long version0 = table.appendRows(
                    List.of(
                            row(schema, 1L, "Alice", 20),
                            row(schema, 2L, "Bob", 25)
                    )
            );

            assertEquals(0, version0);
            assertEquals(0, table.version());

            long version1 = table.appendRows(
                    List.of(
                            row(schema, 3L, "Charlie", 30),
                            row(schema, 4L, "David", 35)
                    )
            );

            assertEquals(1, version1);
            assertEquals(1, table.version());

            List<Row> rows = table.readRows();

            assertEquals(4, rows.size());

            assertEquals(1L, rows.get(0).get("id"));
            assertEquals(2L, rows.get(1).get("id"));
            assertEquals(3L, rows.get(2).get("id"));
            assertEquals(4L, rows.get(3).get("id"));
    }

    @Test
    void currentSchemaComesFromMetadata() throws Exception {
        LocalStorage storage = new LocalStorage(tempDir);

            DeltaTable table = DeltaTable.open(storage);

            TableSchema schema = schema();

            table.appendRows(
                    List.of(
                            row(schema, 1L, "Alice", 20)
                    )
            );

            TableSchema current =
                    table.currentSchema();

            assertNotNull(current);

            assertEquals(
                    schema.json(),
                    current.json()
            );

            assertEquals(
                    List.of("id", "name", "age"),
                    current.fieldNames()
            );
    }

    @Test
    void schemaMismatchIsRejectedOnSecondAppend()
            throws Exception {

        LocalStorage storage = new LocalStorage(tempDir);

            DeltaTable table = DeltaTable.open(storage);

            TableSchema original =
                    schema();

            table.appendRows(
                    List.of(
                            row(original, 1L, "Alice", 20)
                    )
            );

            assertEquals(0, table.version());

            /*
             * Completely incompatible schema.
             */
            String incompatibleSchemaJson = """
                    {
                      "type": "record",
                      "name": "DifferentRow",
                      "fields": [
                        {
                          "name": "id",
                          "type": "long"
                        },
                        {
                          "name": "different",
                          "type": "string"
                        }
                      ]
                    }
                    """;

            TableSchema incompatible =
                    new TableSchema(
                            new Schema.Parser()
                                    .parse(incompatibleSchemaJson)
                    );

            Row incompatibleRow =
                    Row.of(
                            incompatible,
                            Map.of(
                                    "id", 2L,
                                    "different", "bad"
                            )
                    );

            assertThrows(
                    IllegalArgumentException.class,
                    () -> table.appendRows(
                            List.of(incompatibleRow)
                    )
            );

            /*
             * Failed append must not create
             * another transaction-log version.
             */
            assertEquals(0, table.version());

            /*
             * Existing data must remain intact.
             */
            List<Row> rows =
                    table.readRows();

            assertEquals(1, rows.size());

            assertEquals(
                    1L,
                    rows.get(0).get("id")
            );

            assertEquals(
                    "Alice",
                    rows.get(0).get("name")
            );

            assertEquals(
                    20,
                    rows.get(0).get("age")
            );
    }

    @Test
    void emptyAppendIsRejected() throws Exception {
        LocalStorage storage = new LocalStorage(tempDir);

            DeltaTable table =
                    DeltaTable.open(storage);

            assertThrows(
                    IllegalArgumentException.class,
                    () -> table.appendRows(List.of())
            );

            /*
             * Empty append should not create
             * a Delta table.
             */
            assertFalse(table.exists());

            assertEquals(
                    -1,
                    table.version()
            );
    }

    @Test
    void snapshotAndReadHistoricalVersion()
            throws Exception {

        LocalStorage storage = new LocalStorage(tempDir);

            DeltaTable table =
                    DeltaTable.open(storage);

            TableSchema schema =
                    schema();

            /*
             * Version 0.
             */
            table.appendRows(
                    List.of(
                            row(schema, 1L, "Alice", 20)
                    )
            );

            assertEquals(
                    0,
                    table.version()
            );

            /*
             * Version 1.
             */
            table.appendRows(
                    List.of(
                            row(schema, 2L, "Bob", 25)
                    )
            );

            assertEquals(
                    1,
                    table.version()
            );

            Snapshot snapshotV0 =
                    table.snapshot(0);

            Snapshot snapshotV1 =
                    table.snapshot(1);

            assertEquals(
                    0,
                    snapshotV0.version()
            );

            assertEquals(
                    1,
                    snapshotV1.version()
            );

            /*
             * Version 0 contains one active file.
             */
            assertEquals(
                    1,
                    snapshotV0.fileCount()
            );

            /*
             * Version 1 contains both files.
             */
            assertEquals(
                    2,
                    snapshotV1.fileCount()
            );

            /*
             * Time travel read.
             */
            List<Row> historicalRows =
                    table.readRows(0);

            assertEquals(
                    1,
                    historicalRows.size()
            );

            assertEquals(
                    1L,
                    historicalRows.get(0).get("id")
            );

            /*
             * Current table contains both rows.
             */
            List<Row> currentRows =
                    table.readRows();

            assertEquals(
                    2,
                    currentRows.size()
            );
    }

    @Test
    void appendCreatesRealParquetDataFile()
            throws Exception {

            LocalStorage storage = new LocalStorage(tempDir);
            DeltaTable table =
                    DeltaTable.open(storage);

            TableSchema schema =
                    schema();

            table.appendRows(
                    List.of(
                            row(schema, 1L, "Alice", 20),
                            row(schema, 2L, "Bob", 25)
                    )
            );

            Snapshot snapshot =
                    table.snapshot();

            assertEquals(
                    1,
                    snapshot.fileCount()
            );

            AddFile file =
                    snapshot.activeFiles()
                            .iterator()
                            .next();

            String dataPath =
                    file.path();

            assertTrue(
                    dataPath.startsWith("data/")
            );

            assertTrue(
                    dataPath.endsWith(".parquet")
            );

            /*
             * LocalStorage stores the logical Delta path
             * underneath the temporary directory.
             */
            Path physicalPath =
                    tempDir.resolve(dataPath);

            assertTrue(
                    Files.exists(physicalPath)
            );

            assertTrue(
                    Files.isRegularFile(physicalPath)
            );

            long actualSize =
                    Files.size(physicalPath);

            assertTrue(
                    actualSize > 0
            );

            /*
             * AddFile metadata should contain
             * the actual Parquet object size.
             */
            assertEquals(
                    actualSize,
                    file.size()
            );
    }
}