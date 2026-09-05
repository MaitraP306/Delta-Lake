package com.delta.deltalake.table;

import com.delta.deltalake.data.Row;
import com.delta.deltalake.data.TableSchema;
import com.delta.deltalake.log.AddFile;
import com.delta.deltalake.storage.LocalStorage;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DeltaTableQueryVerificationTest {

    @TempDir
    java.nio.file.Path tempDir;

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

    /**
     * Create three separate Delta versions/files.
     *
     * File 1:
     *   ids 1-10
     *
     * File 2:
     *   ids 101-110
     *
     * File 3:
     *   ids 201-210
     */
    private DeltaTable createThreeFileTable(
            LocalStorage storage,
            TableSchema schema) throws Exception {

        DeltaTable table =
                DeltaTable.open(storage);

        List<Row> first = new ArrayList<>();

        for (long id = 1; id <= 10; id++) {
            first.add(
                    row(
                            schema,
                            id,
                            "row-" + id,
                            (int) id
                    )
            );
        }

        table.appendRows(first);

        List<Row> second = new ArrayList<>();

        for (long id = 101; id <= 110; id++) {
            second.add(
                    row(
                            schema,
                            id,
                            "row-" + id,
                            (int) id
                    )
            );
        }

        table.appendRows(second);

        List<Row> third = new ArrayList<>();

        for (long id = 201; id <= 210; id++) {
            third.add(
                    row(
                            schema,
                            id,
                            "row-" + id,
                            (int) id
                    )
            );
        }

        table.appendRows(third);

        return table;
    }

    @Test
    void queryIdRangeReturnsOnlyMatchingRows()
            throws Exception {

        LocalStorage storage =
                new LocalStorage(tempDir);

        TableSchema schema =
                schema();

        DeltaTable table =
                createThreeFileTable(
                        storage,
                        schema
                );

        List<Row> result =
                table.queryRows(
                        Map.of(
                                "id",
                                new DeltaTable.QueryRange(
                                        105L,
                                        108L
                                )
                        )
                );

        assertEquals(
                4,
                result.size()
        );

        List<Long> ids =
                result.stream()
                        .map(row ->
                                ((Number) row.get("id"))
                                        .longValue()
                        )
                        .toList();

        assertEquals(
                List.of(105L, 106L, 107L, 108L),
                ids
        );
    }

    @Test
    void queryIdRangeSkipsFilesOutsideRange()
            throws Exception {

        LocalStorage storage =
                new LocalStorage(tempDir);

        TableSchema schema =
                schema();

        DeltaTable table =
                createThreeFileTable(
                        storage,
                        schema
                );

        Snapshot snapshot =
                table.snapshot();

        /*
         * We should have three active files.
         */
        assertEquals(
                3,
                snapshot.fileCount()
        );

        DeltaTable.QueryRange range =
                new DeltaTable.QueryRange(
                        105L,
                        108L
                );

        /*
         * Verify the file ranges represented by
         * AddFile statistics.
         */
        int candidateFiles = 0;

        for (AddFile file :
                snapshot.activeFiles()) {

            assertNotNull(
                    file.stats()
            );

            var stats =
                    file.stats()
                            .columns()
                            .get("id");

            assertNotNull(stats);

            long min =
                    ((Number) stats.min())
                            .longValue();

            long max =
                    ((Number) stats.max())
                            .longValue();

            boolean mayMatch =
                    max >=
                            ((Number) range.min())
                                    .longValue()
                    &&
                    min <=
                            ((Number) range.max())
                                    .longValue();

            if (mayMatch) {
                candidateFiles++;
            }
        }

        /*
         * Only the 101-110 file can contain
         * ids 105-108.
         */
        assertEquals(
                1,
                candidateFiles
        );
    }

    @Test
    void queryRangeDoesNotReturnRowsOutsideRange()
            throws Exception {

        LocalStorage storage =
                new LocalStorage(tempDir);

        TableSchema schema =
                schema();

        DeltaTable table =
                createThreeFileTable(
                        storage,
                        schema
                );

        List<Row> result =
                table.queryRows(
                        Map.of(
                                "id",
                                new DeltaTable.QueryRange(
                                        105L,
                                        108L
                                )
                        )
                );

        for (Row row : result) {

            long id =
                    ((Number) row.get("id"))
                            .longValue();

            assertTrue(
                    id >= 105L &&
                    id <= 108L
            );
        }
    }

    @Test
    void queryRangeIsInclusive()
            throws Exception {

        LocalStorage storage =
                new LocalStorage(tempDir);

        TableSchema schema =
                schema();

        DeltaTable table =
                createThreeFileTable(
                        storage,
                        schema
                );

        List<Row> result =
                table.queryRows(
                        Map.of(
                                "id",
                                new DeltaTable.QueryRange(
                                        105L,
                                        105L
                                )
                        )
                );

        assertEquals(
                1,
                result.size()
        );

        assertEquals(
                105L,
                ((Number) result.get(0).get("id"))
                        .longValue()
        );
    }

    @Test
    void queryRangeWithNoMatchingFileReturnsEmpty()
            throws Exception {

        LocalStorage storage =
                new LocalStorage(tempDir);

        TableSchema schema =
                schema();

        DeltaTable table =
                createThreeFileTable(
                        storage,
                        schema
                );

        List<Row> result =
                table.queryRows(
                        Map.of(
                                "id",
                                new DeltaTable.QueryRange(
                                        500L,
                                        600L
                                )
                        )
                );

        assertTrue(
                result.isEmpty()
        );
    }

    @Test
    void queryRangeAcrossMultipleFilesReturnsAllMatches()
            throws Exception {

        LocalStorage storage =
                new LocalStorage(tempDir);

        TableSchema schema =
                schema();

        DeltaTable table =
                createThreeFileTable(
                        storage,
                        schema
                );

        /*
         * This range intersects:
         *
         *   File 1: 1-10
         *   File 2: 101-110
         *
         * but not File 3: 201-210.
         */
        List<Row> result =
                table.queryRows(
                        Map.of(
                                "id",
                                new DeltaTable.QueryRange(
                                        5L,
                                        105L
                                )
                        )
                );

        assertEquals(
                11,
                result.size()
        );

        for (Row row : result) {

            long id =
                    ((Number) row.get("id"))
                            .longValue();

            assertTrue(
                    id >= 5L &&
                    id <= 105L
            );
        }
    }

    @Test
    void queryIdRangeConvenienceMethodWorks()
            throws Exception {

        LocalStorage storage =
                new LocalStorage(tempDir);

        TableSchema schema =
                schema();

        DeltaTable table =
                createThreeFileTable(
                        storage,
                        schema
                );

        List<Row> result =
                table.queryRows(
                        Map.of(
                                "id",
                                new DeltaTable.QueryRange(
                                        201L,
                                        205L
                                )
                        )
                );

        assertEquals(
                5,
                result.size()
        );

        assertEquals(
                List.of(
                        201L,
                        202L,
                        203L,
                        204L,
                        205L
                ),
                result.stream()
                        .map(row ->
                                ((Number) row.get("id"))
                                        .longValue()
                        )
                        .toList()
        );
    }

    @Test
    void predicateQueryReturnsCorrectRows()
            throws Exception {

        LocalStorage storage =
                new LocalStorage(tempDir);

        TableSchema schema =
                schema();

        DeltaTable table =
                createThreeFileTable(
                        storage,
                        schema
                );

        /*
         * This tests the Predicate<Row> overload,
         * which is a separate query path from
         * statistics-based data skipping.
         */
        List<Row> result =
                table.queryRows(
                        row ->
                                ((Number) row.get("id"))
                                        .longValue() >= 105L
                                &&
                                ((Number) row.get("id"))
                                        .longValue() <= 108L
                );

        assertEquals(
                4,
                result.size()
        );
    }

    @Test
    void queryAndFullReadProduceSameRows()
            throws Exception {

        LocalStorage storage =
                new LocalStorage(tempDir);

        TableSchema schema =
                schema();

        DeltaTable table =
                createThreeFileTable(
                        storage,
                        schema
                );

        List<Row> expected =
                table.readRows()
                        .stream()
                        .filter(
                                row -> {
                                    long id =
                                            ((Number) row.get("id"))
                                                    .longValue();

                                    return id >= 105L &&
                                           id <= 108L;
                                }
                        )
                        .toList();

        List<Row> actual =
                table.queryRows(
                        Map.of(
                                "id",
                                new DeltaTable.QueryRange(
                                        105L,
                                        108L
                                )
                        )
                );

        assertEquals(
                expected.size(),
                actual.size()
        );

        assertEquals(
                expected.stream()
                        .map(row ->
                                ((Number) row.get("id"))
                                        .longValue()
                        )
                        .toList(),
                actual.stream()
                        .map(row ->
                                ((Number) row.get("id"))
                                        .longValue()
                        )
                        .toList()
        );
    }

    @Test
    void queryDoesNotChangeTableVersion()
            throws Exception {

        LocalStorage storage =
                new LocalStorage(tempDir);

        TableSchema schema =
                schema();

        DeltaTable table =
                createThreeFileTable(
                        storage,
                        schema
                );

        long before =
                table.version();

        table.queryRows(
                Map.of(
                        "id",
                        new DeltaTable.QueryRange(
                                105L,
                                108L
                        )
                )
        );

        assertEquals(
                before,
                table.version()
        );
    }
}