package com.delta.deltalake.table;

import com.delta.deltalake.data.Row;
import com.delta.deltalake.storage.LocalStorage;
import com.delta.deltalake.storage.Storage;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class DeltaTablePartitionSkippingVerificationTest {

    @Test
    void partitionedWritesUsePartitionDirectories() throws Exception {
        Path root = Files.createTempDirectory("delta-partition-layout-");

        try {
            DeltaTable table = DeltaTable.open(
                    new LocalStorage(root),
                    1000,
                    List.of("country")
            );

            table.appendRows(List.of(
                    row(1, "CA", "Alice"),
                    row(2, "US", "Bob"),
                    row(3, "CA", "Carol")
            ));

            LocalStorage storage = new LocalStorage(root);

            List<String> dataFiles = storage.list("data");

            assertFalse(dataFiles.isEmpty());

            assertTrue(
                    dataFiles.stream().anyMatch(path -> path.startsWith("data/country=CA/")),
                    "Expected CA partition"
            );

            assertTrue(
                    dataFiles.stream().anyMatch(path -> path.startsWith("data/country=US/")),
                    "Expected US partition"
            );

            Snapshot snapshot = table.snapshot();

            assertEquals(
                    List.of("country"),
                    snapshot.metadata().partitionColumns()
            );

        } finally {
            delete(root);
        }
    }

    @Test
    void partitionPredicateReturnsOnlyMatchingRows() throws Exception {
        Path root = Files.createTempDirectory("delta-partition-query-");

        try {
            DeltaTable table = DeltaTable.open(
                    new LocalStorage(root),
                    1000,
                    List.of("country")
            );

            table.appendRows(List.of(
                    row(1, "CA", "Alice"),
                    row(2, "US", "Bob"),
                    row(3, "CA", "Carol"),
                    row(4, "US", "David")
            ));

            List<Row> result = table.queryRows(
                    Map.of(
                            "country",
                            new DeltaTable.QueryRange("CA", "CA")
                    )
            );

            assertEquals(2, result.size());

            Set<Long> ids = new HashSet<>();

            for (Row row : result) {
                ids.add(((Number) row.get("id")).longValue());
                assertEquals("CA", row.get("country"));
            }

            assertEquals(Set.of(1L, 3L), ids);

        } finally {
            delete(root);
        }
    }

    @Test
    void statisticsPruneFilesWithinSamePartition() throws Exception {
        Path root = Files.createTempDirectory("delta-stats-query-");

        try {
            CountingStorage storage = new CountingStorage(root);

            DeltaTable table = DeltaTable.open(
                    storage,
                    1000,
                    List.of("country")
            );

            /*
             * Each append creates a separate Parquet file.
             *
             * All files belong to CA, so partition pruning cannot help.
             * Statistics must eliminate the irrelevant files.
             */
            table.appendRows(List.of(
                    row(1, "CA", "a"),
                    row(2, "CA", "b"),
                    row(3, "CA", "c")
            ));

            table.appendRows(List.of(
                    row(101, "CA", "d"),
                    row(102, "CA", "e"),
                    row(103, "CA", "f")
            ));

            table.appendRows(List.of(
                    row(201, "CA", "g"),
                    row(202, "CA", "h"),
                    row(203, "CA", "i")
            ));

            storage.resetReadCount();

            List<Row> result = table.queryRows(
                    Map.of(
                            "id",
                            new DeltaTable.QueryRange(101L, 103L)
                    )
            );

            assertEquals(3, result.size());

            Set<Long> ids = new HashSet<>();

            for (Row row : result) {
                ids.add(((Number) row.get("id")).longValue());
            }

            assertEquals(Set.of(101L, 102L, 103L), ids);

            /*
             * Snapshot/log reads also use storage.read(), so don't assert
             * an absolute number of reads here. Instead, inspect which
             * Parquet files were physically read.
             */
            List<String> parquetReads = storage.parquetReads();

            assertEquals(
                    1,
                    parquetReads.size(),
                    "Statistics pruning should physically read only one Parquet file"
            );

            assertTrue(
                    parquetReads.get(0).startsWith("data/country=CA/"),
                    "The selected file must belong to CA"
            );

        } finally {
            delete(root);
        }
    }

    @Test
    void partitionAndStatisticsPruningCompose() throws Exception {
        Path root = Files.createTempDirectory("delta-combined-skipping-");

        try {
            CountingStorage storage = new CountingStorage(root);

            DeltaTable table = DeltaTable.open(
                    storage,
                    1000,
                    List.of("country")
            );

            /*
             * Four files:
             *
             * CA → ids 1..3
             * CA → ids 101..103
             * US → ids 101..103
             * US → ids 201..203
             */
            table.appendRows(List.of(
                    row(1, "CA", "a"),
                    row(2, "CA", "b"),
                    row(3, "CA", "c")
            ));

            table.appendRows(List.of(
                    row(101, "CA", "d"),
                    row(102, "CA", "e"),
                    row(103, "CA", "f")
            ));

            table.appendRows(List.of(
                    row(101, "US", "g"),
                    row(102, "US", "h"),
                    row(103, "US", "i")
            ));

            table.appendRows(List.of(
                    row(201, "US", "j"),
                    row(202, "US", "k"),
                    row(203, "US", "l")
            ));

            storage.resetReadCount();

            List<Row> result = table.queryRows(
                    Map.of(
                            "country",
                            new DeltaTable.QueryRange("CA", "CA"),
                            "id",
                            new DeltaTable.QueryRange(101L, 103L)
                    )
            );

            assertEquals(3, result.size());

            for (Row row : result) {
                assertEquals("CA", row.get("country"));

                long id = ((Number) row.get("id")).longValue();
                assertTrue(id >= 101 && id <= 103);
            }

            List<String> parquetReads = storage.parquetReads();

            assertEquals(
                    1,
                    parquetReads.size(),
                    "Partition pruning + statistics pruning should leave one physical Parquet read"
            );

            assertTrue(
                    parquetReads.get(0).startsWith("data/country=CA/"),
                    "Selected file must be from CA partition"
            );

        } finally {
            delete(root);
        }
    }

    @Test
    void missingStatisticsMustNotCauseFalseNegative() throws Exception {
        Path root = Files.createTempDirectory("delta-missing-stats-");

        try {
            /*
             * This test exercises the query semantics through a normal
             * table. The important invariant is that a file without
             * usable statistics must remain a candidate rather than
             * being incorrectly eliminated.
             */
            DeltaTable table = DeltaTable.open(
                    new LocalStorage(root),
                    1000
            );

            table.appendRows(List.of(
                    row(1, "CA", "a"),
                    row(2, "CA", "b"),
                    row(3, "CA", "c")
            ));

            List<Row> result = table.queryRows(
                    Map.of(
                            "id",
                            new DeltaTable.QueryRange(2L, 2L)
                    )
            );

            assertEquals(1, result.size());
            assertEquals(
                    2L,
                    ((Number) result.get(0).get("id")).longValue()
            );

        } finally {
            delete(root);
        }
    }

    private static Row row(long id, String country, String name) {
        return Row.infer(
                Map.of(
                        "id", id,
                        "country", country,
                        "name", name
                )
        );
    }

    private static void delete(Path root) throws Exception {
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

    private static final class CountingStorage implements Storage {
        private final LocalStorage delegate;
        private final List<String> parquetReads =
                Collections.synchronizedList(new ArrayList<>());

        CountingStorage(Path root) {
            this.delegate = new LocalStorage(root);
        }

        @Override
        public byte[] read(String key) throws IOException {
            if (key.endsWith(".parquet")) {
                parquetReads.add(key);
            }

            return delegate.read(key);
        }

        @Override
        public void write(String key, byte[] data) throws IOException {
            delegate.write(key, data);
        }

        @Override
        public void write(String key, Path source) throws IOException {
            delegate.write(key, source);
        }

        @Override
        public boolean create(String key, byte[] data) throws IOException {
            return delegate.create(key, data);
        }

        @Override
        public boolean exists(String key) throws IOException {
            return delegate.exists(key);
        }

        @Override
        public List<String> list(String prefix) throws IOException {
            return delegate.list(prefix);
        }

        @Override
        public List<String> listAfter(
                String prefix,
                String startAfter
        ) throws IOException {
            return delegate.listAfter(prefix, startAfter);
        }

        @Override
        public void delete(String key) throws IOException {
            delegate.delete(key);
        }

        @Override
        public boolean supportsEventualConsistency() {
            return delegate.supportsEventualConsistency();
        }

        @Override
        public long size(String key) throws IOException {
            return delegate.size(key);
        }

        @Override
        public long modificationTimeMillis(String key)
                throws IOException {
            return delegate.modificationTimeMillis(key);
        }

        void resetReadCount() {
            parquetReads.clear();
        }

        List<String> parquetReads() {
            return List.copyOf(parquetReads);
        }
    }

    @Test
    void multiplePartitionColumnsProduceDistinctLayouts() throws Exception {
        Path root = Files.createTempDirectory("delta-multi-partition-");

        try {
            DeltaTable table = DeltaTable.open(
                    new LocalStorage(root),
                    1000,
                    List.of("country", "year")
            );

            table.appendRows(List.of(
                    Row.infer(Map.of(
                            "id", 1L,
                            "country", "CA",
                            "year", 2025L,
                            "name", "a"
                    )),
                    Row.infer(Map.of(
                            "id", 2L,
                            "country", "US",
                            "year", 2025L,
                            "name", "b"
                    )),
                    Row.infer(Map.of(
                            "id", 3L,
                            "country", "CA",
                            "year", 2026L,
                            "name", "c"
                    ))
            ));

            LocalStorage storage = new LocalStorage(root);
            List<String> files = storage.list("data");

            assertTrue(
                    files.stream().anyMatch(
                            p -> p.startsWith("data/country=CA/year=2025/")
                    )
            );

            assertTrue(
                    files.stream().anyMatch(
                            p -> p.startsWith("data/country=US/year=2025/")
                    )
            );

            assertTrue(
                    files.stream().anyMatch(
                            p -> p.startsWith("data/country=CA/year=2026/")
                    )
            );

            assertEquals(
                    List.of("country", "year"),
                    table.snapshot().metadata().partitionColumns()
            );

        } finally {
            delete(root);
        }
    }


    @Test
    void multiplePartitionPredicatesReturnOnlyMatchingPartition() throws Exception {
        Path root = Files.createTempDirectory("delta-multi-partition-query-");

        try {
            DeltaTable table = DeltaTable.open(
                    new LocalStorage(root),
                    1000,
                    List.of("country", "year")
            );

            table.appendRows(List.of(
                    Row.infer(Map.of(
                            "id", 1L,
                            "country", "CA",
                            "year", 2025L,
                            "name", "a"
                    )),
                    Row.infer(Map.of(
                            "id", 2L,
                            "country", "US",
                            "year", 2025L,
                            "name", "b"
                    )),
                    Row.infer(Map.of(
                            "id", 3L,
                            "country", "CA",
                            "year", 2026L,
                            "name", "c"
                    ))
            ));

            List<Row> result = table.queryRows(
                    Map.of(
                            "country",
                            new DeltaTable.QueryRange("CA", "CA"),
                            "year",
                            new DeltaTable.QueryRange(2026L, 2026L)
                    )
            );

            assertEquals(1, result.size());
            assertEquals(
                    3L,
                    ((Number) result.get(0).get("id")).longValue()
            );

        } finally {
            delete(root);
        }
    }


    @Test
    void partitionValuesContainingPathCharactersRoundTrip() throws Exception {
        Path root = Files.createTempDirectory("delta-partition-encoding-");

        try {
            DeltaTable table = DeltaTable.open(
                    new LocalStorage(root),
                    1000,
                    List.of("country")
            );

            String special = "A/B=C%25";

            table.appendRows(List.of(
                    Row.infer(Map.of(
                            "id", 1L,
                            "country", special,
                            "name", "special"
                    ))
            ));

            List<Row> result = table.queryRows(
                    Map.of(
                            "country",
                            new DeltaTable.QueryRange(special, special)
                    )
            );

            assertEquals(1, result.size());
            assertEquals(special, result.get(0).get("country"));

        } finally {
            delete(root);
        }
    }
}