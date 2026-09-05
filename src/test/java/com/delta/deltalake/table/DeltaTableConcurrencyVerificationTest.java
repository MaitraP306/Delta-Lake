package com.delta.deltalake.table;

import com.delta.deltalake.data.Row;
import com.delta.deltalake.log.VersionedLogRecord;
import com.delta.deltalake.storage.LocalStorage;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;

class DeltaTableConcurrencyVerificationTest {

    @Test
    void concurrentIndependentAppendsPreserveAllWrites() throws Exception {
        Path root = Files.createTempDirectory("delta-concurrent-");

        try {
            DeltaTable table = DeltaTable.open(new LocalStorage(root), 1000);

            // Create the table before starting concurrent writers so this test
            // isolates concurrent commit/version allocation.
            table.appendRows(List.of(
                    Row.infer(java.util.Map.of(
                            "id", 0L,
                            "name", "initial"
                    ))
            ));

            int writers = 20;
            List<Long> committedVersions = runConcurrentAppends(root, writers, 1);

            assertEquals(
                    writers,
                    new HashSet<>(committedVersions).size(),
                    "Every concurrent writer should receive a unique commit version"
            );

            DeltaTable finalTable =
                    DeltaTable.open(new LocalStorage(root), 1000);

            assertEquals(
                    writers,
                    finalTable.version(),
                    "The log should contain one new version per successful append"
            );

            assertEquals(
                    writers + 1,
                    finalTable.readRows().size(),
                    "No concurrent append should be lost"
            );

            assertEquals(
                    Set.of(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L,
                            10L, 11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L, 19L, 20L),
                    ids(finalTable.readRows())
            );

            assertContiguousLog(finalTable, 0, writers);
            assertNoOrphanDataFiles(finalTable);

        } finally {
            delete(root);
        }
    }

    @Test
    void concurrentInitialAppendsCreateOneValidTable() throws Exception {
        Path root = Files.createTempDirectory("delta-concurrent-initial-");

        try {
            int writers = 12;

            List<Long> committedVersions =
                    runConcurrentAppends(root, writers, 0);

            assertEquals(
                    writers,
                    new HashSet<>(committedVersions).size(),
                    "Concurrent initial writers must receive unique versions"
            );

            DeltaTable table =
                    DeltaTable.open(new LocalStorage(root), 1000);

            assertEquals(
                    writers - 1,
                    table.version(),
                    "One writer creates version 0 and each remaining writer adds one version"
            );

            assertEquals(
                    writers,
                    table.readRows().size()
            );

            assertEquals(
                    Set.of(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L),
                    ids(table.readRows())
            );

            assertContiguousLog(table, 0, writers - 1);
            assertNoOrphanDataFiles(table);

        } finally {
            delete(root);
        }
    }

    @Test
    void repeatedHighContentionAppendsDoNotStarveWriters() throws Exception {
        for (int round = 0; round < 3; round++) {
            Path root = Files.createTempDirectory(
                    "delta-concurrent-round-" + round + "-"
            );

            try {
                DeltaTable table = DeltaTable.open(
                        new LocalStorage(root),
                        1000
                );

                table.appendRows(List.of(
                        Row.infer(java.util.Map.of(
                                "id", 0L,
                                "name", "initial"
                        ))
                ));

                int writers = 20;

                List<Long> committedVersions =
                        runConcurrentAppends(root, writers, 1);

                assertEquals(
                        writers,
                        new HashSet<>(committedVersions).size(),
                        "Round " + round + " had duplicate commit versions"
                );

                DeltaTable finalTable =
                        DeltaTable.open(new LocalStorage(root), 1000);

                assertEquals(writers, finalTable.version());
                assertEquals(writers + 1, finalTable.readRows().size());
                assertContiguousLog(finalTable, 0, writers);
                assertNoOrphanDataFiles(finalTable);

            } finally {
                delete(root);
            }
        }
    }

    @Test
    void failedConcurrentCommitDoesNotLeaveUncommittedDataFiles()
            throws Exception {

        Path root = Files.createTempDirectory("delta-concurrent-cleanup-");

        try {
            DeltaTable table = DeltaTable.open(
                    new LocalStorage(root),
                    1000
            );

            table.appendRows(List.of(
                    Row.infer(java.util.Map.of(
                            "id", 0L,
                            "name", "initial"
                    ))
            ));

            int writers = 20;
            runConcurrentAppends(root, writers, 1);

            DeltaTable finalTable =
                    DeltaTable.open(new LocalStorage(root), 1000);

            List<String> physicalDataFiles =
                    finalTable.storage().list("data").stream()
                            .filter(path -> path.endsWith(".parquet"))
                            .sorted()
                            .toList();

            long activeDataFiles = finalTable.snapshot()
                    .activeFiles()
                    .stream()
                    .count();

            assertEquals(
                    activeDataFiles,
                    physicalDataFiles.size(),
                    "Failed commit attempts must clean up their uncommitted data files"
            );

        } finally {
            delete(root);
        }
    }

    // -------------------------------------------------------------------------
    // Concurrent append harness
    // -------------------------------------------------------------------------

    private static List<Long> runConcurrentAppends(
            Path root,
            int writers,
            int firstId
    ) throws Exception {
        ExecutorService executor =
                Executors.newFixedThreadPool(writers);

        CountDownLatch ready = new CountDownLatch(writers);
        CountDownLatch start = new CountDownLatch(1);

        List<Future<Long>> futures = new ArrayList<>();

        try {
            for (int i = 0; i < writers; i++) {
                final long id = firstId + i;

                futures.add(executor.submit(() -> {
                    ready.countDown();
                    if (!ready.await(10, java.util.concurrent.TimeUnit.SECONDS)) {
                        throw new AssertionError("Writer did not reach concurrency barrier");
                    }

                    if (!start.await(10, java.util.concurrent.TimeUnit.SECONDS)) {
                        throw new AssertionError("Writer did not receive start signal");
                    }

                    DeltaTable writer = DeltaTable.open(
                            new LocalStorage(root),
                            1000
                    );

                    long version = writer.appendRows(List.of(
                            Row.infer(java.util.Map.of(
                                    "id", id,
                                    "name", "writer-" + id
                            ))
                    ));

                    System.out.println(
                            "writer " + id + " committed version " + version
                    );

                    return version;
                }));
            }

            assertTrue(
                    ready.await(10, java.util.concurrent.TimeUnit.SECONDS),
                    "Not all writers reached the concurrency barrier"
            );

            start.countDown();

            List<Long> versions = new ArrayList<>();

            for (Future<Long> future : futures) {
                versions.add(future.get());
            }

            return versions;

        } finally {
            executor.shutdownNow();
            if (!executor.awaitTermination(
                    10,
                    java.util.concurrent.TimeUnit.SECONDS
            )) {
                throw new AssertionError("Concurrent writer executor did not terminate");
            }
        }
    }

    // -------------------------------------------------------------------------
    // Invariants
    // -------------------------------------------------------------------------

    private static void assertContiguousLog(
            DeltaTable table,
            long firstVersion,
            long lastVersion
    ) throws Exception {
        List<VersionedLogRecord> records = table.tail(firstVersion - 1);

        Set<Long> versions = new HashSet<>();

        for (VersionedLogRecord record : records) {
            versions.add(record.version());
        }

        for (long version = firstVersion; version <= lastVersion; version++) {
            assertTrue(
                    versions.contains(version),
                    "Missing transaction-log version " + version
            );
        }
    }

    private static void assertNoOrphanDataFiles(
            DeltaTable table
    ) throws Exception {
        Set<String> activePaths = table.snapshot()
                .activeFiles()
                .stream()
                .map(file -> file.path())
                .collect(java.util.stream.Collectors.toSet());

        List<String> physicalPaths = table.storage()
                .list("data")
                .stream()
                .filter(path -> path.endsWith(".parquet"))
                .toList();

        assertEquals(
                activePaths.size(),
                physicalPaths.size(),
                "Physical data files should equal active AddFile entries after concurrent appends"
        );

        assertTrue(
                physicalPaths.containsAll(activePaths),
                "Every active AddFile must have a corresponding physical data file"
        );
    }

    private static Set<Long> ids(List<Row> rows) {
        Set<Long> ids = new HashSet<>();

        for (Row row : rows) {
            ids.add(
                    ((Number) row.get("id")).longValue()
            );
        }

        return ids;
    }

    // -------------------------------------------------------------------------
    // Cleanup
    // -------------------------------------------------------------------------

    private static void delete(Path root) throws Exception {
        if (!Files.exists(root)) {
            return;
        }

        for (int attempt = 0; attempt < 20; attempt++) {
            try {
                try (var stream = Files.walk(root)) {
                    stream
                            .sorted(Comparator.reverseOrder())
                            .forEach(path -> {
                                try {
                                    Files.deleteIfExists(path);
                                } catch (Exception ignored) {
                                    // Retry below.
                                }
                            });
                }

                if (!Files.exists(root)) {
                    return;
                }
            } catch (Exception ignored) {
                // Retry below.
            }

            Thread.sleep(25L);
        }

        if (Files.exists(root)) {
            throw new AssertionError(
                    "Could not clean temporary test directory: " + root
            );
        }
    }
}
