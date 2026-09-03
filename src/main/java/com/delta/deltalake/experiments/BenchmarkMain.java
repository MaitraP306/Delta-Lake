package com.delta.deltalake.experiments;

import com.delta.deltalake.data.ParquetReader;
import com.delta.deltalake.data.Row;
import com.delta.deltalake.data.TableSchema;
import com.delta.deltalake.storage.LocalStorage;
import com.delta.deltalake.storage.Storage;
import com.delta.deltalake.table.DeltaTable;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class BenchmarkMain {
    private BenchmarkMain() {}

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            usage();
            return;
        }
        switch (args[0]) {
            case "generate" -> {
                require(args, 4);
                generate(Path.of(args[1]), Integer.parseInt(args[2]), Integer.parseInt(args[3]));
            }
            case "all" -> {
                require(args, 6);
                Path root = Path.of(args[1]);
                int files = Integer.parseInt(args[2]);
                int rowsPerFile = Integer.parseInt(args[3]);
                int warmups = Integer.parseInt(args[4]);
                int iterations = Integer.parseInt(args[5]);
                generate(root, files, rowsPerFile);
                runAll(root, warmups, iterations);
            }
            default -> usage();
        }
    }

    private static void generate(Path root, int files, int rowsPerFile) throws Exception {
        if (files <= 0 || rowsPerFile <= 0) throw new IllegalArgumentException("files and rowsPerFile must be > 0");
        deleteRecursively(root);
        Files.createDirectories(root);
        Storage storage = new LocalStorage(root);
        DeltaTable table = DeltaTable.open(storage, Integer.MAX_VALUE, List.of("bucket"));

        TableSchema schema = schema();
        final int filesPerBatch = 1000;
        for (int batchStart = 0; batchStart < files; batchStart += filesPerBatch) {
            int batchEnd = Math.min(files, batchStart + filesPerBatch);
            List<Row> rows = new ArrayList<>((batchEnd - batchStart) * rowsPerFile);
            for (int file = batchStart; file < batchEnd; file++) {
                for (int i = 0; i < rowsPerFile; i++) {
                    long id = (long) file * rowsPerFile + i;
                    Map<String, Object> values = new LinkedHashMap<>();
                    values.put("id", id);
                    values.put("value", (double) id);
                    values.put("bucket", file);
                    rows.add(Row.of(schema, values));
                }
            }
            table.appendRows(rows);
        }
        table.checkpoint();
        System.out.printf("generated root=%s files=%d rows=%d%n", root, files, files * rowsPerFile);
    }

    private static void runAll(Path root, int warmups, int iterations) throws Exception {
        benchmarkMetadata(root, warmups, iterations);
        benchmarkDataSkippingAndLatency(root, warmups, iterations);
    }

    private static void benchmarkMetadata(Path root, int warmups, int iterations) throws Exception {
        LocalStorage storage = new LocalStorage(root);
        for (int i = 0; i < warmups; i++) {
            storage.list("data");
            DeltaTable.open(storage).snapshot();
        }

        List<Long> directoryNanos = new ArrayList<>();
        List<Long> deltaNanos = new ArrayList<>();
        for (int i = 0; i < iterations; i++) {
            long start = System.nanoTime();
            int baselineFiles = storage.list("data").size();
            long baselineElapsed = System.nanoTime() - start;
            start = System.nanoTime();
            int deltaFiles = DeltaTable.open(storage).snapshot().fileCount();
            long deltaElapsed = System.nanoTime() - start;
            if (baselineFiles != deltaFiles) throw new IllegalStateException("Benchmark state mismatch");
            directoryNanos.add(baselineElapsed);
            deltaNanos.add(deltaElapsed);
        }
        System.out.printf("metadata_discovery,directory_listing_ms=%.3f,delta_snapshot_ms=%.3f,speedup=%.2fx%n",
                medianMillis(directoryNanos), medianMillis(deltaNanos),
                medianMillis(directoryNanos) / Math.max(0.000001, medianMillis(deltaNanos)));
    }

    private static void benchmarkDataSkippingAndLatency(Path root, int warmups, int iterations) throws Exception {
        LocalStorage storage = new LocalStorage(root);
        DeltaTable table = DeltaTable.open(storage);
        DeltaTable.QueryRange range = new DeltaTable.QueryRange(0L, 9L);

        List<String> files = storage.list("data").stream().filter(f -> f.endsWith(".parquet")).sorted().toList();
        int candidates = 0;
        for (var file : table.snapshot().activeFiles()) {
            var stats = file.stats();
            if (stats == null) { candidates++; continue; }
            var idStats = stats.columns().get("id");
            if (idStats == null || idStats.min() == null || idStats.max() == null) { candidates++; continue; }
            long min = ((Number) idStats.min()).longValue();
            long max = ((Number) idStats.max()).longValue();
            if (!(max < 0L || min > 9L)) candidates++;
        }

        for (int i = 0; i < warmups; i++) {
            DeltaTable.open(storage).queryRows(Map.of("id", range));
            baselineQuery(storage, files, range);
        }

        List<Long> deltaNanos = new ArrayList<>();
        List<Long> baselineNanos = new ArrayList<>();
        int expectedRows = -1;
        for (int i = 0; i < iterations; i++) {
            long start = System.nanoTime();
            int deltaRows = DeltaTable.open(storage).queryRows(Map.of("id", range)).size();
            deltaNanos.add(System.nanoTime() - start);

            start = System.nanoTime();
            int baselineRows = baselineQuery(storage, files, range);
            baselineNanos.add(System.nanoTime() - start);
            if (expectedRows < 0) expectedRows = deltaRows;
            if (deltaRows != baselineRows || deltaRows != expectedRows) {
                throw new IllegalStateException("Benchmark query result mismatch");
            }
        }
        double skippedPercent = files.isEmpty() ? 0.0 : (100.0 * (files.size() - candidates) / files.size());
        System.out.printf("data_skipping,total_files=%d,candidate_files=%d,files_skipped_pct=%.2f%n", files.size(), candidates, skippedPercent);
        System.out.printf("query_latency,baseline_ms=%.3f,delta_ms=%.3f,speedup=%.2fx%n", medianMillis(baselineNanos), medianMillis(deltaNanos), medianMillis(baselineNanos) / Math.max(0.000001, medianMillis(deltaNanos)));
    }

    private static int baselineQuery(LocalStorage storage, List<String> files, DeltaTable.QueryRange range) throws Exception {
        int count = 0;
        for (String file : files) {
            Path path = storage.root().resolve(file).normalize();
            for (var record : ParquetReader.read(path)) {
                Object raw = record.get("id");
                long id = ((Number) raw).longValue();
                if (id >= ((Number) range.min()).longValue() && id <= ((Number) range.max()).longValue()) count++;
            }
        }
        return count;
    }

    private static TableSchema schema() {
        return Row.infer(Map.of("id", 0L, "value", 0.0, "bucket", 0)).schema();
    }

    private static double medianMillis(List<Long> nanos) {
        List<Long> sorted = new ArrayList<>(nanos);
        sorted.sort(Comparator.naturalOrder());
        long median = sorted.get(sorted.size() / 2);
        return median / 1_000_000.0;
    }

    private static void require(String[] args, int count) {
        if (args.length != count) {
            usage();
            throw new IllegalArgumentException("Expected " + (count - 1) + " arguments after the command");
        }
    }

    private static void usage() {
        System.out.println("Usage:");
        System.out.println("  generate <root> <files> <rowsPerFile>");
        System.out.println("  all <root> <files> <rowsPerFile> <warmups> <iterations>");
    }

    private static void deleteRecursively(Path root) throws Exception {
        if (!Files.exists(root)) return;
        try (var stream = Files.walk(root)) {
            stream.sorted(Comparator.reverseOrder()).forEach(path -> {
                try { Files.deleteIfExists(path); } catch (Exception e) { throw new RuntimeException(e); }
            });
        }
    }
}
