package com.delta.deltalake.experiments;

import com.delta.deltalake.data.ParquetReader;
import com.delta.deltalake.data.Row;
import com.delta.deltalake.data.TableSchema;
import com.delta.deltalake.storage.LocalStorage;
import com.delta.deltalake.storage.S3Storage;
import com.delta.deltalake.storage.Storage;
import com.delta.deltalake.table.DeltaTable;
import software.amazon.awssdk.regions.Region;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public final class BenchmarkMain {
    private BenchmarkMain() {}

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            usage();
            return;
        }
        switch (args[0]) {
            case "generate" -> generateCommand(args);
            case "metadata" -> benchmarkCommand(args, "metadata");
            case "data-skipping" -> benchmarkCommand(args, "data-skipping");
            case "z-order" -> benchmarkZOrderCommand(args);
            case "all" -> benchmarkCommand(args, "all");
            default -> usage();
        }
    }

    private static void generateCommand(String[] args) throws Exception {
        requireAtLeast(args, 4);
        Config c = Config.parse(args, 1);
        generate(c);
    }

    private static void benchmarkZOrderCommand(String[] args) throws Exception {
        requireAtLeast(args, 2);
        Config c = Config.parse(args, 1);
        emit(benchmarkZOrder(c), c.output);
    }

    private static void benchmarkCommand(String[] args, String experiment) throws Exception {
        requireAtLeast(args, 2);
        Config c = Config.parse(args, 1);

        if (c.generate) {
            generate(c);
        }

        if ("all".equals(experiment)) {
            BenchmarkResult metadata = benchmarkMetadata(c);
            BenchmarkResult dataSkipping = benchmarkDataSkipping(c);
            emitCombined(c, List.of(metadata, dataSkipping));
            return;
        }

        if ("metadata".equals(experiment)) {
            emit(benchmarkMetadata(c), c.output);
        } else if ("data-skipping".equals(experiment)) {
            emit(benchmarkDataSkipping(c), c.output);
        }
    }

    private static void generate(Config c) throws Exception {
        if (c.files <= 0 || c.rowsPerFile <= 0) {
            throw new IllegalArgumentException("files and rowsPerFile must be > 0");
        }
        if (c.uploadThreads <= 0) {
            throw new IllegalArgumentException("uploadThreads must be > 0");
        }

        if ("local".equals(c.backend)) {
            Path root = Path.of(c.root);
            deleteRecursively(root);
            Files.createDirectories(root);
        }

        try (StorageHandle handle = openStorage(c)) {
            Storage storage = handle.storage();
            DeltaTable table = DeltaTable.open(storage, Integer.MAX_VALUE, List.of("bucket"));
            TableSchema schema = schema();
            final int filesPerBatch = 1000;

            long generationStart = System.nanoTime();

            for (int batchStart = 0; batchStart < c.files; batchStart += filesPerBatch) {

                int batchEnd = Math.min(c.files, batchStart + filesPerBatch);
                List<List<Row>> groups = new ArrayList<>(batchEnd - batchStart);
                for (int file = batchStart; file < batchEnd; file++) {
                    List<Row> rows = new ArrayList<>(c.rowsPerFile);
                    for (int i = 0; i < c.rowsPerFile; i++) {
                        long id = (long) file * c.rowsPerFile + i;
                        Map<String, Object> values = new LinkedHashMap<>();
                        values.put("id", id);
                        values.put("value", (double) id);
                        values.put("bucket", file);

                        rows.add(Row.of(schema, values));
                    }

                    groups.add(rows);
                }

                table.appendRowsParallel(groups, c.uploadThreads);
            }

            table.checkpoint();

            double generationSeconds = (System.nanoTime() - generationStart) / 1_000_000_000.0;

            System.out.printf("generated backend=%s location=%s files=%d rows=%d uploadThreads=%d generation_seconds=%.3f%n", c.backend, c.location(), c.files, c.files * c.rowsPerFile, c.uploadThreads, generationSeconds);
        }
    }

    private static BenchmarkResult benchmarkMetadata(Config c) throws Exception {
        try (StorageHandle handle = openStorage(c)) {
            Storage storage = handle.storage();
            warmupMetadata(storage, c.warmups);
            List<Long> directoryNanos = new ArrayList<>();
            List<Long> deltaNanos = new ArrayList<>();
            for (int i = 0; i < c.iterations; i++) {
                long start = System.nanoTime();
                int baselineFiles = storage.list("data").stream().filter(f -> f.endsWith(".parquet")).toList().size();
                directoryNanos.add(System.nanoTime() - start);

                start = System.nanoTime();
                int deltaFiles = DeltaTable.open(storage).snapshot().fileCount();
                deltaNanos.add(System.nanoTime() - start);
                if (baselineFiles != deltaFiles) throw new IllegalStateException("Benchmark state mismatch");
            }
            double baseline = medianMillis(directoryNanos);
            double delta = medianMillis(deltaNanos);
            BenchmarkResult result = new BenchmarkResult("metadata_discovery", c.backend).put("location", c.location()).put("files", storage.list("data").stream().filter(f -> f.endsWith(".parquet")).count()).put("warmups", c.warmups).put("iterations", c.iterations).put("directory_listing_ms", baseline).put("delta_snapshot_ms", delta).put("speedup", baseline / Math.max(0.000001, delta));
            return result;
        }
    }

    private static BenchmarkResult benchmarkZOrder(Config c) throws Exception {
        if (c.files <= 0 || c.rowsPerFile <= 0) {
            throw new IllegalArgumentException("files and rowsPerFile must be > 0");
        }
        if (c.uploadThreads <= 0) {
            throw new IllegalArgumentException("uploadThreads must be > 0");
        }

        try (StorageHandle globalHandle = openPhase14Storage(c, "global-sort");
        StorageHandle zHandle = openPhase14Storage(c, "z-order")) {
            Storage globalStorage = globalHandle.storage();
            Storage zStorage = zHandle.storage();

            List<Row> rows = phase14Rows(c.files * c.rowsPerFile);

            DeltaTable global = DeltaTable.open(globalStorage);
            DeltaTable zOrder = DeltaTable.open(zStorage);

            appendInFileGroups(global, rowsSorted(rows), c.rowsPerFile, c.uploadThreads);
            appendInFileGroups(zOrder, rows, c.rowsPerFile, c.uploadThreads);
            zOrder.optimizeZOrder("sourceIP", "sourcePort", "destIP", "destPort");

            SnapshotStats globalStats = new SnapshotStats(global.snapshot().activeFiles().stream().toList());
            SnapshotStats zStats = new SnapshotStats(zOrder.snapshot().activeFiles().stream().toList());

            Map<String, Long> probes = new LinkedHashMap<>();
            Row probe = rows.get(0);
            probes.put("sourceIP", ((Number) probe.get("sourceIP")).longValue());
            probes.put("sourcePort", ((Number) probe.get("sourcePort")).longValue());
            probes.put("destIP", ((Number) probe.get("destIP")).longValue());
            probes.put("destPort", ((Number) probe.get("destPort")).longValue());

            Map<String, Object> globalSkipped = new LinkedHashMap<>();
            Map<String, Object> zSkipped = new LinkedHashMap<>();
            double globalTotal = 0.0;
            double zTotal = 0.0;
            for (Map.Entry<String, Long> entry : probes.entrySet()) {
                double gs = skippedPercent(globalStats.files(), entry.getKey(), entry.getValue());
                double zs = skippedPercent(zStats.files(), entry.getKey(), entry.getValue());
                globalSkipped.put(entry.getKey(), gs);
                zSkipped.put(entry.getKey(), zs);
                globalTotal += gs;
                zTotal += zs;
            }

            BenchmarkResult result = new BenchmarkResult("z_ordering", c.backend).put("location", c.location()).put("files", c.files).put("rows", c.files * c.rowsPerFile).put("rows_per_file", c.rowsPerFile).put("global_sort_order", List.of("sourceIP", "sourcePort", "destIP", "destPort")).put("z_order", List.of("sourceIP", "sourcePort", "destIP", "destPort")).put("global_sort_skipped_pct", globalSkipped).put("z_order_skipped_pct", zSkipped).put("global_sort_average_skipped_pct", globalTotal / probes.size()).put("z_order_average_skipped_pct", zTotal / probes.size()).put("global_sort_active_files", globalStats.files().size()).put("z_order_active_files", zStats.files().size());
            return result;
        }
    }

    private static List<Row> phase14Rows(int count) {
        java.util.SplittableRandom random = new java.util.SplittableRandom(20260905L);
        List<Row> result = new ArrayList<>(count);
        TableSchema schema = null;
        for (int i = 0; i < count; i++) {
            long sourceIP = random.nextLong(1L << 32);
            int sourcePort = random.nextInt(1 << 16);
            long destIP = random.nextLong(1L << 32);
            int destPort = random.nextInt(1 << 16);
            Map<String, Object> values = new LinkedHashMap<>();
            values.put("sourceIP", sourceIP);
            values.put("sourcePort", sourcePort);
            values.put("destIP", destIP);
            values.put("destPort", destPort);
            values.put("value", (long) i);
            if (schema == null) {
                Row first = Row.infer(values);
                schema = first.schema();
                result.add(first);
            } else {
                result.add(Row.of(schema, values));
            }
        }
        return result;
    }

    private static List<Row> rowsSorted(List<Row> rows) {
        List<Row> sorted = new ArrayList<>(rows);
        sorted.sort((left, right) -> {
            int c = Long.compare(((Number) left.get("sourceIP")).longValue(), ((Number) right.get("sourceIP")).longValue());
            if (c != 0) return c;
            c = Integer.compare(((Number) left.get("sourcePort")).intValue(), ((Number) right.get("sourcePort")).intValue());
            if (c != 0) return c;
            c = Long.compare(((Number) left.get("destIP")).longValue(), ((Number) right.get("destIP")).longValue());
            if (c != 0) return c;
            return Integer.compare(((Number) left.get("destPort")).intValue(), ((Number) right.get("destPort")).intValue());
        });
        return sorted;
    }

    private static void appendInFileGroups(DeltaTable table, List<Row> rows, int rowsPerFile, int uploadThreads) throws Exception {
        for (int start = 0; start < rows.size(); start += rowsPerFile * 1000) {
            int end = Math.min(rows.size(), start + rowsPerFile * 1000);
            List<List<Row>> groups = new ArrayList<>();
            for (int fileStart = start; fileStart < end; fileStart += rowsPerFile) {
                groups.add(new ArrayList<>(rows.subList(fileStart, Math.min(end, fileStart + rowsPerFile))));
            }
            table.appendRowsParallel(groups, uploadThreads);
        }
    }

    private static double skippedPercent(List<com.delta.deltalake.log.AddFile> files, String column, Object probe) {
        if (files.isEmpty()) return 0.0;
        int candidates = 0;
        for (var file : files) {
            var stats = file.stats();
            if (stats == null) { candidates++; continue; }
            var cs = stats.columns().get(column);
            if (cs == null || cs.min() == null || cs.max() == null) { candidates++; continue; }
            if (compareBenchmarkValues(cs.min(), probe) <= 0 && compareBenchmarkValues(cs.max(), probe) >= 0) candidates++;
        }
        return 100.0 * (files.size() - candidates) / files.size();
    }

    private static int compareBenchmarkValues(Object left, Object right) {
        if (left instanceof Number l && right instanceof Number r) {
            return new java.math.BigDecimal(l.toString()).compareTo(new java.math.BigDecimal(r.toString()));
        }
        return ((Comparable<Object>) left).compareTo(right);
    }

    private static StorageHandle openPhase14Storage(Config c, String layout) {
        if ("local".equals(c.backend)) {
            return new StorageHandle(new LocalStorage(Path.of(c.root).resolve(layout)), null);
        }
        if ("s3".equals(c.backend)) {
            S3Storage storage = new S3Storage(c.bucket, c.prefix + "/" + layout, Region.of(c.region));
            return new StorageHandle(storage, storage);
        }
        throw new IllegalArgumentException("Unknown backend: " + c.backend);
    }

    private record SnapshotStats(List<com.delta.deltalake.log.AddFile> files) {}

    private static BenchmarkResult benchmarkDataSkipping(Config c) throws Exception {
        try (StorageHandle handle = openStorage(c)) {
            Storage storage = handle.storage();
            DeltaTable table = DeltaTable.open(storage);
            DeltaTable.QueryRange range = new DeltaTable.QueryRange(0L, 9L);
            var snapshot = table.snapshot();
            int totalFiles = snapshot.fileCount();
            int candidates = 0;
            for (var file : snapshot.activeFiles()) {
                var stats = file.stats();
                if (stats == null) { candidates++; continue; }
                var idStats = stats.columns().get("id");
                if (idStats == null || idStats.min() == null || idStats.max() == null) { candidates++; continue; }
                long min = ((Number) idStats.min()).longValue();
                long max = ((Number) idStats.max()).longValue();
                if (!(max < 0L || min > 9L)) candidates++;
            }

            for (int i = 0; i < c.warmups; i++) {
                table.queryRows(Map.of("id", range));
            }
            List<Long> deltaNanos = new ArrayList<>();
            int expectedRows = -1;
            for (int i = 0; i < c.iterations; i++) {
                long start = System.nanoTime();
                int rows = table.queryRows(Map.of("id", range)).size();
                deltaNanos.add(System.nanoTime() - start);
                if (expectedRows < 0) expectedRows = rows;
                if (rows != expectedRows) throw new IllegalStateException("Benchmark query result mismatch");
            }

            for (int i = 0; i < c.warmups; i++) {
                int rows = fullScanRows(storage);
                if (rows != expectedRows) throw new IllegalStateException("Full-scan result mismatch");
            }
            List<Long> fullScanNanos = new ArrayList<>();
            for (int i = 0; i < c.iterations; i++) {
                long start = System.nanoTime();
                int rows = fullScanRows(storage);
                fullScanNanos.add(System.nanoTime() - start);
                if (rows != expectedRows) throw new IllegalStateException("Full-scan result mismatch");
            }

            double skipped = totalFiles == 0 ? 0.0 : 100.0 * (totalFiles - candidates) / totalFiles;
            double fullScan = medianMillis(fullScanNanos);
            double deltaQuery = medianMillis(deltaNanos);
            BenchmarkResult result = new BenchmarkResult("data_skipping", c.backend).put("location", c.location()).put("total_files", totalFiles).put("candidate_files", candidates).put("files_skipped_pct", skipped).put("rows_returned", expectedRows).put("warmups", c.warmups).put("iterations", c.iterations).put("full_scan_ms", fullScan).put("delta_query_ms", deltaQuery).put("speedup", fullScan / Math.max(0.000001, deltaQuery));
            return result;
        }
    }

    private static int fullScanRows(Storage storage) throws Exception {
        int rows = 0;
        List<String> files = storage.list("data").stream().filter(f -> f.endsWith(".parquet")).sorted().toList();
        for (String file : files) {
            Path source;
            boolean deleteTemp = false;
            if (storage instanceof LocalStorage local) {
                source = local.root().resolve(file).normalize();
                if (!source.startsWith(local.root())) {
                    throw new IOException("Invalid data path: " + file);
                }
            } else {
                source = Files.createTempFile("delta-full-scan-", ".parquet");
                deleteTemp = true;
                Files.write(source, storage.read(file));
            }

            try {
                for (var record : ParquetReader.read(source)) {
                    Object value = record.get("id");
                    if (value instanceof Number number) {
                        long id = number.longValue();
                        if (id >= 0L && id <= 9L) rows++;
                    }
                }
            } finally {
                if (deleteTemp) Files.deleteIfExists(source);
            }
        }
        return rows;
    }

    private static void warmupMetadata(Storage storage, int warmups) throws Exception {
        for (int i = 0; i < warmups; i++) {
            storage.list("data");
            DeltaTable.open(storage).snapshot();
        }
    }

    private static StorageHandle openStorage(Config c) {
        if ("local".equals(c.backend)) return new StorageHandle(new LocalStorage(Path.of(c.root)), null);
        if ("s3".equals(c.backend)) {
            S3Storage storage = new S3Storage(c.bucket, c.prefix, Region.of(c.region));
            return new StorageHandle(storage, storage);
        }
        throw new IllegalArgumentException("Unknown backend: " + c.backend);
    }

    private static void emitCombined(Config c, List<BenchmarkResult> results) throws Exception {
        Map<String, Object> combined = new LinkedHashMap<>();
        combined.put("benchmark", "all");
        combined.put("backend", c.backend);
        combined.put("location", c.location());
        combined.put("warmups", c.warmups);
        combined.put("iterations", c.iterations);
        combined.put("results", results.stream().map(BenchmarkResult::values).toList());

        String json = new com.fasterxml.jackson.databind.ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(combined);

        System.out.println(json);

        if (c.output != null) {
            Path path = Path.of(c.output);
            if (path.getParent() != null) {
                Files.createDirectories(path.getParent());
            }
            Files.writeString(
                    path,
                    json + System.lineSeparator()
            );
        }
    }

    private static void emit(BenchmarkResult result, String output) throws Exception {
        String json = result.toJson();
        System.out.println(json);
        if (output != null) {
            Path path = Path.of(output);
            if (path.getParent() != null) Files.createDirectories(path.getParent());
            Files.writeString(path, json + System.lineSeparator());
        }
    }

    private static TableSchema schema() {
        return Row.infer(Map.of("id", 0L, "value", 0.0, "bucket", 0)).schema();
    }

    private static double medianMillis(List<Long> nanos) {
        if (nanos.isEmpty()) return 0.0;
        List<Long> sorted = new ArrayList<>(nanos);
        sorted.sort(Comparator.naturalOrder());
        return sorted.get(sorted.size() / 2) / 1_000_000.0;
    }

    private static void deleteRecursively(Path root) throws Exception {
        if (!Files.exists(root)) return;
        try (var stream = Files.walk(root)) {
            stream.sorted(Comparator.reverseOrder()).forEach(path -> {
                try { Files.deleteIfExists(path); } catch (Exception e) { throw new RuntimeException(e); }
            });
        }
    }

    private static void requireAtLeast(String[] args, int count) {
        if (args.length < count) throw new IllegalArgumentException("Insufficient arguments");
    }

    private static void usage() {
        System.out.println("Usage:");
        System.out.println("  generate --backend local --root <path> --files <n> --rows-per-file <n>");
        System.out.println("  generate --backend s3 --bucket <bucket> --prefix <prefix> --region <region> --files <n> --rows-per-file <n>");
        System.out.println("  metadata|data-skipping|z-order|all [options]");
        System.out.println("Options: --warmups <n> --iterations <n> --output <file> --generate --upload-threads <n>");
    }

    private record StorageHandle(Storage storage, AutoCloseable closeable) implements AutoCloseable {
        @Override public void close() throws Exception { if (closeable != null) closeable.close(); }
    }

    private static final class Config {
        String backend = "local";
        String root = "benchmark-data";
        String bucket;
        String prefix = "phase13/" + UUID.randomUUID();
        String region = "us-east-2";
        int files = 10;
        int rowsPerFile = 100;
        int warmups = 3;
        int iterations = 10;
        int uploadThreads = 16;
        String output;
        boolean generate;

        static Config parse(String[] args, int start) {
            Config c = new Config();
            for (int i = start; i < args.length; i++) {
                String a = args[i];
                switch (a) {
                    case "--backend" -> c.backend = next(args, ++i, a);
                    case "--root" -> c.root = next(args, ++i, a);
                    case "--bucket" -> c.bucket = next(args, ++i, a);
                    case "--prefix" -> c.prefix = next(args, ++i, a);
                    case "--region" -> c.region = next(args, ++i, a);
                    case "--files" -> c.files = Integer.parseInt(next(args, ++i, a));
                    case "--rows-per-file" -> c.rowsPerFile = Integer.parseInt(next(args, ++i, a));
                    case "--warmups" -> c.warmups = Integer.parseInt(next(args, ++i, a));
                    case "--iterations" -> c.iterations = Integer.parseInt(next(args, ++i, a));
                    case "--upload-threads" -> c.uploadThreads = Integer.parseInt(next(args, ++i, a));
                    case "--output" -> c.output = next(args, ++i, a);
                    case "--generate" -> c.generate = true;
                    default -> throw new IllegalArgumentException("Unknown option: " + a);
                }
            }
            if (c.warmups < 0 || c.iterations <= 0 || c.uploadThreads <= 0) throw new IllegalArgumentException("warmups >= 0, iterations > 0, and uploadThreads > 0 required");
            if ("s3".equals(c.backend) && (c.bucket == null || c.bucket.isBlank())) throw new IllegalArgumentException("--bucket is required for S3");
            return c;
        }

        String location() { return "local".equals(backend) ? root : "s3://" + bucket + "/" + prefix; }

        private static String next(String[] args, int index, String option) {
            if (index >= args.length) throw new IllegalArgumentException("Missing value for " + option);
            return args[index];
        }
    }
}
