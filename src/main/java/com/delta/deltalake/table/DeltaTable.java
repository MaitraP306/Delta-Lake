package com.delta.deltalake.table;

import com.delta.deltalake.data.ParquetReader;
import com.delta.deltalake.data.ParquetWriter;
import com.delta.deltalake.data.Record;
import com.delta.deltalake.data.RecordCodec;
import com.delta.deltalake.data.Row;
import com.delta.deltalake.data.RowCodec;
import com.delta.deltalake.data.SchemaValidator;
import com.delta.deltalake.data.TableSchema;
import com.delta.deltalake.cache.DeltaCache;
import com.delta.deltalake.log.*;
import com.delta.deltalake.storage.Storage;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

public final class DeltaTable {
    private static final int MAX_COMMIT_RETRIES = 8;
    private static final int DEFAULT_CHECKPOINT_INTERVAL = 10;
    private static final String APPEND_ONLY = "delta.appendOnly";
    private static final String AUTO_OPTIMIZE = "delta.autoOptimize";
    private static final String DELETED_FILE_RETENTION_MILLIS = "delta.deletedFileRetentionMillis";
    private static final long DEFAULT_DELETED_FILE_RETENTION_MILLIS = Duration.ofDays(7).toMillis();
    private static final int AUTO_OPTIMIZE_FILE_THRESHOLD = 4;
    private static final int DATA_CACHE_LIMIT = 128;
    private static final long DEFAULT_OPTIMIZE_TARGET_BYTES = 128L * 1024 * 1024;

    private final Storage storage;
    private final TransactionLog transactionLog;
    private final SnapshotManager snapshotManager;
    private final CheckpointManager checkpointManager;
    private final ObjectMapper mapper;
    private final int checkpointInterval;
    private final List<String> partitionColumns;
    private volatile List<String> effectivePartitionColumnsCache;
    private final DeltaCache<String, List<Row>> dataCache = new DeltaCache<>(DATA_CACHE_LIMIT);

    private DeltaTable(Storage storage, int checkpointInterval, List<String> partitionColumns) {
        this.storage = Objects.requireNonNull(storage);
        this.transactionLog = new TransactionLog(storage);
        this.checkpointManager = new CheckpointManager(storage, transactionLog);
        this.snapshotManager = new SnapshotManager(transactionLog, checkpointManager);
        this.mapper = transactionLog.mapper();
        this.checkpointInterval = checkpointInterval;
        this.partitionColumns = validatePartitionColumns(partitionColumns);
    }

    public static DeltaTable open(Storage storage) { return new DeltaTable(storage, DEFAULT_CHECKPOINT_INTERVAL, List.of()); }
    public static DeltaTable open(Storage storage, int checkpointInterval) { return open(storage, checkpointInterval, List.of()); }

    public static DeltaTable open(Storage storage, int checkpointInterval, List<String> partitionColumns) {
        if (checkpointInterval <= 0) throw new IllegalArgumentException("checkpointInterval must be > 0");
        return new DeltaTable(storage, checkpointInterval, partitionColumns);
    }
    public static DeltaTable open(Storage storage, List<String> partitionColumns) { return new DeltaTable(storage, DEFAULT_CHECKPOINT_INTERVAL, partitionColumns); }

    public boolean exists() throws IOException { return transactionLog.latestVersion() >= 0; }
    public long version() throws IOException { return transactionLog.latestVersion(); }
    public Storage storage() { return storage; }
    public TableSchema currentSchema() throws IOException { return tableSchema(); }

    public Snapshot snapshot() throws IOException {
        long version = version();
        if (version < 0) throw new IllegalStateException("Table does not exist");
        return snapshotManager.loadSnapshot(version);
    }
    public Snapshot snapshot(long version) throws IOException { return snapshotManager.loadSnapshot(version); }
    public OptimisticTransaction beginTransaction() throws IOException { return new OptimisticTransaction(transactionLog, snapshotManager, snapshot()); }

    public long appendRows(List<Row> rows) throws IOException { return appendRows(rows, null, null); }

    public long appendRows(List<Row> rows, String appId, Long txnVersion) throws IOException {
        validateRows(rows);
        if ((appId == null) != (txnVersion == null)) throw new IllegalArgumentException("appId and txnVersion must be supplied together");
        TableSchema writeSchema = exists() ? tableSchema() : rows.get(0).schema();
        validateRowsAgainstSchema(rows, writeSchema);

        List<String> dataPaths = new ArrayList<>();
        long committedVersion = -1;
        try {
            for (int attempt = 0; attempt < MAX_COMMIT_RETRIES; attempt++) {
                long latest = transactionLog.latestVersion();
                if (latest >= 0) assertSupportedProtocol(snapshotManager.loadSnapshot(latest).protocol());
                if (appId != null && latest >= 0 && snapshotManager.loadSnapshot(latest).transactions().values().stream().anyMatch(txn -> txn.appId().equals(appId) && txn.version() >= txnVersion)) {
                    cleanup(dataPaths); return latest;
                }
                cleanup(dataPaths); dataPaths.clear();
                List<LogRecord> actions = new ArrayList<>();
                if (latest < 0) {
                    actions.add(ActionCodec.encode(initialProtocol()));
                    actions.add(ActionCodec.encode(initialMetadata(writeSchema)));
                }
                for (List<Row> group : partitionGroups(rows).values()) {
                    String path = writeDataFile(group, writeSchema);
                    dataPaths.add(path);
                    actions.add(ActionCodec.encode(addFile(path, group, writeSchema, true)));
                }
                actions.add(ActionCodec.encode(new CommitInfo(System.currentTimeMillis(), "WRITE", Map.of("mode", "append"), null)));
                if (appId != null) actions.add(ActionCodec.encode(new Txn(appId, txnVersion, System.currentTimeMillis())));
                long target = latest + 1;
                if (transactionLog.append(target, actions)) { committedVersion = target; break; }
            }
        } catch (IOException | RuntimeException e) {
            cleanup(dataPaths); throw e;
        }
        if (committedVersion < 0) { cleanup(dataPaths); throw new IOException("Could not commit append after " + MAX_COMMIT_RETRIES + " attempts"); }
        checkpointBestEffort(committedVersion);
        maybeAutoOptimize();
        return committedVersion;
    }

    public long append(List<Record> records) throws IOException { return append(records, null, null); }
    public long append(List<Record> records, String appId, Long txnVersion) throws IOException {
        return appendRows(records.stream().map(RecordCodec::toRow).toList(), appId, txnVersion);
    }

    public long deleteRows(Predicate<Row> predicate) throws IOException { return deleteRows(Map.of(), predicate, null); }
    public long deleteRows(Map<String, QueryRange> predicates, Predicate<Row> predicate) throws IOException { return deleteRows(predicates, predicate, null); }
    public long deleteRows(Map<String, QueryRange> predicates, Predicate<Row> predicate, String userMetadata) throws IOException {
        Objects.requireNonNull(predicate); Objects.requireNonNull(predicates);
        for (int attempt = 0; attempt < MAX_COMMIT_RETRIES; attempt++) {
            Snapshot snap = snapshot();
            List<LogRecord> actions = new ArrayList<>();
            List<String> newPaths = new ArrayList<>();
            Set<String> readPaths = new LinkedHashSet<>();
            long removedRows = 0;
            for (AddFile file : snap.activeFiles()) {
                if (!predicates.isEmpty() && !mayMatch(file, predicates, snap.metadata().partitionColumns())) continue;
                readPaths.add(file.path());
                List<Row> rows = projectRows(readDataFile(file.path()), tableSchema());
                List<Row> remaining = rows.stream().filter(predicate.negate()).toList();
                if (remaining.size() == rows.size()) continue;
                removedRows += rows.size() - remaining.size();
                actions.add(ActionCodec.encode(new RemoveFile(file.path(), System.currentTimeMillis(), true)));
                if (!remaining.isEmpty()) {
                    for (List<Row> group : partitionGroups(remaining).values()) {
                        String path = writeDataFile(group, tableSchema()); newPaths.add(path);
                        actions.add(ActionCodec.encode(addFile(path, group, tableSchema(), true)));
                    }
                }
            }
            if (removedRows == 0) return snap.version();
            actions.add(0, ActionCodec.encode(new CommitInfo(System.currentTimeMillis(), "DELETE", Map.of("removedRows", Long.toString(removedRows)), userMetadata)));
            OptimisticTransaction tx = new OptimisticTransaction(transactionLog, snapshotManager, snap);
            tx.readPaths(new java.util.HashSet<>(readPaths));
            if (tx.commit(actions)) { checkpointBestEffort(snap.version() + 1); maybeAutoOptimize(); return snap.version() + 1; }
            cleanup(newPaths);
        }
        throw new IOException("Could not commit delete after " + MAX_COMMIT_RETRIES + " attempts");
    }

    public long delete(Predicate<Record> predicate) throws IOException { return deleteRows(r -> predicate.test(toLegacyRecord(r))); }

    public long upsertRows(List<Row> incoming, String keyColumn) throws IOException { return upsertRows(incoming, keyColumn, null); }
    public long upsertRows(List<Row> incoming, String keyColumn, String userMetadata) throws IOException {
        return mergeRows(incoming, keyColumn, MergeSpec.builder().whenMatchedUpdate((target, source) -> source).whenNotMatchedInsert(source -> source).build(), userMetadata);
    }

    public long mergeRows(List<Row> incoming, String keyColumn, MergeSpec spec) throws IOException { return mergeRows(incoming, keyColumn, spec, null); }
    public long mergeRows(List<Row> incoming, String keyColumn, MergeSpec spec, String userMetadata) throws IOException {
        validateRows(incoming); Objects.requireNonNull(keyColumn); Objects.requireNonNull(spec);
        if (!exists()) {
            if (spec.notMatchedClauses().isEmpty()) return version();
            List<Row> inserts = incoming.stream().filter(source -> spec.shouldInsert(source)).map(spec::insertRow).toList();
            return inserts.isEmpty() ? version() : appendRows(inserts);
        }
        TableSchema schema = tableSchema();
        for (Row source : incoming) {
            if (!source.contains(keyColumn)) throw new IllegalArgumentException("Merge source is missing key column: " + keyColumn);
        }
        if (schema.field(keyColumn) == null) throw new IllegalArgumentException("Unknown merge key: " + keyColumn);
        Map<Object, Row> sourceByKey = uniqueRowsByKey(incoming, keyColumn);

        for (int attempt = 0; attempt < MAX_COMMIT_RETRIES; attempt++) {
            Snapshot snap = snapshot();
            List<LogRecord> actions = new ArrayList<>();
            List<String> newPaths = new ArrayList<>();
            Set<Object> matchedKeys = new HashSet<>();
            Set<String> readPaths = new HashSet<>();

            for (AddFile file : snap.activeFiles()) {
                if (!fileMayContainAnyKey(file, keyColumn, sourceByKey.keySet())) continue;
                readPaths.add(file.path());
                List<Row> rows = projectRows(readDataFile(file.path()), schema);
                List<Row> output = new ArrayList<>(rows.size());
                boolean touched = false;
                for (Row target : rows) {
                    Row source = findEqualKey(sourceByKey, target.get(keyColumn));
                    if (source == null) { output.add(target); continue; }
                    matchedKeys.add(normalizeComparable(target.get(keyColumn)));
                    MergeResult result = spec.apply(target, source);
                    touched |= result.changed();
                    if (!result.deleted()) {
                        Row updated = result.row();
                        validateRowsAgainstSchema(List.of(updated), schema);
                        output.add(Row.of(schema, updated.values()));
                    }
                }
                if (!touched) continue;
                actions.add(ActionCodec.encode(new RemoveFile(file.path(), System.currentTimeMillis(), true)));
                for (List<Row> group : partitionGroups(output).values()) {
                    if (group.isEmpty()) continue;
                    String path = writeDataFile(group, schema); newPaths.add(path);
                    actions.add(ActionCodec.encode(addFile(path, group, schema, true)));
                }
            }
            List<Row> inserts = new ArrayList<>();
            for (Row source : incoming) {
                if (findEqualKey(matchedKeys, normalizeComparable(source.get(keyColumn))) != null) continue;
                if (!spec.shouldInsert(source)) continue;
                Row mapped = spec.insertRow(source);
                Row targetRow = Row.of(schema, mapped.values());
                validateRowsAgainstSchema(List.of(targetRow), schema);
                inserts.add(targetRow);
            }
            for (List<Row> group : partitionGroups(inserts).values()) {
                if (group.isEmpty()) continue;
                String path = writeDataFile(group, schema); newPaths.add(path);
                actions.add(ActionCodec.encode(addFile(path, group, schema, true)));
            }
            if (actions.isEmpty()) return snap.version();
            actions.add(0, ActionCodec.encode(new CommitInfo(System.currentTimeMillis(), "MERGE", Map.of("matchedKey", keyColumn, "matchedClauses", Integer.toString(spec.matchedClauses().size()), "notMatchedClauses", Integer.toString(spec.notMatchedClauses().size())), userMetadata)));
            OptimisticTransaction tx = new OptimisticTransaction(transactionLog, snapshotManager, snap);
            tx.readPaths(readPaths);
            tx.failIfNewFileMatches(file -> fileMayContainAnyKey(file, keyColumn, sourceByKey.keySet()));
            if (tx.commit(actions)) { checkpointBestEffort(snap.version() + 1); maybeAutoOptimize(); return snap.version() + 1; }
            cleanup(newPaths);
        }
        throw new IOException("Could not commit merge after " + MAX_COMMIT_RETRIES + " attempts");
    }

    public long upsert(List<Record> incoming) throws IOException { return upsertRows(incoming.stream().map(RecordCodec::toRow).toList(), "id"); }
    public long merge(List<Record> incoming) throws IOException { return upsert(incoming); }
    public long mergeRows(List<Row> incoming, String keyColumn) throws IOException { return upsertRows(incoming, keyColumn); }

    public List<Row> readRows() throws IOException { return readRows(snapshot()); }
    public List<Row> readRows(long version) throws IOException { return readRows(snapshot(version)); }
    public List<Record> readAll() throws IOException { return toLegacyRecords(readRows()); }
    public List<Record> read(long version) throws IOException { return toLegacyRecords(readRows(version)); }

    public List<Row> queryRows(Predicate<Row> predicate) throws IOException { Objects.requireNonNull(predicate); return readRows().stream().filter(predicate).toList(); }
    public List<Row> queryRows(Map<String, QueryRange> predicates) throws IOException {
        Objects.requireNonNull(predicates); Snapshot snap = snapshot(); List<Row> result = new ArrayList<>();
        for (AddFile file : snap.activeFiles()) {
            if (!mayMatch(file, predicates, snap.metadata().partitionColumns())) continue;
            result.addAll(readDataFile(file.path()).stream().filter(r -> matches(r, predicates)).toList());
        }
        return result;
    }
    public List<Record> query(Predicate<Record> predicate) throws IOException { return queryRows((Row r) -> predicate.test(toLegacyRecord(r))).stream().map(DeltaTable::toLegacyRecord).toList(); }
    public List<Record> query(Map<String, QueryRange> predicates) throws IOException { return toLegacyRecords(queryRows(predicates)); }
    public List<Record> queryIdRange(long minInclusive, long maxInclusive) throws IOException { return query(Map.of("id", new QueryRange(minInclusive, maxInclusive))); }

    public Snapshot snapshotAsOf(Instant timestamp) throws IOException {
        Objects.requireNonNull(timestamp); long latest = version(); if (latest < 0) throw new IllegalStateException("Table does not exist");
        long chosen = -1; for (HistoryEntry entry : history()) if (entry.timestamp() <= timestamp.toEpochMilli()) chosen = Math.max(chosen, entry.version());
        if (chosen < 0) throw new IllegalArgumentException("No table version existed at " + timestamp); return snapshot(chosen);
    }

    public List<HistoryEntry> history() throws IOException {
        List<HistoryEntry> result = new ArrayList<>(); long latest = version();
        for (long v = 0; v <= latest; v++) for (LogRecord record : transactionLog.read(v)) {
            if (!record.type().equalsIgnoreCase("commitInfo")) continue;
            CommitInfo info = mapper.convertValue(record.action(), CommitInfo.class);
            result.add(new HistoryEntry(v, info.timestamp(), info.operation(), info.operationParameters(), info.userMetadata()));
        }
        return result.reversed();
    }

    public List<VersionedLogRecord> tail(long afterVersion) throws IOException {
        long latest = version(); if (afterVersion < -1 || afterVersion > latest) throw new IllegalArgumentException("Invalid starting version: " + afterVersion);
        return transactionLog.tail(afterVersion, latest);
    }

    public List<VersionedLogRecord> incrementalFiles(long afterVersion) throws IOException {
        long latest = version();
        if (afterVersion < -1 || afterVersion > latest) {
            throw new IllegalArgumentException("Invalid starting version: " + afterVersion);
        }
        List<VersionedLogRecord> result = new ArrayList<>();
        for (VersionedLogRecord versioned : transactionLog.tail(afterVersion, latest)) {
            LogRecord record = versioned.record();
            LogAction action = ActionCodec.decode(record.type(), record.action(), mapper);
            if (action instanceof AddFile add && add.dataChange()) {
                result.add(new VersionedLogRecord(versioned.version(), new LogRecord(record.type(), action)));
            }
        }
        return result;
    }

    public final class StreamingConsumer {
        private long lastProcessedVersion = -1;
        public List<VersionedLogRecord> poll() throws IOException {
            List<VersionedLogRecord> result = incrementalFiles(lastProcessedVersion);
            lastProcessedVersion = result.isEmpty() ? version() : result.get(result.size() - 1).version();
            return result;
        }
        public long lastProcessedVersion() { return lastProcessedVersion; }
    }
    public StreamingConsumer streamingConsumer() { return new StreamingConsumer(); }

    public long optimize() throws IOException { return optimize(DEFAULT_OPTIMIZE_TARGET_BYTES); }

    public long optimize(long targetFileBytes) throws IOException {
        if (targetFileBytes <= 0) throw new IllegalArgumentException("targetFileBytes must be > 0");
        for (int attempt = 0; attempt < MAX_COMMIT_RETRIES; attempt++) {
            Snapshot snap = snapshot(); if (snap.fileCount() <= 1) return snap.version();
            TableSchema schema = tableSchema(); List<LogRecord> actions = new ArrayList<>(); List<String> newPaths = new ArrayList<>();
            long totalBytes = snap.activeFiles().stream().mapToLong(AddFile::size).sum(); long totalRows = snap.activeFiles().stream().mapToLong(f -> f.stats() == null ? 0 : f.stats().numRecords()).sum();
            int rowsPerTarget = (int) Math.max(1, Math.round(totalRows * ((double) targetFileBytes / Math.max(1, totalBytes))));
            actions.add(ActionCodec.encode(new CommitInfo(System.currentTimeMillis(), "OPTIMIZE", Map.of("files", Integer.toString(snap.fileCount()), "targetBytes", Long.toString(targetFileBytes)), null)));
            List<Row> rows = readRows(snap);
            for (AddFile file : snap.activeFiles()) actions.add(ActionCodec.encode(new RemoveFile(file.path(), System.currentTimeMillis(), false)));
            for (Map.Entry<String, List<Row>> entry : partitionGroups(rows).entrySet()) {
                List<Row> group = entry.getValue();
                for (int start = 0; start < group.size(); start += rowsPerTarget) {
                    List<Row> chunk = group.subList(start, Math.min(group.size(), start + rowsPerTarget));
                    String path = writeDataFile(chunk, schema); newPaths.add(path); actions.add(ActionCodec.encode(addFile(path, chunk, schema, false)));
                }
            }
            OptimisticTransaction tx = new OptimisticTransaction(transactionLog, snapshotManager, snap);
            tx.readPaths(snap.activeFiles().stream().map(AddFile::path).collect(java.util.stream.Collectors.toSet()));
            if (tx.commit(actions)) { checkpointBestEffort(snap.version() + 1); return snap.version() + 1; }
            cleanup(newPaths);
        }
        throw new IOException("Could not commit optimize after " + MAX_COMMIT_RETRIES + " attempts");
    }

    public long optimizeZOrder(String... columns) throws IOException { return optimizeZOrder(null, columns); }
    public long optimizeZOrder(Map<String, QueryRange> scope, String... columns) throws IOException {
        Objects.requireNonNull(columns); if (columns.length == 0) throw new IllegalArgumentException("Provide at least one Z-order column");
        TableSchema schema = tableSchema(); Set<String> names = new HashSet<>(); for (String c : columns) { if (schema.field(c) == null) throw new IllegalArgumentException("Unknown Z-order column: " + c); if (!names.add(c)) throw new IllegalArgumentException("Duplicate Z-order column"); }
        for (int attempt = 0; attempt < MAX_COMMIT_RETRIES; attempt++) {
            Snapshot snap = snapshot(); if (snap.fileCount() == 0) return snap.version();
            List<Row> source = scope == null || scope.isEmpty() ? readRows(snap) : readCandidateRows(snap, scope);
            if (source.isEmpty()) return snap.version();
            Map<String, Map<Object, Integer>> ranks = buildRanks(source, columns);
            source.sort(Comparator.comparing(r -> zOrderKey(r, columns, ranks)));
            Set<String> affectedPaths = new LinkedHashSet<>();
            for (AddFile file : snap.activeFiles()) {
                if (scope == null || scope.isEmpty() || mayMatch(file, scope, snap.metadata().partitionColumns())) affectedPaths.add(file.path());
            }
            List<LogRecord> actions = new ArrayList<>(); List<String> newPaths = new ArrayList<>();
            actions.add(ActionCodec.encode(new CommitInfo(System.currentTimeMillis(), "ZORDER", Map.of("columns", String.join(",", columns)), null)));
            for (String path : affectedPaths) actions.add(ActionCodec.encode(new RemoveFile(path, System.currentTimeMillis(), false)));
            for (List<Row> group : partitionGroups(source).values()) {
                String path = writeDataFile(group, schema); newPaths.add(path); actions.add(ActionCodec.encode(addFile(path, group, schema, false)));
            }
            OptimisticTransaction tx = new OptimisticTransaction(transactionLog, snapshotManager, snap);
            tx.readPaths(affectedPaths);
            if (tx.commit(actions)) { checkpointBestEffort(snap.version() + 1); return snap.version() + 1; }
            cleanup(newPaths);
        }
        throw new IOException("Could not commit Z-order rewrite after " + MAX_COMMIT_RETRIES + " attempts");
    }

    public Thread optimizeAsync() {
        Thread worker = new Thread(() -> { try { optimize(); } catch (IOException e) { throw new RuntimeException(e); } }, "delta-optimize");
        worker.setDaemon(true); worker.start(); return worker;
    }

    public int vacuum() throws IOException {
        return vacuum(retention());
    }

    public int vacuum(Duration retention) throws IOException {
        Objects.requireNonNull(retention);
        if (retention.isNegative()) throw new IllegalArgumentException("retention must be non-negative");
        Snapshot snap = snapshot();
        long cutoff = System.currentTimeMillis() - retention.toMillis();
        Set<String> protectedPaths = new HashSet<>();
        snap.activeFiles().forEach(f -> protectedPaths.add(f.path()));
        int deleted = 0;
        for (RemoveFile tombstone : snap.tombstones()) {
            if (tombstone.deletionTimestamp() <= cutoff && !protectedPaths.contains(tombstone.path()) && storage.exists(tombstone.path())) {
                storage.delete(tombstone.path());
                deleted++;
            }
        }
        return deleted;
    }

    public Duration retention() throws IOException {
        if (!exists()) return Duration.ofMillis(DEFAULT_DELETED_FILE_RETENTION_MILLIS);
        String configured = snapshot().metadata().configuration().get(DELETED_FILE_RETENTION_MILLIS);
        if (configured == null) return Duration.ofMillis(DEFAULT_DELETED_FILE_RETENTION_MILLIS);
        try {
            long millis = Long.parseLong(configured);
            if (millis < 0) throw new NumberFormatException("negative retention");
            return Duration.ofMillis(millis);
        } catch (NumberFormatException e) {
            throw new IOException("Invalid " + DELETED_FILE_RETENTION_MILLIS + ": " + configured, e);
        }
    }

    public long setRetention(Duration retention) throws IOException {
        Objects.requireNonNull(retention);
        if (retention.isNegative()) throw new IllegalArgumentException("retention must be non-negative");
        Snapshot snap = snapshot();
        Map<String, String> configuration = new LinkedHashMap<>(snap.metadata().configuration());
        configuration.put(DELETED_FILE_RETENTION_MILLIS, Long.toString(retention.toMillis()));
        List<LogRecord> actions = List.of(ActionCodec.encode(new CommitInfo(System.currentTimeMillis(), "SET TBLPROPERTIES", Map.of(DELETED_FILE_RETENTION_MILLIS, Long.toString(retention.toMillis())), null)), ActionCodec.encode(new Metadata(snap.metadata().id(), snap.metadata().format(), snap.metadata().schemaString(), snap.metadata().partitionColumns(), configuration)));
        OptimisticTransaction tx = new OptimisticTransaction(transactionLog, snapshotManager, snap);
        tx.readPaths(new HashSet<>(snap.activeFiles().stream().map(AddFile::path).toList()));
        tx.failOnMetadataChanges();
        if (!tx.commit(actions)) throw new IOException("Could not update retention setting");
        checkpointBestEffort(snap.version() + 1);
        return snap.version() + 1;
    }

    public long rollbackToVersion(long targetVersion) throws IOException {
        Snapshot current = snapshot(targetVersion); Snapshot latest = snapshot(); if (targetVersion == latest.version()) return latest.version();
        for (AddFile file : current.activeFiles()) if (!storage.exists(file.path())) throw new IOException("Cannot rollback: historical data file is missing: " + file.path());
        Set<String> currentPaths = new HashSet<>(); latest.activeFiles().forEach(f -> currentPaths.add(f.path()));
        List<LogRecord> actions = new ArrayList<>();
        for (AddFile file : latest.activeFiles()) if (!current.contains(file.path())) actions.add(ActionCodec.encode(new RemoveFile(file.path(), System.currentTimeMillis(), true)));
        for (AddFile file : current.activeFiles()) if (!currentPaths.contains(file.path())) actions.add(ActionCodec.encode(new AddFile(file.path(), file.size(), file.modificationTime(), true, file.stats())));
        if (current.metadata() != null) actions.add(ActionCodec.encode(current.metadata()));
        if (current.protocol() != null) actions.add(ActionCodec.encode(current.protocol()));
        actions.add(0, ActionCodec.encode(new CommitInfo(System.currentTimeMillis(), "ROLLBACK", Map.of("targetVersion", Long.toString(targetVersion)), null)));
        OptimisticTransaction tx = new OptimisticTransaction(transactionLog, snapshotManager, latest);
        tx.readPaths(new HashSet<>(latest.activeFiles().stream().map(AddFile::path).toList()));
        if (!tx.commit(actions)) throw new IOException("Table changed concurrently; retry rollback");
        checkpointBestEffort(latest.version() + 1); return latest.version() + 1;
    }

    public long upgradeProtocol(int readerVersion, int writerVersion) throws IOException {
        if (readerVersion < 1 || writerVersion < 1) throw new IllegalArgumentException("Protocol versions must be >= 1");
        Snapshot snap = snapshot(); if (readerVersion < snap.protocol().minReaderVersion() || writerVersion < snap.protocol().minWriterVersion()) throw new IllegalArgumentException("Protocol version cannot be downgraded");
        List<LogRecord> actions = List.of(ActionCodec.encode(new CommitInfo(System.currentTimeMillis(), "SET PROTOCOL", Map.of("reader", Integer.toString(readerVersion), "writer", Integer.toString(writerVersion)), null)), ActionCodec.encode(new Protocol(readerVersion, writerVersion)));
        OptimisticTransaction tx = new OptimisticTransaction(transactionLog, snapshotManager, snap);
        tx.readPaths(new HashSet<>(snap.activeFiles().stream().map(AddFile::path).toList()));
        tx.failOnMetadataChanges();
        if (!tx.commit(actions)) throw new IOException("Could not upgrade protocol");
        checkpointBestEffort(snap.version() + 1); return snap.version() + 1;
    }

    public long evolveSchema(TableSchema newSchema) throws IOException { return evolveSchema(newSchema, null); }
    public long evolveSchema(TableSchema newSchema, String userMetadata) throws IOException {
        Objects.requireNonNull(newSchema); Snapshot snap = snapshot(); TableSchema current = tableSchema();
        if (!newSchema.isCompatibleEvolutionFrom(current)) throw new IllegalArgumentException("Schema evolution is not compatible with the current table schema");
        for (String partitionColumn : snap.metadata().partitionColumns()) {
            if (newSchema.field(partitionColumn) == null) throw new IllegalArgumentException("Cannot drop partition column: " + partitionColumn);
        }
        if (newSchema.json().equals(current.json())) return snap.version();

        boolean rewrite = newSchema.requiresPhysicalRewriteFrom(current);
        List<LogRecord> actions = new ArrayList<>();
        List<String> newPaths = new ArrayList<>();
        Map<String, String> parameters = new LinkedHashMap<>();
        parameters.put("operation", "schema evolution");
        parameters.put("fromFields", Integer.toString(current.fieldNames().size()));
        parameters.put("toFields", Integer.toString(newSchema.fieldNames().size()));
        parameters.put("physicalRewrite", Boolean.toString(rewrite));
        actions.add(ActionCodec.encode(new CommitInfo(System.currentTimeMillis(), "ALTER TABLE", parameters, userMetadata)));

        try {
            if (rewrite) {
                for (AddFile file : snap.activeFiles()) {
                    List<Row> rows = projectRows(readDataFile(file.path()), newSchema);
                    actions.add(ActionCodec.encode(new RemoveFile(file.path(), System.currentTimeMillis(), true)));
                    for (List<Row> group : partitionGroups(rows).values()) {
                        if (group.isEmpty()) continue;
                        String path = writeDataFile(group, newSchema); newPaths.add(path);
                        actions.add(ActionCodec.encode(addFile(path, group, newSchema, true)));
                    }
                }
            }
            actions.add(ActionCodec.encode(new Metadata(snap.metadata().id(), snap.metadata().format(), newSchema.json(), snap.metadata().partitionColumns(), snap.metadata().configuration())));
            OptimisticTransaction tx = new OptimisticTransaction(transactionLog, snapshotManager, snap);
            tx.readPaths(new HashSet<>(snap.activeFiles().stream().map(AddFile::path).toList()));
            tx.failOnMetadataChanges();
            if (!tx.commit(actions)) { cleanup(newPaths); throw new IOException("Could not commit schema evolution"); }
            checkpointBestEffort(snap.version() + 1); return snap.version() + 1;
        } catch (IOException | RuntimeException e) {
            cleanup(newPaths); throw e;
        }
    }

    public long setAutoOptimize(boolean enabled) throws IOException {
        Snapshot snap = snapshot(); Map<String, String> configuration = new HashMap<>(snap.metadata().configuration()); configuration.put(AUTO_OPTIMIZE, Boolean.toString(enabled));
        List<LogRecord> actions = List.of(ActionCodec.encode(new CommitInfo(System.currentTimeMillis(), "SET TBLPROPERTIES", Map.of(AUTO_OPTIMIZE, Boolean.toString(enabled)), null)), ActionCodec.encode(new Metadata(snap.metadata().id(), snap.metadata().format(), snap.metadata().schemaString(), snap.metadata().partitionColumns(), configuration)));
        OptimisticTransaction tx = new OptimisticTransaction(transactionLog, snapshotManager, snap);
        tx.readPaths(new HashSet<>(snap.activeFiles().stream().map(AddFile::path).toList()));
        tx.failOnMetadataChanges();
        if (!tx.commit(actions)) throw new IOException("Could not update auto optimize setting");
        checkpointBestEffort(snap.version() + 1); return snap.version() + 1;
    }

    public void checkpoint() throws IOException {
        long latest = version();
        if (latest >= 0) {
            checkpointManager.create(latest);
            snapshotManager.invalidate(latest);
        }
    }

    private void maybeCheckpoint(long version) throws IOException {
        if ((version + 1) % checkpointInterval == 0) {
            checkpointManager.create(version);
            snapshotManager.invalidate(version);
        }
    }
    private void checkpointBestEffort(long version) { try { maybeCheckpoint(version); } catch (IOException e) { System.err.println("Delta checkpoint failed at version " + version + ": " + e.getMessage()); } }

    private Protocol initialProtocol() { return new Protocol(1, 1); }
    private Metadata initialMetadata(TableSchema schema) {
        Map<String, String> configuration = new LinkedHashMap<>();
        configuration.put(APPEND_ONLY, "false");
        configuration.put(AUTO_OPTIMIZE, "false");
        configuration.put(DELETED_FILE_RETENTION_MILLIS, Long.toString(DEFAULT_DELETED_FILE_RETENTION_MILLIS));
        return new Metadata(UUID.randomUUID().toString(), "parquet", schema.json(), partitionColumns, configuration);
    }

    private AddFile addFile(String path, List<Row> rows, TableSchema schema, boolean dataChange) throws IOException {
        long size = storage.size(path);
        long modificationTime = storage.modificationTimeMillis(path);
        Map<String, FileStats.ColumnStats> columns = new LinkedHashMap<>();
        for (Schema.Field field : schema.avroSchema().getFields()) {
            Object min = null, max = null; long nullCount = 0;
            boolean trackBounds = supportsFileStatistics(field.schema());
            for (Row row : rows) {
                Object value = row.get(field.name());
                if (value == null) { nullCount++; continue; }
                if (!trackBounds) continue;
                value = normalizeComparable(value);
                if (min == null || compare(min, value) > 0) min = value;
                if (max == null || compare(max, value) < 0) max = value;
            }
            columns.put(field.name(), new FileStats.ColumnStats(min, max, nullCount));
        }
        return new AddFile(path, size, modificationTime, dataChange, new FileStats(rows.size(), columns));
    }

    private static boolean supportsFileStatistics(Schema schema) {
        Schema base = TableSchema.unwrapNullable(schema);
        if (base.getType() == Schema.Type.ARRAY || base.getType() == Schema.Type.MAP || base.getType() == Schema.Type.RECORD) return false;
        return switch (base.getType()) {
            case INT, LONG, FLOAT, DOUBLE, STRING, BOOLEAN, BYTES -> true;
            default -> false;
        };
    }

    private String writeDataFile(List<Row> rows, TableSchema schema) throws IOException {
        if (rows.isEmpty()) throw new IllegalArgumentException("rows cannot be empty");
        String partitionPath = partitionPath(rows); String dataPath = (partitionPath.isEmpty() ? "data/" : partitionPath + "/") + UUID.randomUUID() + ".parquet";
        Path temp = Files.createTempFile("delta-data-", ".parquet");
        try {
            List<GenericRecord> genericRecords = rows.stream().map(r -> RowCodec.encode(r, schema)).toList();
            ParquetWriter.write(temp, schema.avroSchema(), genericRecords); storage.write(dataPath, temp);
            dataCache.remove(dataPath);
            return dataPath;
        } finally { Files.deleteIfExists(temp); }
    }

    private Map<String, List<Row>> partitionGroups(List<Row> rows) throws IOException {
        List<String> columns = effectivePartitionColumns(); if (columns.isEmpty()) return Map.of("", rows);
        Map<String, List<Row>> groups = new LinkedHashMap<>(); for (Row row : rows) groups.computeIfAbsent(partitionKey(row, columns), k -> new ArrayList<>()).add(row); return groups;
    }
    private String partitionPath(List<Row> rows) throws IOException { List<String> columns = effectivePartitionColumns(); return columns.isEmpty() ? "" : partitionKey(rows.get(0), columns); }
    private String partitionKey(Row row, List<String> columns) {
        StringBuilder key = new StringBuilder("data"); for (String column : columns) key.append('/').append(column).append('=').append(escapePathValue(row.get(column)));
        return key.toString();
    }
    private static String escapePathValue(Object value) { return String.valueOf(value).replace("%", "%25").replace("/", "%2F").replace("=", "%3D"); }
    private static String decodePathValue(String value) { return value.replace("%3D", "=").replace("%2F", "/").replace("%25", "%"); }
    private List<String> effectivePartitionColumns() throws IOException {
        List<String> cached = effectivePartitionColumnsCache;
        if (cached != null) return cached;
        if (!partitionColumns.isEmpty()) {
            effectivePartitionColumnsCache = partitionColumns;
            return partitionColumns;
        }
        if (!exists()) {
            effectivePartitionColumnsCache = List.of();
            return List.of();
        }
        List<String> persisted = snapshot().metadata().partitionColumns();
        effectivePartitionColumnsCache = persisted;
        return persisted;
    }

    private List<Row> readRows(Snapshot snap) throws IOException {
        TableSchema logicalSchema = snap.metadata() == null ? null : TableSchema.fromJson(snap.metadata().schemaString());
        List<Row> rows = new ArrayList<>();
        for (AddFile file : snap.activeFiles()) {
            List<Row> physical = readDataFile(file.path());
            rows.addAll(logicalSchema == null ? physical : projectRows(physical, logicalSchema));
        }
        return rows;
    }

    private static List<Row> projectRows(List<Row> rows, TableSchema targetSchema) {
        return rows.stream().map(row -> {
            Map<String, Object> values = new LinkedHashMap<>();
            for (Schema.Field field : targetSchema.avroSchema().getFields()) {
                String sourceName = row.schema().field(field.name()) != null ? field.name() : null;
                if (sourceName == null) for (String alias : field.aliases()) if (row.schema().field(alias) != null) { sourceName = alias; break; }
                Object value = sourceName != null && row.contains(sourceName) ? row.get(sourceName) : null;
                if (value == null && sourceName == null && field.hasDefaultValue()) value = normalizeAvroDefault(field.defaultVal());
                if (value == null && sourceName != null && !row.contains(sourceName) && field.hasDefaultValue()) value = normalizeAvroDefault(field.defaultVal());
                values.put(field.name(), coerceProjectedValue(field.schema(), value));
            }
            return Row.of(targetSchema, values);
        }).toList();
    }

    private static Object normalizeAvroDefault(Object value) {
        return value instanceof org.apache.avro.JsonProperties.Null ? null : value;
    }

    private static Object coerceProjectedValue(Schema target, Object value) {
        if (value == null) return null;
        Schema base = TableSchema.unwrapNullable(target);
        if (!(value instanceof Number n)) return value;
        return switch (base.getType()) {
            case INT -> n.intValue();
            case LONG -> n.longValue();
            case FLOAT -> n.floatValue();
            case DOUBLE -> n.doubleValue();
            default -> value;
        };
    }
    private List<Row> readDataFile(String path) throws IOException {
        List<Row> cached = dataCache.get(path);
        if (cached != null) return cached;
        Path source;
        boolean deleteTemp = false;
        if (storage instanceof com.delta.deltalake.storage.LocalStorage local) {
            source = local.root().resolve(path).normalize();
            if (!source.startsWith(local.root())) throw new IOException("Invalid data path: " + path);
        } else {
            source = Files.createTempFile("delta-data-read-", ".parquet");
            deleteTemp = true;
            Files.write(source, storage.read(path));
        }
        try {
            List<Row> decoded = ParquetReader.read(source).stream().map((GenericRecord r) -> RowCodec.decode(r)).toList();
            dataCache.put(path, decoded);
            return decoded;
        } finally {
            if (deleteTemp) Files.deleteIfExists(source);
        }
    }

    private boolean mayMatch(AddFile file, Map<String, QueryRange> predicates, List<String> partitionColumns) {
        if (!partitionMayMatch(file.path(), predicates, partitionColumns)) return false;
        FileStats stats = file.stats(); if (stats == null) return true;
        for (Map.Entry<String, QueryRange> entry : predicates.entrySet()) {
            FileStats.ColumnStats cs = stats.columns().get(entry.getKey()); if (cs == null || cs.min() == null || cs.max() == null) continue;
            if (compare(cs.max(), entry.getValue().min()) < 0 || compare(cs.min(), entry.getValue().max()) > 0) return false;
        }
        return true;
    }
    private boolean partitionMayMatch(String path, Map<String, QueryRange> predicates, List<String> columns) {
        if (columns.isEmpty()) return true; String[] segments = path.split("/");
        for (String column : columns) {
            String prefix = column + "="; String segment = Arrays.stream(segments).filter(s -> s.startsWith(prefix)).findFirst().orElse(null); if (segment == null) continue;
            QueryRange range = predicates.get(column); if (range == null) continue; Object raw = decodePathValue(segment.substring(prefix.length()));
            Object value = coercePartitionValue(raw, range.min());
            if (compare(value, range.min()) < 0 || compare(value, range.max()) > 0) return false;
        }
        return true;
    }
    private boolean matches(Row row, Map<String, QueryRange> predicates) { for (Map.Entry<String, QueryRange> e : predicates.entrySet()) { Object value = row.get(e.getKey()); if (value == null || compare(value, e.getValue().min()) < 0 || compare(value, e.getValue().max()) > 0) return false; } return true; }

    private static Object coercePartitionValue(Object raw, Object example) {
        if (raw == null || example == null) return raw;
        if (example instanceof Byte) return Byte.valueOf(raw.toString());
        if (example instanceof Short) return Short.valueOf(raw.toString());
        if (example instanceof Integer) return Integer.valueOf(raw.toString());
        if (example instanceof Long) return Long.valueOf(raw.toString());
        if (example instanceof Float) return Float.valueOf(raw.toString());
        if (example instanceof Double) return Double.valueOf(raw.toString());
        if (example instanceof BigInteger) return new BigInteger(raw.toString());
        if (example instanceof BigDecimal) return new BigDecimal(raw.toString());
        if (example instanceof LocalDate) return LocalDate.parse(raw.toString());
        if (example instanceof Instant) return Instant.parse(raw.toString());
        if (example instanceof UUID) return UUID.fromString(raw.toString());
        if (example instanceof Boolean) return Boolean.valueOf(raw.toString());
        return raw;
    }

    private static int compare(Object left, Object right) {
        if (left == null || right == null) throw new IllegalArgumentException("Query values cannot be null");
        left = normalizeComparable(left); right = normalizeComparable(right);
        if (left instanceof Number l && right instanceof Number r) return new BigDecimal(l.toString()).compareTo(new BigDecimal(r.toString()));
        if (left instanceof ByteBuffer l && right instanceof ByteBuffer r) return compareBytes(l, r);
        if (left instanceof Comparable && left.getClass().isInstance(right)) return compareComparable(left, right);
        throw new IllegalArgumentException("Query values must be mutually comparable: " + left.getClass() + " and " + right.getClass());
    }
    private static int compareBytes(ByteBuffer left, ByteBuffer right) { ByteBuffer a = left.duplicate(), b = right.duplicate(); while (a.hasRemaining() && b.hasRemaining()) { int c = Byte.compare(a.get(), b.get()); if (c != 0) return c; } return Integer.compare(a.remaining(), b.remaining()); }
    private static Object normalizeComparable(Object value) {
        return value instanceof CharSequence && !(value instanceof String) ? value.toString() : value;
    }

    private boolean fileMayContainAnyKey(AddFile file, String keyColumn, Collection<Object> keys) {
        FileStats.ColumnStats stats = file.stats() == null ? null : file.stats().columns().get(keyColumn); if (stats == null || stats.min() == null || stats.max() == null) return true;
        for (Object key : keys) if (compare(stats.min(), key) <= 0 && compare(stats.max(), key) >= 0) return true; return false;
    }
    private static <T> T findEqualKey(Map<Object, T> map, Object key) { for (Map.Entry<Object,T> e : map.entrySet()) if (compare(e.getKey(), key) == 0) return e.getValue(); return null; }
    private static Object findEqualKey(Set<Object> set, Object key) { for (Object candidate : set) if (compare(candidate, key) == 0) return candidate; return null; }
    private static Map<Object, Row> uniqueRowsByKey(List<Row> rows, String keyColumn) { Map<Object, Row> result = new LinkedHashMap<>(); for (Row row : rows) { Object key = row.get(keyColumn); if (key == null) throw new IllegalArgumentException("Merge key cannot be null"); if (findEqualKey(result, key) != null) throw new IllegalArgumentException("Duplicate merge key: " + key); result.put(normalizeComparable(key), row); } return result; }

    private static Map<String, Map<Object, Integer>> buildRanks(List<Row> rows, String[] columns) {
        Map<String, Map<Object, Integer>> result = new HashMap<>();
        for (String column : columns) {
            List<Object> values = rows.stream().map(r -> normalizeComparable(r.get(column))).filter(Objects::nonNull).distinct().sorted(DeltaTable::compare).toList();
            Map<Object,Integer> ranks = new HashMap<>(); for (int i = 0; i < values.size(); i++) ranks.put(values.get(i), i); result.put(column, ranks);
        }
        return result;
    }
    private static BigInteger zOrderKey(Row row, String[] columns, Map<String, Map<Object, Integer>> ranks) {
        BigInteger key = BigInteger.ZERO; int bitWidth = 32;
        for (int bit = bitWidth - 1; bit >= 0; bit--) for (int dimension = 0; dimension < columns.length; dimension++) {
            Integer rank = ranks.get(columns[dimension]).get(normalizeComparable(row.get(columns[dimension]))); int unsigned = (rank == null ? 0 : rank) ^ Integer.MIN_VALUE;
            if (((unsigned >>> bit) & 1) != 0) key = key.setBit((bitWidth - 1 - bit) * columns.length + (columns.length - 1 - dimension));
        }
        return key;
    }

    private List<Row> readCandidateRows(Snapshot snap, Map<String, QueryRange> scope) throws IOException { List<Row> result = new ArrayList<>(); for (AddFile f : snap.activeFiles()) if (mayMatch(f, scope, snap.metadata().partitionColumns())) result.addAll(readDataFile(f.path())); return result; }

    private boolean shouldAutoOptimize() throws IOException { return exists() && Boolean.parseBoolean(snapshot().metadata().configuration().getOrDefault(AUTO_OPTIMIZE, "false")); }
    private void maybeAutoOptimize() throws IOException { if (shouldAutoOptimize() && snapshot().fileCount() >= AUTO_OPTIMIZE_FILE_THRESHOLD) optimize(); }

    private static List<String> validatePartitionColumns(List<String> columns) {
        List<String> result = List.copyOf(columns == null ? List.of() : columns); if (new HashSet<>(result).size() != result.size()) throw new IllegalArgumentException("Duplicate partition column");
        for (String column : result) if (column == null || column.isBlank()) throw new IllegalArgumentException("Partition column cannot be blank"); return result;
    }

    public record MergeContext(Row target, Row source) {}
    public enum MergeAction { UPDATE, DELETE }
    public record MatchedClause(Predicate<MergeContext> condition, MergeAction action, BiFunction<Row, Row, Row> updater) {
        public MatchedClause {
            Objects.requireNonNull(condition); Objects.requireNonNull(action);
            if (action == MergeAction.UPDATE) Objects.requireNonNull(updater);
        }
        private MergeResult apply(Row target, Row source) {
            if (!condition.test(new MergeContext(target, source))) return new MergeResult(target, false, false);
            return action == MergeAction.DELETE ? new MergeResult(null, true, true) : new MergeResult(Objects.requireNonNull(updater.apply(target, source)), true, false);
        }
    }
    public record NotMatchedClause(Predicate<Row> condition, Function<Row, Row> mapper) {
        public NotMatchedClause { Objects.requireNonNull(condition); Objects.requireNonNull(mapper); }
    }
    public record MergeResult(Row row, boolean changed, boolean deleted) {}

    public static final class MergeSpec {
        private final List<MatchedClause> matchedClauses;
        private final List<NotMatchedClause> notMatchedClauses;
        private MergeSpec(List<MatchedClause> matchedClauses, List<NotMatchedClause> notMatchedClauses) {
            this.matchedClauses = List.copyOf(matchedClauses); this.notMatchedClauses = List.copyOf(notMatchedClauses);
        }
        public static Builder builder() { return new Builder(); }
        public List<MatchedClause> matchedClauses() { return matchedClauses; }
        public List<NotMatchedClause> notMatchedClauses() { return notMatchedClauses; }
        private MergeResult apply(Row target, Row source) {
            for (MatchedClause clause : matchedClauses) { MergeResult result = clause.apply(target, source); if (result.changed()) return result; }
            return new MergeResult(target, false, false);
        }
        private boolean shouldInsert(Row source) { for (NotMatchedClause clause : notMatchedClauses) if (clause.condition().test(source)) return true; return false; }
        private Row insertRow(Row source) { for (NotMatchedClause clause : notMatchedClauses) if (clause.condition().test(source)) return Objects.requireNonNull(clause.mapper().apply(source)); throw new IllegalStateException("No matching NOT MATCHED clause"); }

        public static final class Builder {
            private final List<MatchedClause> matched = new ArrayList<>();
            private final List<NotMatchedClause> notMatched = new ArrayList<>();
            public Builder whenMatchedUpdate(BiFunction<Row, Row, Row> updater) { return whenMatchedUpdate(ctx -> true, updater); }
            public Builder whenMatchedUpdate(Predicate<MergeContext> condition, BiFunction<Row, Row, Row> updater) { matched.add(new MatchedClause(condition, MergeAction.UPDATE, updater)); return this; }
            public Builder whenMatchedDelete() { return whenMatchedDelete(ctx -> true); }
            public Builder whenMatchedDelete(Predicate<MergeContext> condition) { matched.add(new MatchedClause(condition, MergeAction.DELETE, null)); return this; }
            public Builder whenNotMatchedInsert(Function<Row, Row> mapper) { return whenNotMatchedInsert(source -> true, mapper); }
            public Builder whenNotMatchedInsert(Predicate<Row> condition, Function<Row, Row> mapper) { notMatched.add(new NotMatchedClause(condition, mapper)); return this; }
            public MergeSpec build() { return new MergeSpec(matched, notMatched); }
        }
    }

    public record QueryRange(Object min, Object max) {
        public QueryRange { Objects.requireNonNull(min); Objects.requireNonNull(max); if (compare(min, max) > 0) throw new IllegalArgumentException("QueryRange min > max"); }
    }

    @SuppressWarnings("unchecked")
    private static int compareComparable(Object left, Object right) { return ((Comparable<Object>) left).compareTo(right); }
    private static void validateRows(List<Row> rows) { if (rows == null || rows.isEmpty()) throw new IllegalArgumentException("Rows cannot be empty"); String schema = rows.get(0).schema().json(); for (Row row : rows) if (!row.schema().json().equals(schema)) throw new IllegalArgumentException("All rows in a batch must share the same schema"); }
    private void validateRowsAgainstSchema(List<Row> rows, TableSchema schema) { for (Row row : rows) SchemaValidator.validate(schema, RowCodec.encode(row, schema)); }
    private TableSchema tableSchema() throws IOException { return TableSchema.fromJson(snapshot().metadata().schemaString()); }
    // private static List<Row> toRows(List<Record> records) { return records.stream().map(RecordCodec::toRow).toList(); }
    private static Record toLegacyRecord(Row row) { Object id = row.get("id"), name = row.get("name"), age = row.get("age"); if (!(id instanceof Number) || name == null || !(age instanceof Number)) throw new IllegalStateException("This API requires legacy columns id/name/age"); return new Record(((Number) id).longValue(), name.toString(), ((Number) age).intValue()); }
    private static List<Record> toLegacyRecords(List<Row> rows) { return rows.stream().map(DeltaTable::toLegacyRecord).toList(); }
    private void cleanup(Collection<String> paths) { for (String path : paths) try { storage.delete(path); } catch (IOException ignored) {} }
    private static void assertSupportedProtocol(Protocol protocol) { if (protocol == null || protocol.minReaderVersion() > 1 || protocol.minWriterVersion() > 1) throw new IllegalStateException("Unsupported table protocol: " + protocol); }
}
