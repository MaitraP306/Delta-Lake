package com.delta.deltalake.table;

import com.delta.deltalake.data.ParquetReader;
import com.delta.deltalake.data.ParquetWriter;
import com.delta.deltalake.data.Record;
import com.delta.deltalake.data.RecordCodec;
import com.delta.deltalake.data.RecordSchema;
import com.delta.deltalake.data.SchemaValidator;
import com.delta.deltalake.data.TableSchema;
import com.delta.deltalake.log.*;
import com.delta.deltalake.storage.Storage;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.function.Predicate;

import org.apache.avro.generic.GenericRecord;

public final class DeltaTable {
    private static final int MAX_COMMIT_RETRIES = 8;
    private static final int DEFAULT_CHECKPOINT_INTERVAL = 10;
    private static final String APPEND_ONLY = "delta.appendOnly";

    private final Storage storage;
    private final TransactionLog transactionLog;
    private final SnapshotManager snapshotManager;
    private final CheckpointManager checkpointManager;
    private final ObjectMapper mapper;
    private final int checkpointInterval;

    private DeltaTable(Storage storage, int checkpointInterval) {
        this.storage = Objects.requireNonNull(storage);
        this.transactionLog = new TransactionLog(storage);
        this.snapshotManager = new SnapshotManager(transactionLog, storage);
        this.checkpointManager = new CheckpointManager(storage, transactionLog);
        this.mapper = transactionLog.mapper();
        this.checkpointInterval = checkpointInterval;
    }

    public static DeltaTable open(Storage storage) {
        return new DeltaTable(storage, DEFAULT_CHECKPOINT_INTERVAL);
    }

    public static DeltaTable open(Storage storage, int checkpointInterval) {
        if (checkpointInterval <= 0) throw new IllegalArgumentException("checkpointInterval must be > 0");
        return new DeltaTable(storage, checkpointInterval);
    }

    public boolean exists() throws IOException {
        return transactionLog.latestVersion() >= 0;
    }

    public long version() throws IOException {
        return transactionLog.latestVersion();
    }

    public Snapshot snapshot() throws IOException {
        long version = version();
        if (version < 0) throw new IllegalStateException("Table does not exist");
        return snapshotManager.loadSnapshot(version);
    }

    public Snapshot snapshot(long version) throws IOException {
        return snapshotManager.loadSnapshot(version);
    }

    public long append(List<Record> records) throws IOException {
        return append(records, null, null);
    }

    public long append(List<Record> records, String appId, Long txnVersion) throws IOException {
        validateRecords(records);
        if (exists()) {
            validateSchema(records);
        }
        if ((appId == null) != (txnVersion == null)) {
            throw new IllegalArgumentException("appId and txnVersion must be supplied together");
        }

        String dataPath = writeDataFile(records);
        long committedVersion = -1;
        try {
            for (int attempt = 0; attempt < MAX_COMMIT_RETRIES; attempt++) {
                long latest = transactionLog.latestVersion();
                if (appId != null && latest >= 0 && snapshotManager.loadSnapshot(latest).transactions().values().stream()
                        .anyMatch(txn -> txn.appId().equals(appId) && txn.version() >= txnVersion)) {
                    storage.delete(dataPath);
                    return latest;
                }
                List<LogRecord> actions = new ArrayList<>();
                if (latest < 0) actions.add(ActionCodec.encode(initialProtocol()));
                if (latest < 0) actions.add(ActionCodec.encode(initialMetadata()));
                actions.add(ActionCodec.encode(addFile(dataPath, records, true)));
                actions.add(ActionCodec.encode(new CommitInfo(
                        System.currentTimeMillis(), "WRITE", Map.of("mode", "append"), null)));
                if (appId != null) actions.add(ActionCodec.encode(new Txn(appId, txnVersion, System.currentTimeMillis())));
                long target = latest + 1;
                if (transactionLog.append(target, actions)) {
                    committedVersion = target;
                    break;
                }
            }
        } catch (IOException e) {
            storage.delete(dataPath);
            throw e;
        }
        if (committedVersion < 0) {
            storage.delete(dataPath);
            throw new IOException("Could not commit append after " + MAX_COMMIT_RETRIES + " attempts");
        }
        checkpointBestEffort(committedVersion);
        return committedVersion;
    }

    public long delete(Predicate<Record> predicate) throws IOException {
        Objects.requireNonNull(predicate);
        for (int attempt = 0; attempt < MAX_COMMIT_RETRIES; attempt++) {
            Snapshot snapshot = snapshot();
            List<LogRecord> actions = new ArrayList<>();
            List<String> newlyWritten = new ArrayList<>();
            long removedRows = 0;
            for (AddFile file : snapshot.activeFiles()) {
                List<Record> rows = readDataFile(file.path());
                List<Record> remaining = rows.stream().filter(predicate.negate()).toList();
                if (remaining.size() == rows.size()) continue;
                removedRows += rows.size() - remaining.size();
                actions.add(ActionCodec.encode(new RemoveFile(file.path(), System.currentTimeMillis(), true)));
                if (!remaining.isEmpty()) {
                    String path = writeDataFile(remaining);
                    newlyWritten.add(path);
                    actions.add(ActionCodec.encode(addFile(path, remaining, true)));
                }
            }
            if (removedRows == 0) return snapshot.version();
            actions.add(0, ActionCodec.encode(new CommitInfo(
                    System.currentTimeMillis(), "DELETE", Map.of("removedRows", Long.toString(removedRows)), null)));
            long target = snapshot.version() + 1;
            if (transactionLog.append(target, actions)) {
                checkpointBestEffort(target);
                return target;
            }
            for (String path : newlyWritten) storage.delete(path);
        }
        throw new IOException("Could not commit delete after " + MAX_COMMIT_RETRIES + " attempts");
    }

    public long upsert(List<Record> incoming) throws IOException {
        validateRecords(incoming);
        if (!exists()) return append(incoming);
        validateSchema(incoming);
        Map<Long, Record> updates = new LinkedHashMap<>();
        incoming.forEach(r -> updates.put(r.id(), r));
        for (int attempt = 0; attempt < MAX_COMMIT_RETRIES; attempt++) {
            Snapshot snapshot = snapshot();
            Map<Long, Record> existing = new LinkedHashMap<>();
            List<Record> all = readAll(snapshot);
            for (Record row : all) existing.put(row.id(), row);
            existing.putAll(updates);
            List<Record> merged = new ArrayList<>(existing.values());
            List<LogRecord> actions = new ArrayList<>();
            for (AddFile file : snapshot.activeFiles()) {
                actions.add(ActionCodec.encode(new RemoveFile(file.path(), System.currentTimeMillis(), true)));
            }
            String path = writeDataFile(merged);
            actions.add(ActionCodec.encode(addFile(path, merged, true)));
            actions.add(0, ActionCodec.encode(new CommitInfo(
                    System.currentTimeMillis(), "MERGE", Map.of("matchedKey", "id"), null)));
            long target = snapshot.version() + 1;
            if (transactionLog.append(target, actions)) {
                checkpointBestEffort(target);
                return target;
            }
            storage.delete(path);
        }
        throw new IOException("Could not commit merge after " + MAX_COMMIT_RETRIES + " attempts");
    }


    public long merge(List<Record> incoming) throws IOException {
        return upsert(incoming);
    }

    public List<Record> readAll() throws IOException {
        return readAll(snapshot());
    }

    public List<Record> read(long version) throws IOException {
        return readAll(snapshot(version));
    }

    public List<Record> query(Predicate<Record> predicate) throws IOException {
        Objects.requireNonNull(predicate);
        return readAll().stream().filter(predicate).toList();
    }

    public List<Record> queryIdRange(long minInclusive, long maxInclusive) throws IOException {
        if (minInclusive > maxInclusive) throw new IllegalArgumentException("minInclusive > maxInclusive");
        List<Record> result = new ArrayList<>();
        for (AddFile file : snapshot().activeFiles()) {
            FileStats stats = file.stats();
            if (stats != null) {
                FileStats.ColumnStats idStats = stats.columns().get("id");
                if (idStats != null && idStats.min() instanceof Long minId && idStats.max() instanceof Long maxId && (maxId < minInclusive || minId > maxInclusive)) {
                    continue;
                }
            }
            result.addAll(readDataFile(file.path()).stream()
                    .filter(r -> r.id() >= minInclusive && r.id() <= maxInclusive)
                    .toList());
        }
        return result;
    }

    public Snapshot snapshotAsOf(Instant timestamp) throws IOException {
        Objects.requireNonNull(timestamp);
        long latest = version();
        if (latest < 0) throw new IllegalStateException("Table does not exist");
        long chosen = -1;
        for (HistoryEntry entry : history()) {
            if (entry.timestamp() <= timestamp.toEpochMilli()) chosen = Math.max(chosen, entry.version());
        }
        if (chosen < 0) throw new IllegalArgumentException("No table version existed at " + timestamp);
        return snapshot(chosen);
    }

    public List<HistoryEntry> history() throws IOException {
        List<HistoryEntry> result = new ArrayList<>();
        long latest = version();
        for (long v = 0; v <= latest; v++) {
            for (LogRecord record : transactionLog.read(v)) {
                if (!record.type().equalsIgnoreCase("commitInfo")) continue;
                CommitInfo info = mapper.convertValue(record.action(), CommitInfo.class);
                result.add(new HistoryEntry(v, info.timestamp(), info.operation(), info.operationParameters(), info.userMetadata()));
            }
        }
        return result.reversed();
    }

    public List<VersionedLogRecord> tail(long afterVersion) throws IOException {
        long latest = version();
        if (afterVersion < -1 || afterVersion > latest) throw new IllegalArgumentException("Invalid starting version: " + afterVersion);
        return transactionLog.tail(afterVersion, latest);
    }

    public long optimize() throws IOException {
        for (int attempt = 0; attempt < MAX_COMMIT_RETRIES; attempt++) {
            Snapshot snapshot = snapshot();
            if (snapshot.fileCount() <= 1) return snapshot.version();
            List<Record> rows = readAll(snapshot);
            String path = writeDataFile(rows);
            List<LogRecord> actions = new ArrayList<>();
            actions.add(ActionCodec.encode(new CommitInfo(
                    System.currentTimeMillis(), "OPTIMIZE", Map.of("files", Integer.toString(snapshot.fileCount())), null)));
            for (AddFile file : snapshot.activeFiles()) {
                actions.add(ActionCodec.encode(new RemoveFile(file.path(), System.currentTimeMillis(), false)));
            }
            actions.add(ActionCodec.encode(addFile(path, rows, false)));
            long target = snapshot.version() + 1;
            if (transactionLog.append(target, actions)) {
                checkpointBestEffort(target);
                return target;
            }
            storage.delete(path);
        }
        throw new IOException("Could not commit optimize after " + MAX_COMMIT_RETRIES + " attempts");
    }

    public int vacuum(Duration retention) throws IOException {
        Objects.requireNonNull(retention);
        if (retention.isNegative()) throw new IllegalArgumentException("retention must be non-negative");
        Snapshot snapshot = snapshot();
        long cutoff = System.currentTimeMillis() - retention.toMillis();
        Set<String> protectedPaths = new HashSet<>();
        snapshot.activeFiles().forEach(f -> protectedPaths.add(f.path()));
        int deleted = 0;
        for (RemoveFile tombstone : snapshot.tombstones()) {
            if (tombstone.deletionTimestamp() <= cutoff && !protectedPaths.contains(tombstone.path()) && storage.exists(tombstone.path())) {
                storage.delete(tombstone.path());
                deleted++;
            }
        }
        return deleted;
    }

    public void checkpoint() throws IOException {
        long latest = version();
        if (latest >= 0) checkpointManager.create(latest);
    }

    private void maybeCheckpoint(long version) throws IOException {
        long transactionCount = version + 1;
        if (transactionCount % checkpointInterval == 0) {
            checkpointManager.create(version);
        }
    }
    private void checkpointBestEffort(long version) {
        try {
            maybeCheckpoint(version);
        } catch (IOException checkpointFailure) {
            System.err.println("Delta checkpoint failed at version " + version + ": " + checkpointFailure.getMessage());
        }
    }

    private Metadata initialMetadata() {
        return new Metadata(UUID.randomUUID().toString(),"parquet", RecordSchema.schema().toString(), List.of(), Map.of(APPEND_ONLY, "false"));
    }

    private Protocol initialProtocol() {
        return new Protocol(1, 1);
    }

    private AddFile addFile(String path, List<Record> records, boolean dataChange) throws IOException {
        long size;
        long modificationTime;
        {
            if (storage instanceof com.delta.deltalake.storage.LocalStorage local) {
                Path p = local.root().resolve(path);
                size = Files.size(p);
                modificationTime = Files.getLastModifiedTime(p).toMillis();
            } else {
                throw new IllegalStateException("Non-local storage is not supported by this build");
            }
        }
        Map<String, FileStats.ColumnStats> columns = Map.of(
                "id",
                new FileStats.ColumnStats(records.stream().map(Record::id).min(Long::compareTo).orElse(null), records.stream().map(Record::id).max(Long::compareTo).orElse(null), 0),
                "age",
                new FileStats.ColumnStats(records.stream().map(Record::age).min(Integer::compareTo).orElse(null), records.stream().map(Record::age).max(Integer::compareTo).orElse(null), 0)
        );

        return new AddFile(path, size, modificationTime, dataChange, new FileStats(records.size(), columns));
    }

    private String writeDataFile(List<Record> records) throws IOException {
        String dataPath = "data/" + UUID.randomUUID() + ".parquet";
        Path temp = Files.createTempFile("delta-data-", ".parquet");
        try {
            List<GenericRecord> genericRecords = records.stream().map(RecordCodec::encode).toList();
            ParquetWriter.write(temp, RecordSchema.schema(), genericRecords);
            storage.write(dataPath, temp);
            return dataPath;
        } finally {
            Files.deleteIfExists(temp);
        }
    }

    private List<Record> readAll(Snapshot snapshot) throws IOException {
        List<Record> rows = new ArrayList<>();
        for (AddFile file : snapshot.activeFiles()) rows.addAll(readDataFile(file.path()));
        return rows;
    }

    private List<Record> readDataFile(String dataPath) throws IOException {
        if (!(storage instanceof com.delta.deltalake.storage.LocalStorage local)) {
            throw new IOException("This reproduction only supports LocalStorage");
        }
        Path source = local.root().resolve(dataPath).normalize();
        if (!source.startsWith(local.root())) throw new IOException("Invalid data path: " + dataPath);
        return ParquetReader.read(source).stream().map(RecordCodec::decode).toList();
    }

    private static void validateRecords(List<Record> records) {
        if (records == null || records.isEmpty()) throw new IllegalArgumentException("Records cannot be empty");
        Set<Long> ids = new HashSet<>();
        for (Record record : records) {
            if (!ids.add(record.id())) throw new IllegalArgumentException("Duplicate id in input: " + record.id());
        }
    }

    private TableSchema tableSchema() throws IOException {
        return TableSchema.fromJson(snapshot().metadata().schemaString());
    }

    private void validateSchema(List<Record> records) throws IOException {
        TableSchema schema = tableSchema();

        for (Record record : records) {
            SchemaValidator.validate(schema, RecordSchema.toGenericRecord(record));
        }
    }
}
