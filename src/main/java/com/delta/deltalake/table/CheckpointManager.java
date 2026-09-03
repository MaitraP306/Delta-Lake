package com.delta.deltalake.table;

import com.delta.deltalake.data.CheckpointCodec;
import com.delta.deltalake.data.CheckpointParquetReader;
import com.delta.deltalake.data.CheckpointParquetWriter;
import com.delta.deltalake.data.CheckpointSchema;
import com.delta.deltalake.cache.DeltaCache;
import com.delta.deltalake.log.LastCheckpoint;
import com.delta.deltalake.log.LogAction;
import com.delta.deltalake.log.RemoveFile;
import com.delta.deltalake.log.Metadata;
import com.delta.deltalake.log.Protocol;
import com.delta.deltalake.log.TransactionLog;
import com.delta.deltalake.storage.Storage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.generic.GenericRecord;

public final class CheckpointManager {
    private static final String DELETED_FILE_RETENTION_MILLIS = "delta.deletedFileRetentionMillis";
    private static final long DEFAULT_DELETED_FILE_RETENTION_MILLIS = Duration.ofDays(7).toMillis();
    private static final Object LAST_CHECKPOINT_LOCK = new Object();

    private final Storage storage;
    private final TransactionLog transactionLog;
    private final DeltaCache<Long, List<LogAction>> loadCache = new DeltaCache<>(16);

    public CheckpointManager(Storage storage, TransactionLog transactionLog) {
        this.storage = storage;
        this.transactionLog = transactionLog;
    }

    public void create(long version) throws IOException {
        Snapshot snapshot = new SnapshotManager(transactionLog, storage).loadSnapshot(version, false);
        List<LogAction> actions = checkpointActions(snapshot);
        List<GenericRecord> rows = actions.stream().map(CheckpointCodec::encode).toList();
        Path temp = Files.createTempFile("delta-checkpoint-", ".parquet");

        try {
            CheckpointParquetWriter.write(temp, CheckpointSchema.schema(), rows);
            boolean created = storage.create(checkpointPath(version), Files.readAllBytes(temp));
            if (created || storage.exists(checkpointPath(version))) {
                publishLastCheckpointIfNewer(version);
            }
            synchronized (loadCache) {
                loadCache.remove(version);
            }
        } finally {
            Files.deleteIfExists(temp);
        }
    }

    public boolean exists(long version) throws IOException {
        return storage.exists(checkpointPath(version));
    }

    public List<LogAction> load(long version) throws IOException {
        List<LogAction> cached = loadCache.get(version);
        if (cached != null) return cached;
        Path temp = Files.createTempFile("delta-checkpoint-read-", ".parquet");
        try {
            Files.write(temp, storage.read(checkpointPath(version)));
            List<LogAction> actions = CheckpointParquetReader.read(temp).parallelStream().map((GenericRecord r) -> CheckpointCodec.decode(r)).toList();
            synchronized (loadCache) {
                loadCache.put(version, actions);
            }
            return actions;
        } finally {
            Files.deleteIfExists(temp);
        }
    }

    private List<LogAction> checkpointActions(Snapshot snapshot) throws IOException {
        long cutoff = System.currentTimeMillis() - retentionMillis(snapshot.metadata());
        List<LogAction> actions = new ArrayList<>();
        Protocol protocol = snapshot.protocol();
        Metadata metadata = snapshot.metadata();
        if (protocol != null) actions.add(protocol);
        if (metadata != null) actions.add(metadata);
        actions.addAll(snapshot.activeFiles());
        for (RemoveFile tombstone : snapshot.tombstones()) {
            if (tombstone.deletionTimestamp() > cutoff || storage.exists(tombstone.path())) {
                actions.add(tombstone);
            }
        }
        actions.addAll(snapshot.transactions().values());
        return actions;
    }

    private long retentionMillis(Metadata metadata) throws IOException {
        if (metadata == null) return DEFAULT_DELETED_FILE_RETENTION_MILLIS;
        String value = metadata.configuration().get(DELETED_FILE_RETENTION_MILLIS);
        if (value == null) return DEFAULT_DELETED_FILE_RETENTION_MILLIS;
        try {
            long millis = Long.parseLong(value);
            if (millis < 0) throw new NumberFormatException("negative retention");
            return millis;
        } catch (NumberFormatException e) {
            throw new IOException("Invalid " + DELETED_FILE_RETENTION_MILLIS + ": " + value, e);
        }
    }

    private void publishLastCheckpointIfNewer(long version) throws IOException {
        synchronized (LAST_CHECKPOINT_LOCK) {
            long current = readLastCheckpointVersion();
            if (current >= version) return;
            storage.write(TransactionLog.LAST_CHECKPOINT, transactionLog.serialize(new LastCheckpoint(version)));
        }
    }

    private long readLastCheckpointVersion() throws IOException {
        if (!storage.exists(TransactionLog.LAST_CHECKPOINT)) return -1;
        try {
            LastCheckpoint checkpoint = transactionLog.deserialize(storage.read(TransactionLog.LAST_CHECKPOINT), LastCheckpoint.class);
            return checkpoint.version();
        } catch (Exception e) {
            return -1;
        }
    }

    private long parseCheckpointVersion(String file) throws IOException {
        String name = file.substring(file.lastIndexOf('/') + 1);
        String suffix = ".checkpoint.parquet";
        if (!name.endsWith(suffix)) throw new IOException("Malformed checkpoint filename: " + file);
        String versionString = name.substring(0, name.length() - suffix.length());
        try {
            return Long.parseLong(versionString);
        } catch (NumberFormatException e) {
            throw new IOException("Malformed checkpoint filename: " + file, e);
        }
    }

    public long latestCheckpointAtOrBefore(long targetVersion) throws IOException {
        if (targetVersion < 0) throw new IllegalArgumentException("targetVersion must be non-negative");
        long hinted = readLastCheckpointVersion();
        if (hinted >= 0 && hinted <= targetVersion) {
            long latest = hinted;
            for (String file : storage.listAfter(TransactionLog.LOG_DIRECTORY, checkpointPath(hinted))) {
                if (!file.endsWith(".checkpoint.parquet")) continue;
                long version = parseCheckpointVersion(file);
                if (version > targetVersion) break;
                latest = Math.max(latest, version);
            }
            return latest;
        }
        long latest = -1;
        for (String file : storage.list(TransactionLog.LOG_DIRECTORY)) {
            if (!file.endsWith(".checkpoint.parquet")) continue;
            long version = parseCheckpointVersion(file);
            if (version <= targetVersion) latest = Math.max(latest, version);
        }
        return latest;
    }

    private String checkpointPath(long version) {
        return TransactionLog.LOG_DIRECTORY + "/" + String.format("%020d.checkpoint.parquet", version);
    }
}
