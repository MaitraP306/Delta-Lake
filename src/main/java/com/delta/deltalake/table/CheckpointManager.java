package com.delta.deltalake.table;

import com.delta.deltalake.data.CheckpointCodec;
import com.delta.deltalake.data.CheckpointParquetReader;
import com.delta.deltalake.data.CheckpointParquetWriter;
import com.delta.deltalake.data.CheckpointSchema;
import com.delta.deltalake.log.LastCheckpoint;
import com.delta.deltalake.log.LogAction;
import com.delta.deltalake.log.TransactionLog;
import com.delta.deltalake.storage.Storage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.apache.avro.generic.GenericRecord;

public final class CheckpointManager {
    private final Storage storage;
    private final TransactionLog transactionLog;

    public CheckpointManager(Storage storage, TransactionLog transactionLog) {
        this.storage = storage;
        this.transactionLog = transactionLog;
    }

    public void create(long version) throws IOException {
        Snapshot snapshot = new SnapshotManager(transactionLog, storage).loadSnapshot(version, false);
        List<LogAction> actions = snapshot.checkpointActions();
        List<GenericRecord> rows = actions.stream().map(CheckpointCodec::encode).toList();
        Path temp = Files.createTempFile("delta-checkpoint-", ".parquet");

        try {
            CheckpointParquetWriter.write(temp, CheckpointSchema.schema(), rows);
            storage.create(checkpointPath(version), Files.readAllBytes(temp));
            storage.write(TransactionLog.LAST_CHECKPOINT, transactionLog.serialize(new LastCheckpoint(version)));
        } finally {
            Files.deleteIfExists(temp);
        }
    }

    public boolean exists(long version) throws IOException {
        return storage.exists(checkpointPath(version));
    }

    public List<LogAction> load(long version)throws IOException {

        Path temp = Files.createTempFile("delta-checkpoint-read-", ".parquet");

        try {
            Files.write(temp, storage.read(checkpointPath(version)));
            return CheckpointParquetReader.read(temp).stream().map(CheckpointCodec::decode).toList();
        } finally {
            Files.deleteIfExists(temp);
        }
    }

    private long readLastCheckpointVersion() throws IOException {
        if (!storage.exists(TransactionLog.LAST_CHECKPOINT)) {
            return -1;
        }
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
        String versionString = name.substring(0, name.length() - suffix.length());
        try {
            return Long.parseLong(versionString);
        } catch (NumberFormatException e) {
            throw new IOException("Malformed checkpoint filename: " + file, e);
        }
    }


    public long latestCheckpointAtOrBefore(long targetVersion) throws IOException {
        if (targetVersion < 0) {
            throw new IllegalArgumentException("targetVersion must be non-negative");
        }
        long hinted = readLastCheckpointVersion();
        if (hinted >= 0 && hinted <= targetVersion) {
            long latest = hinted;
            for (String file : storage.listAfter(TransactionLog.LOG_DIRECTORY, checkpointPath(hinted))) {
                if (!file.endsWith(".checkpoint.parquet")) {
                    continue;
                }
                long version = parseCheckpointVersion(file);
                if (version > targetVersion) {
                    break;
                }
                latest = Math.max(latest, version);
            }
            return latest;
        }
        long latest = -1;
        for (String file : storage.list(TransactionLog.LOG_DIRECTORY)) {
            if (!file.endsWith(".checkpoint.parquet")) {
                continue;
            }
            long version = parseCheckpointVersion(file);
            if (version <= targetVersion) {
                latest = Math.max(latest, version);
            }
        }
        return latest;
    }

    private String checkpointPath(long version) {
        return TransactionLog.LOG_DIRECTORY + "/" + String.format("%020d.checkpoint.parquet", version);
    }
}
