package com.delta.deltalake.table;

import com.delta.deltalake.log.LogAction;
import com.delta.deltalake.log.TransactionLog;
import com.delta.deltalake.log.VersionedLogRecord;
import com.delta.deltalake.storage.Storage;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public final class SnapshotManager {
    private final TransactionLog transactionLog;
    private final CheckpointManager checkpointManager;
    private final ObjectMapper mapper;

    public SnapshotManager(TransactionLog transactionLog) {
        this(transactionLog, null);
    }

    public SnapshotManager(TransactionLog transactionLog, Storage storage) {
        this.transactionLog = transactionLog;
        this.checkpointManager = storage == null ? null : new CheckpointManager(storage, transactionLog);
        this.mapper = transactionLog.mapper();
    }

    public Snapshot loadSnapshot(long targetVersion) throws IOException {
        return loadSnapshot(targetVersion, true);
    }

    Snapshot loadSnapshot(long targetVersion, boolean useCheckpoint) throws IOException {
        if (targetVersion < 0) throw new IllegalArgumentException("Version must be non-negative");
        long latest = transactionLog.latestVersion();
        if (targetVersion > latest) {
            throw new IllegalArgumentException("Requested version " + targetVersion + " but latest version is " + latest);
        }
        SnapshotBuilder builder = new SnapshotBuilder(mapper);
        long startVersion = -1;
        if (useCheckpoint && checkpointManager != null) {
            long checkpoint = checkpointManager.latestCheckpointAtOrBefore(targetVersion);
            if (checkpoint >= 0) {
                for (LogAction action : checkpointManager.load(checkpoint)) {
                    builder.apply(action);
                }
                startVersion = checkpoint;
            }
        }
        for (VersionedLogRecord versionedRecord : transactionLog.tail(startVersion, targetVersion)) {
            builder.apply(versionedRecord.record());
        }
        return builder.build(targetVersion);
    }
}
