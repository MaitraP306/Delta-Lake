package com.delta.deltalake.table;

import com.delta.deltalake.log.AddFile;
import com.delta.deltalake.log.LogAction;
import com.delta.deltalake.log.LogRecord;
import com.delta.deltalake.log.Metadata;
import com.delta.deltalake.log.Protocol;
import com.delta.deltalake.log.TransactionLog;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

public final class OptimisticTransaction {
    private final TransactionLog transactionLog;
    private final SnapshotManager snapshotManager;
    private final Snapshot base;
    private final Map<String, AddFile> readSet = new HashMap<>();
    private Predicate<AddFile> newFileConflictPredicate;
    private boolean conflictOnMetadataChanges;

    OptimisticTransaction(TransactionLog transactionLog, SnapshotManager snapshotManager, Snapshot base) {
        this.transactionLog = Objects.requireNonNull(transactionLog);
        this.snapshotManager = Objects.requireNonNull(snapshotManager);
        this.base = Objects.requireNonNull(base);
    }

    public long baseVersion() {
        return base.version();
    }

    public Snapshot baseSnapshot() {
        return base;
    }

    public void readPath(String path) {
        Objects.requireNonNull(path);
        for (AddFile file : base.activeFiles()) {
            if (file.path().equals(path)) {
                readSet.put(path, file);
                return;
            }
        }
        readSet.put(path, null);
    }

    public void readPaths(Set<String> paths) {
        Objects.requireNonNull(paths);
        for (String path : paths) readPath(path);
    }

    public void failIfNewFileMatches(Predicate<AddFile> predicate) {
        newFileConflictPredicate = Objects.requireNonNull(predicate);
    }

    public void failOnMetadataChanges() {
        conflictOnMetadataChanges = true;
    }

    public boolean commit(java.util.List<LogRecord> actions) throws IOException {
        Objects.requireNonNull(actions);
        if (actions.isEmpty()) throw new IllegalArgumentException("actions cannot be empty");

        long currentVersion = transactionLog.latestVersion();
        if (currentVersion < base.version()) return false;

        Snapshot current = currentVersion == base.version() ? base : snapshotManager.loadSnapshot(currentVersion);

        for (Map.Entry<String, AddFile> entry : readSet.entrySet()) {
            AddFile now = current.activeFiles().stream().filter(file -> file.path().equals(entry.getKey())).findFirst().orElse(null);
            if (!Objects.equals(entry.getValue(), now)) return false;
        }

        if (newFileConflictPredicate != null) {
            for (AddFile file : current.activeFiles()) {
                if (!base.contains(file.path()) && newFileConflictPredicate.test(file)) return false;
            }
        }

        if (conflictOnMetadataChanges && currentVersion > base.version()) {
            for (var versioned : transactionLog.tail(base.version(), currentVersion)) {
                LogRecord record = versioned.record();
                LogAction action = ActionCodec.decode(record.type(), record.action(), transactionLog.mapper());
                if (action instanceof Metadata || action instanceof Protocol) return false;
            }
        }

        return transactionLog.append(currentVersion + 1, actions);
    }
}
