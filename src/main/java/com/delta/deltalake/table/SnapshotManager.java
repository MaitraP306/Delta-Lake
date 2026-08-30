package com.delta.deltalake.table;

import com.delta.deltalake.log.AddFile;
import com.delta.deltalake.log.LogAction;
import com.delta.deltalake.log.LogRecord;
import com.delta.deltalake.log.RemoveFile;
import com.delta.deltalake.log.TransactionLog;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.List;

public class SnapshotManager {

    private final TransactionLog transactionLog;
    private final ObjectMapper mapper;

    public SnapshotManager(TransactionLog transactionLog) {
        this.transactionLog = transactionLog;
        this.mapper = new ObjectMapper();
    }

    public Snapshot loadSnapshot(long targetVersion) throws IOException {
        if (targetVersion < 0) {
            throw new IllegalArgumentException(
                    "Version must be non-negative"
            );
        }

        long latest = transactionLog.latestVersion();

        if (targetVersion > latest) {
            throw new IllegalArgumentException(
                    "Requested version " + targetVersion +
                    " but latest version is " + latest
            );
        }

        SnapshotBuilder builder = new SnapshotBuilder();

        for (long version = 0; version <= targetVersion; version++) {
            List<LogRecord> records = transactionLog.read(version);

            for (LogRecord record : records) {
                LogAction action = parseAction(record);
                builder.apply(action);
            }
        }

        return builder.build(targetVersion);
    }

    private LogAction parseAction(LogRecord record) {
        return switch (record.type()) {
            case "add" -> mapper.convertValue(
                    record.action(),
                    AddFile.class
            );

            case "remove" -> mapper.convertValue(
                    record.action(),
                    RemoveFile.class
            );

            default -> throw new IllegalArgumentException(
                    "Unsupported log action: " + record.type()
            );
        };
    }
}