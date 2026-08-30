package com.delta.deltalake.table;

import com.delta.deltalake.log.TransactionLog;
import com.delta.deltalake.storage.Storage;

import com.delta.deltalake.data.Record;
import com.delta.deltalake.data.ParquetWriter;
import com.delta.deltalake.log.AddFile;
import com.delta.deltalake.log.LogRecord;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

public final class DeltaTable {

    private final Storage storage;
    private final TransactionLog transactionLog;
    private final SnapshotManager snapshotManager;

    private DeltaTable(Storage storage) {
        this.storage = storage;
        this.transactionLog = new TransactionLog(storage);
        this.snapshotManager = new SnapshotManager(transactionLog);
    }

    public static DeltaTable open(Storage storage) {
        return new DeltaTable(storage);
    }

    public Snapshot snapshot() throws Exception {
        long version = transactionLog.latestVersion();

        if (version < 0) {
            throw new IllegalStateException("Table does not exist");
        }

        return snapshotManager.loadSnapshot(version);
    }

    public Snapshot snapshot(long version) throws Exception {
        return snapshotManager.loadSnapshot(version);
    }

    public void append(List<Record> records) throws IOException {
        if (records == null || records.isEmpty()) {
            throw new IllegalArgumentException("Records cannot be empty");
        }

        long nextVersion = transactionLog.latestVersion() + 1;

        String fileName = UUID.randomUUID() + ".parquet";
        String dataPath = "data/" + fileName;

        Path tempFile = Files.createTempFile(
                "delta-data-",
                ".parquet"
        );

        Files.deleteIfExists(tempFile);
        
        try {
            ParquetWriter.write(tempFile, records);

            storage.write(dataPath, tempFile);

            long size = Files.size(tempFile);
            long modificationTime =
                    Files.getLastModifiedTime(tempFile).toMillis();

            AddFile addFile = new AddFile(
                    dataPath,
                    size,
                    modificationTime,
                    true
            );

            LogRecord record = new LogRecord(
                    "add",
                    addFile
            );

            boolean committed = transactionLog.append(
                    nextVersion,
                    List.of(record)
            );

            if (!committed) {
                throw new IOException(
                        "Transaction commit failed for version " +
                        nextVersion
                );
            }

        } finally {
            Files.deleteIfExists(tempFile);
        }
    }
    }