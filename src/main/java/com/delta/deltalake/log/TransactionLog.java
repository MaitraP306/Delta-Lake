package com.delta.deltalake.log;

import com.delta.deltalake.storage.Storage;
import com.delta.deltalake.table.Snapshot;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.LinkedHashMap;
import java.util.Map;


import java.io.IOException;
import java.util.List;

public class TransactionLog {

    private static final String LOG_DIRECTORY = "_delta_log";

    private final Storage storage;
    private final ObjectMapper mapper;

    public TransactionLog(Storage storage) {
        this.storage = storage;
        this.mapper = new ObjectMapper();
    }

    public long latestVersion() throws IOException {
        List<String> files = storage.list(LOG_DIRECTORY);

        long latest = -1;

        for (String file : files) {
            if (!file.endsWith(".json")) {
                continue;
            }

            String name = file.substring(
                    file.lastIndexOf('/') + 1,
                    file.length() - 5
            );

            try {
                latest = Math.max(latest, Long.parseLong(name));
            } catch (NumberFormatException ignored) {
            }
        }

        return latest;
    }

    public boolean append(long version, List<LogRecord> records)
            throws IOException {

        String path = logPath(version);

        byte[] data = mapper.writeValueAsBytes(records);

        return storage.create(path, data);
    }

    public List<LogRecord> read(long version) throws IOException {
        String path = logPath(version);

        if (!storage.exists(path)) {
            throw new IOException("Transaction log version does not exist: " + version);
        }

        byte[] data = storage.read(path);

        return mapper.readValue(
                data,
                new TypeReference<List<LogRecord>>() {
                }
        );
    }

    private String logPath(long version) {
        return LOG_DIRECTORY + "/" +
                String.format("%020d.json", version);
    }


    public LogAction parseAction(LogRecord record) {
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


    public Snapshot loadSnapshot(long targetVersion) throws IOException {
        if (targetVersion < 0) {
            throw new IllegalArgumentException("Version must be non-negative");
        }

        long latest = latestVersion();

        if (targetVersion > latest) {
            throw new IllegalArgumentException(
                    "Requested version " + targetVersion +
                    " but latest version is " + latest
            );
        }

        Map<String, AddFile> activeFiles = new java.util.LinkedHashMap<>();

        for (long version = 0; version <= targetVersion; version++) {
            List<LogRecord> records = read(version);

            for (LogRecord record : records) {
                LogAction action = parseAction(record);

                if (action instanceof AddFile addFile) {
                    activeFiles.put(addFile.path(), addFile);
                } else if (action instanceof RemoveFile removeFile) {
                    activeFiles.remove(removeFile.path());
                } else {
                    throw new IllegalArgumentException(
                            "Unsupported action type: " + record.type()
                    );
                }
            }
        }

        return new Snapshot(targetVersion, activeFiles);
    }


}