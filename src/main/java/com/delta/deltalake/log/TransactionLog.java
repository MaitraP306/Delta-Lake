package com.delta.deltalake.log;

import com.delta.deltalake.storage.Storage;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class TransactionLog {
    public static final String LOG_DIRECTORY = "_delta_log";
    public static final String LAST_CHECKPOINT = LOG_DIRECTORY + "/_last_checkpoint";

    private final Storage storage;
    private final ObjectMapper mapper;
    public TransactionLog(Storage storage) {
        this.storage = storage;
        this.mapper = new ObjectMapper();
    }

    public long latestVersion() throws IOException {
        long checkpointVersion = readCheckpointVersion();
        long latest = checkpointVersion;
        String startAfter = checkpointVersion >= 0 ? logPath(checkpointVersion) : LOG_DIRECTORY + "/";
        for (String file : storage.listAfter(LOG_DIRECTORY, startAfter)) {
            if (!file.endsWith(".json")) {
                continue;
            }
            long version = parseLogVersion(file);
            latest = Math.max(latest, version);
        }
        return latest;
    }

    private long readCheckpointVersion() throws IOException {
        if (!storage.exists(LAST_CHECKPOINT)) {
            return -1;
        }

        try {
            LastCheckpoint checkpoint = deserialize(storage.read(LAST_CHECKPOINT), LastCheckpoint.class);
            return checkpoint.version();

        } catch (Exception e) {
            return -1;
        }
    }

    public boolean append(long version, List<LogRecord> records) throws IOException {
        if (version < 0 || records == null || records.isEmpty()) {
            throw new IllegalArgumentException("version must be >= 0 and records must be non-empty");
        }
        return storage.create(logPath(version), mapper.writeValueAsBytes(records));
    }
    public List<LogRecord> read(long version) throws IOException {
        byte[] data = storage.read(logPath(version));
        return mapper.readValue(data, new TypeReference<List<LogRecord>>() {});
    }

    private long parseLogVersion(String file) throws IOException {
        String name = file.substring(file.lastIndexOf('/') + 1);
        if (!name.endsWith(".json")) {
            throw new IOException("Not a transaction log file: " + file);
        }
        String versionString = name.substring(0, name.length() - ".json".length());

        try {
            return Long.parseLong(versionString);
        } catch (NumberFormatException e) {
            throw new IOException("Malformed transaction log filename: " + file, e);
        }
    }
    public List<VersionedLogRecord> tail(long startingVersionExclusive, long targetVersionInclusive) throws IOException {
        if (startingVersionExclusive < -1) {
            throw new IllegalArgumentException("startingVersionExclusive must be >= -1");
        }
        if (targetVersionInclusive < startingVersionExclusive) {
            return List.of();
        }
        String startAfter = startingVersionExclusive >= 0 ? logPath(startingVersionExclusive) : LOG_DIRECTORY + "/";
        List<VersionedLogRecord> result = new ArrayList<>();
        long expectedVersion = startingVersionExclusive + 1;
        for (String file : storage.listAfter(LOG_DIRECTORY, startAfter)) {
            if (!file.endsWith(".json")) {
                continue;
            }
            long version = parseLogVersion(file);
            if (version > targetVersionInclusive) {
                break;
            }
            if (version != expectedVersion) {
                throw new IOException("Non-contiguous transaction log: " + "expected version " + expectedVersion + " but found " + version);
            }
            for (LogRecord record : read(version)) {
                result.add(new VersionedLogRecord(version, record));
            }
            expectedVersion++;
        }
        if (expectedVersion <= targetVersionInclusive) {
            throw new IOException("Transaction log ended before target version " + targetVersionInclusive);
        }
        return result;
    }

    public byte[] serialize(Object value) throws IOException {
        return mapper.writeValueAsBytes(value);
    }

    public <T> T deserialize(byte[] bytes, Class<T> type) throws IOException {
        return mapper.readValue(bytes, type);
    }

    public ObjectMapper mapper() {
        return mapper;
    }

    public static String logPath(long version) {
        return LOG_DIRECTORY + "/" + String.format("%020d.json", version);
    }
}
