package com.delta.deltalake.log;

import com.delta.deltalake.storage.Storage;
import com.delta.deltalake.cache.DeltaCache;
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
    private final DeltaCache<Long, List<LogRecord>> readCache = new DeltaCache<>(128);
    public TransactionLog(Storage storage) {
        this.storage = storage;
        this.mapper = new ObjectMapper();
    }

    public long latestVersion() throws IOException {
        long checkpointVersion = readCheckpointVersion();
        long latest = checkpointVersion;
        String startAfter = checkpointVersion >= 0 ? logPath(checkpointVersion) : LOG_DIRECTORY + "/";
        int attempts = storage.supportsEventualConsistency() ? 3 : 1;
        for (int attempt = 1; attempt <= attempts; attempt++) {
            for (String file : storage.listAfter(LOG_DIRECTORY, startAfter)) {
                if (!file.endsWith(".json")) continue;
                long version = parseLogVersion(file);
                latest = Math.max(latest, version);
            }
            if (!storage.supportsEventualConsistency() || attempt == attempts) break;
            try {
                Thread.sleep(50L * attempt);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while refreshing latest transaction-log version", e);
            }
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
        List<LogRecord> cached = readCache.get(version);
        if (cached != null) return cached;
        byte[] data = storage.read(logPath(version));
        List<LogRecord> decoded = List.copyOf(mapper.readValue(data, new TypeReference<List<LogRecord>>() {}));
        readCache.put(version, decoded);
        return decoded;
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
                if (storage.supportsEventualConsistency()) {
                    List<String> refreshed = eventuallyListAfter(LOG_DIRECTORY, startAfter, expectedVersion);
                    if (!refreshed.isEmpty()) {
                        file = refreshed.get(0);
                        version = parseLogVersion(file);
                    }
                }
                if (version != expectedVersion) {
                    throw new IOException("Non-contiguous transaction log: expected version " + expectedVersion + " but found " + version);
                }
            }
            for (LogRecord record : read(version)) {
                result.add(new VersionedLogRecord(version, record));
            }
            expectedVersion++;
        }
        if (expectedVersion <= targetVersionInclusive && storage.supportsEventualConsistency()) {
            List<String> refreshed = eventuallyListAfter(LOG_DIRECTORY, startAfter, expectedVersion);
            for (String file : refreshed) {
                if (!file.endsWith(".json")) continue;
                long version = parseLogVersion(file);
                if (version < expectedVersion) continue;
                if (version > targetVersionInclusive) break;
                if (version != expectedVersion) continue;
                for (LogRecord record : read(version)) {
                    result.add(new VersionedLogRecord(version, record));
                }
                expectedVersion++;
            }
        }
        if (expectedVersion <= targetVersionInclusive) {
            throw new IOException("Transaction log ended before target version " + targetVersionInclusive);
        }
        return result;
    }

    public List<VersionedLogRecord> tailParallel(long startingVersionExclusive, long targetVersionInclusive) throws IOException {
        if (startingVersionExclusive < -1) throw new IllegalArgumentException("startingVersionExclusive must be >= -1");
        if (targetVersionInclusive < startingVersionExclusive) return List.of();
        String startAfter = startingVersionExclusive >= 0 ? logPath(startingVersionExclusive) : LOG_DIRECTORY + "/";
        List<Long> versions = new ArrayList<>();
        long expected = startingVersionExclusive + 1;
        for (String file : storage.listAfter(LOG_DIRECTORY, startAfter)) {
            if (!file.endsWith(".json")) continue;
            long version = parseLogVersion(file);
            if (version > targetVersionInclusive) break;
            if (version != expected) throw new IOException("Non-contiguous transaction log: expected version " + expected + " but found " + version);
            versions.add(version);
            expected++;
        }
        if (expected <= targetVersionInclusive) {
            if (storage.supportsEventualConsistency()) {
                List<VersionedLogRecord> retry = tail(startingVersionExclusive, targetVersionInclusive);
                if (!retry.isEmpty() || targetVersionInclusive == startingVersionExclusive) return retry;
            }
            throw new IOException("Transaction log ended before target version " + targetVersionInclusive);
        }

        try (var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            List<java.util.concurrent.Future<List<LogRecord>>> futures = new ArrayList<>();
            for (long version : versions) futures.add(executor.submit(() -> read(version)));
            List<VersionedLogRecord> result = new ArrayList<>();
            for (int i = 0; i < versions.size(); i++) {
                for (LogRecord record : futures.get(i).get()) {
                    result.add(new VersionedLogRecord(versions.get(i), record));
                }
            }
            return result;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while reconstructing transaction log", e);
        } catch (java.util.concurrent.ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException io) throw io;
            throw new IOException("Failed to reconstruct transaction log", cause);
        }
    }

    private List<String> eventuallyListAfter(String prefix, String startAfter, long expectedVersion) throws IOException {
        IOException last = null;
        for (int attempt = 1; attempt <= 4; attempt++) {
            List<String> files = storage.listAfter(prefix, startAfter);
            for (String file : files) {
                if (file.endsWith(".json") && parseLogVersion(file) == expectedVersion) return files.stream().filter(f -> f.endsWith(".json")).sorted().toList();
            }
            try { Thread.sleep(50L * (1L << (attempt - 1))); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IOException("Interrupted while waiting for eventually consistent log", e); }
            last = new IOException("Expected transaction-log version " + expectedVersion + " is not visible yet");
        }
        if (last != null) return List.of();
        return List.of();
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
