package com.delta.deltalake.log;

import com.delta.deltalake.storage.LocalStorage;
import com.delta.deltalake.storage.Storage;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TransactionLogTest {
    @Test
    void commitIsCreateOnlyAndZeroPadded() throws Exception {
        Path root = Files.createTempDirectory("delta-log");
        TransactionLog log = new TransactionLog(new LocalStorage(root));
        LogRecord record = new LogRecord("add", new AddFile("data/a.parquet", 10, 20, true));
        assertTrue(log.append(0, List.of(record)));
        assertFalse(log.append(0, List.of(record)));
        assertEquals(0, log.latestVersion());
        assertTrue(Files.exists(root.resolve("_delta_log/00000000000000000000.json")));
    }


    @Test
    void latestVersionUsesCheckpointAsStartingPoint() throws Exception {
        Path root = Files.createTempDirectory("transaction-log-latest");
        Storage storage = new LocalStorage(root);
        TransactionLog log = new TransactionLog(storage);
        storage.create("_delta_log/00000000000000000000.json", new byte[] {1});
        storage.create("_delta_log/00000000000000000001.json", new byte[] {1});
        storage.create("_delta_log/00000000000000000002.json", new byte[] {1});
        storage.create("_delta_log/_last_checkpoint", log.serialize(new LastCheckpoint(1)));
        storage.create("_delta_log/00000000000000000003.json", new byte[] {1});
        assertEquals(3, log.latestVersion());
    }

    @Test
    void tailUsesVersionRange() throws Exception {
        Path root = Files.createTempDirectory("transaction-log-tail");
        Storage storage = new LocalStorage(root);
        TransactionLog log = new TransactionLog(storage);
        List<LogRecord> records = List.of(new LogRecord("protocol", new Protocol(1, 1)));
        assertTrue(log.append(0, records));
        assertTrue(log.append(1, records));
        assertTrue(log.append(2, records));
        List<VersionedLogRecord> tail = log.tail(0, 2);
        assertEquals(2, tail.size());
        assertEquals(1, tail.get(0).version());
        assertEquals(2, tail.get(1).version());
    }


    @Test
    void parallelTailMatchesOrderedTail() throws Exception {
        Path root = Files.createTempDirectory("transaction-log-parallel");
        Storage storage = new LocalStorage(root);
        TransactionLog log = new TransactionLog(storage);
        List<LogRecord> records = List.of(new LogRecord("protocol", new Protocol(1, 1)));
        for (int i = 0; i < 6; i++) assertTrue(log.append(i, records));
        assertEquals(log.tail(1, 5), log.tailParallel(1, 5));
    }

    @Test
    void tailRejectsMissingVersion() throws Exception {
        Path root = Files.createTempDirectory("transaction-log-gap");
        Storage storage = new LocalStorage(root);
        TransactionLog log = new TransactionLog(storage);
        List<LogRecord> records = List.of(new LogRecord("protocol", new Protocol(1, 1)));
        assertTrue(log.append(0, records));
        assertTrue(log.append(2, records));
        assertThrows(IOException.class, () -> log.tail(0, 2));
    }
    @Test
    void tailRetriesEventuallyConsistentMissingVersion() throws Exception {
        Storage storage = new FlakyStorage();
        TransactionLog log = new TransactionLog(storage);
        assertTrue(log.tail(-1, 0).isEmpty() == false);
    }

    private static final class FlakyStorage implements Storage {
        private final java.util.Map<String, byte[]> data = new java.util.TreeMap<>();
        private int listCalls;

        FlakyStorage() throws Exception {
            data.put(TransactionLog.logPath(0), new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsBytes(List.of(new LogRecord("protocol", new Protocol(1, 1)))));
        }

        @Override public byte[] read(String key) { return data.get(key); }
        @Override public void write(String key, byte[] bytes) { data.put(key, bytes); }
        @Override public void write(String key, Path source) throws IOException { data.put(key, Files.readAllBytes(source)); }
        @Override public boolean create(String key, byte[] bytes) { return data.putIfAbsent(key, bytes) == null; }
        @Override public boolean exists(String key) { return data.containsKey(key); }
        @Override public List<String> list(String prefix) { return data.keySet().stream().filter(k -> k.startsWith(prefix)).toList(); }
        @Override public List<String> listAfter(String prefix, String startAfter) {
            listCalls++;
            if (listCalls == 1) return List.of();
            return data.keySet().stream().filter(k -> k.startsWith(prefix) && k.compareTo(startAfter) > 0).toList();
        }
        @Override public void delete(String key) { data.remove(key); }
        @Override public boolean supportsEventualConsistency() { return true; }
    }

}
