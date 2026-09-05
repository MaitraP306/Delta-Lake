package com.delta.deltalake.log;

import com.delta.deltalake.storage.LocalStorage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class LogActionVerificationTest {

    @TempDir
    Path tempDir;

    private void verifyActionRoundTrip(String type, LogAction action, Map<String, Object> expectedFields) throws Exception {
        LocalStorage storage = new LocalStorage(tempDir.resolve(type));
        TransactionLog log = new TransactionLog(storage);
        assertTrue(log.append(0, List.of(new LogRecord(type, action))));
        List<LogRecord> records = log.read(0);
        assertEquals(1, records.size());
        LogRecord record = records.get(0);
        assertEquals(type, record.type());
        assertNotNull(record.action());
        assertInstanceOf(Map.class, record.action());
        @SuppressWarnings("unchecked")
        Map<String, Object> decoded = (Map<String, Object>) record.action();
        for (Map.Entry<String, Object> expected : expectedFields.entrySet()) {
            Object expectedValue = expected.getValue();
            Object actualValue = decoded.get(expected.getKey());
            if (expectedValue instanceof Number expectedNumber && actualValue instanceof Number actualNumber) {
                assertEquals(expectedNumber.longValue(), actualNumber.longValue(), "Incorrect numeric field: " + expected.getKey());
            } else {
                assertEquals(expectedValue, actualValue, "Incorrect field: " + expected.getKey());
            }
        }
    }

    @Test
    void addFileRoundTrip() throws Exception {

        AddFile action = new AddFile("data/part-00001.parquet", 12345L, 1700000000000L, true, new FileStats(100L, Map.of("id", new FileStats.ColumnStats(1L, 100L, 0L))));
        verifyActionRoundTrip("add", action, Map.of("path", "data/part-00001.parquet", "size", 12345L, "modificationTime", 1700000000000L, "dataChange", true));
    }

    @Test
    void removeFileRoundTrip() throws Exception {
        RemoveFile action = new RemoveFile("data/part-00001.parquet", 1700000000000L, true);
        verifyActionRoundTrip("remove", action, Map.of("path", "data/part-00001.parquet", "deletionTimestamp", 1700000000000L, "dataChange", true));
    }

    @Test
    void metadataRoundTrip() throws Exception {
        Metadata action = new Metadata("table-123", "parquet", "{\"type\":\"struct\",\"fields\":[]}", List.of("date"), Map.of("delta.appendOnly", "false"));
        verifyActionRoundTrip("metaData", action, Map.of("id", "table-123", "format", "parquet", "schemaString", "{\"type\":\"struct\",\"fields\":[]}"));
    }

    @Test
    void protocolRoundTrip() throws Exception {
        Protocol action = new Protocol(1, 2);
        verifyActionRoundTrip("protocol", action, Map.of("minReaderVersion", 1, "minWriterVersion", 2));
    }

    @Test
    void commitInfoRoundTrip() throws Exception {
        CommitInfo action = new CommitInfo(1700000000000L, "WRITE", Map.of("mode", "append"), "verification-test");
        verifyActionRoundTrip("commitInfo", action, Map.of("timestamp", 1700000000000L, "operation", "WRITE", "userMetadata", "verification-test"));
    }

    @Test
    void txnRoundTrip() throws Exception {
        Txn action = new Txn("verification-app", 42L, 1700000000000L);
        verifyActionRoundTrip("txn", action, Map.of("appId", "verification-app", "version", 42L, "lastUpdated", 1700000000000L));
    }

    @Test
    void multipleActionsCanExistInOneCommit() throws Exception {
        LocalStorage storage = new LocalStorage(tempDir.resolve("multiple"));
        TransactionLog log = new TransactionLog(storage);
        AddFile addFile = new AddFile("data/file.parquet", 1000L, 1700000000000L, true);
        Metadata metadata = new Metadata("table-1", "parquet", "{\"schema\":\"test\"}", List.of(), Map.of());
        Protocol protocol = new Protocol(1, 1);
        CommitInfo commitInfo = new CommitInfo(1700000000000L, "WRITE", Map.of(), null);
        List<LogRecord> records = List.of(new LogRecord("add", addFile), new LogRecord("metaData", metadata), new LogRecord("protocol", protocol), new LogRecord("commitInfo", commitInfo));
        assertTrue(log.append(0, records));
        List<LogRecord> decoded = log.read(0);
        assertEquals(4, decoded.size());
        assertEquals("add", decoded.get(0).type());
        assertEquals("metaData", decoded.get(1).type());
        assertEquals("protocol", decoded.get(2).type());
        assertEquals("commitInfo", decoded.get(3).type());
        assertTrue(decoded.get(0).action() instanceof Map);
        assertTrue(decoded.get(1).action() instanceof Map);
        assertTrue(decoded.get(2).action() instanceof Map);
        assertTrue(decoded.get(3).action() instanceof Map);
    }
}