package com.delta.deltalake.log;

import com.delta.deltalake.storage.LocalStorage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class TransactionLogVerificationTest {

    @TempDir
    Path tempDir;

    private CommitInfo commitInfo(String operation) {
        return new CommitInfo(System.currentTimeMillis(), operation, Map.of(), null);
    }

    private LogRecord commitRecord(String operation) {
        return new LogRecord("commitInfo", commitInfo(operation));
    }

    @Test
    void newLogHasNoVersions() throws Exception {
        LocalStorage storage = new LocalStorage(tempDir);
        TransactionLog log = new TransactionLog(storage);
        assertEquals(-1, log.latestVersion());
    }

    @Test
    void appendCreatesCorrectVersionedLogFile() throws Exception {
        LocalStorage storage = new LocalStorage(tempDir);
        TransactionLog log = new TransactionLog(storage);
        List<LogRecord> records = List.of(commitRecord("WRITE"));
        assertTrue(log.append(0, records));
        assertTrue(storage.exists(TransactionLog.logPath(0)));
        assertEquals("_delta_log/00000000000000000000.json", TransactionLog.logPath(0));
        assertEquals(0, log.latestVersion());
    }

    @Test
    void appendAndReadRoundTrip() throws Exception {
        LocalStorage storage = new LocalStorage(tempDir);
        TransactionLog log = new TransactionLog(storage);
        List<LogRecord> original = List.of(commitRecord("WRITE"));
        assertTrue(log.append(0, original));
        List<LogRecord> decoded = log.read(0);
        assertEquals(1, decoded.size());
        LogRecord record = decoded.get(0);
        assertEquals("commitInfo", record.type());
        assertNotNull(record.action());
        assertTrue(record.action() instanceof Map, "Deserialized action should currently be represented as a Map");
        @SuppressWarnings("unchecked")
        Map<String, Object> action = (Map<String, Object>) record.action();
        assertEquals("WRITE", action.get("operation"));
        assertNotNull(action.get("timestamp"));
    }

    @Test
    void versionsIncreaseSequentially() throws Exception {
        LocalStorage storage = new LocalStorage(tempDir);
        TransactionLog log = new TransactionLog(storage);
        for (long version = 0; version < 5; version++) {
            assertTrue(log.append(version, List.of(commitRecord("WRITE"))));
            assertEquals(version, log.latestVersion());
        }
        for (long version = 0; version < 5; version++) {
            assertTrue(storage.exists(TransactionLog.logPath(version)));
        }
    }

    @Test
    void duplicateVersionDoesNotOverwriteExistingCommit() throws Exception {
        LocalStorage storage = new LocalStorage(tempDir);
        TransactionLog log = new TransactionLog(storage);
        assertTrue(log.append(0, List.of(commitRecord("FIRST"))));
        assertFalse(log.append(0, List.of(commitRecord("SECOND"))));
        List<LogRecord> records = log.read(0);
        assertEquals(1, records.size());
        @SuppressWarnings("unchecked")
        Map<String, Object> action = (Map<String, Object>) records.get(0).action();
        assertEquals("FIRST", action.get("operation"));
    }

    @Test
    void invalidAppendArgumentsAreRejected() throws Exception {
        LocalStorage storage = new LocalStorage(tempDir);
        TransactionLog log = new TransactionLog(storage);
        assertThrows(IllegalArgumentException.class, () -> log.append(-1, List.of(commitRecord("WRITE"))));
        assertThrows(IllegalArgumentException.class, () -> log.append(0, null));
        assertThrows(IllegalArgumentException.class, () -> log.append(0, List.of()));
    }

    @Test
    void logPathUsesTwentyDigitZeroPaddedVersion() {
        assertEquals("_delta_log/00000000000000000000.json", TransactionLog.logPath(0));
        assertEquals("_delta_log/00000000000000000001.json", TransactionLog.logPath(1));
        assertEquals("_delta_log/00000000000000000123.json", TransactionLog.logPath(123));
        assertEquals("_delta_log/00000000000000001000.json", TransactionLog.logPath(1000));
    }

    @Test
    void readMissingVersionFails() throws Exception {
        LocalStorage storage = new LocalStorage(tempDir);
        TransactionLog log = new TransactionLog(storage);
        assertThrows(IOException.class, () -> log.read(0));
    }
}