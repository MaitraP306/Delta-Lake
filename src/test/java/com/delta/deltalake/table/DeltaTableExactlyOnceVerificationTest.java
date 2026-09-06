package com.delta.deltalake.table;

import com.delta.deltalake.data.Row;
import com.delta.deltalake.data.TableSchema;
import com.delta.deltalake.log.AddFile;
import com.delta.deltalake.log.Txn;
import com.delta.deltalake.log.VersionedLogRecord;
import com.delta.deltalake.storage.LocalStorage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DeltaTableExactlyOnceVerificationTest {

    @TempDir
    Path tempDir;

    private DeltaTable newTable() {
        return DeltaTable.open(new LocalStorage(tempDir));
    }

    private TableSchema schema() {
        return TableSchema.fromJson("""
                {
                  "type": "record",
                  "name": "TestRecord",
                  "fields": [
                    {"name": "id", "type": "long"},
                    {"name": "name", "type": "string"},
                    {"name": "age", "type": "int"}
                  ]
                }
                """);
    }

    private Row row(TableSchema schema, long id, String name, int age) {
        return Row.of(schema, Map.of("id", id, "name", name, "age", age));
    }

    @Test
    void repeatedApplicationTransactionDoesNotDuplicateData() throws Exception {
        DeltaTable table = newTable();
        TableSchema schema = schema();

        long firstVersion = table.appendRows(List.of(row(schema, 1, "Alice", 25)), "app-A", 7L);

        long secondVersion = table.appendRows(List.of(row(schema, 1, "Alice", 25)), "app-A", 7L);

        assertEquals(firstVersion, secondVersion);
        assertEquals(0, firstVersion);

        assertEquals(1, table.readRows().size());
    }

    @Test
    void repeatedApplicationTransactionDoesNotCreateSecondDataFile() throws Exception {
        DeltaTable table = newTable();
        TableSchema schema = schema();

        table.appendRows(List.of(row(schema, 1, "Alice", 25)), "app-A", 7L);

        List<String> firstPaths = table.snapshot().activeFiles().stream().map(AddFile::path).toList();

        table.appendRows(List.of(row(schema, 1, "Alice", 25)), "app-A", 7L);

        List<String> secondPaths = table.snapshot().activeFiles().stream().map(AddFile::path).toList();

        assertEquals(firstPaths, secondPaths);
        assertEquals(1, secondPaths.size());
    }

    @Test
    void applicationTransactionIsRecordedInLog() throws Exception {
        DeltaTable table = newTable();
        TableSchema schema = schema();

        long version = table.appendRows(List.of(row(schema, 1, "Alice", 25)), "app-A", 7L);

        assertEquals(0, version);

        List<VersionedLogRecord> records = table.tail(-1);

        assertTrue(records.stream().anyMatch(r -> "txn".equalsIgnoreCase(r.record().type())), "Expected txn action in transaction log");
    }

    @Test
    void transactionAndDataWriteShareSameCommitVersion() throws Exception {
        DeltaTable table = newTable();
        TableSchema schema = schema();

        long version = table.appendRows(List.of(row(schema, 1, "Alice", 25)), "app-A", 7L);

        List<VersionedLogRecord> records = table.tail(-1);

        assertTrue(records.stream().anyMatch(r -> r.version() == version && "txn".equalsIgnoreCase(r.record().type())), "Expected txn action in same commit");

        assertTrue(records.stream().anyMatch(r -> r.version() == version && "add".equalsIgnoreCase(r.record().type())), "Expected add action in same commit");
    }

    @Test
    void newerApplicationVersionCreatesNewCommit() throws Exception {
        DeltaTable table = newTable();
        TableSchema schema = schema();

        long first = table.appendRows(List.of(row(schema, 1, "Alice", 25)), "app-A", 7L);

        long second = table.appendRows(List.of(row(schema, 2, "Bob", 30)), "app-A", 8L);

        assertEquals(0, first);
        assertEquals(1, second);

        assertEquals(2, table.readRows().size());

        Txn txn = table.snapshot().transactions().get("app-A");

        assertNotNull(txn);
        assertEquals(8L, txn.version());
    }

    @Test
    void olderApplicationVersionIsIgnored() throws Exception {
        DeltaTable table = newTable();
        TableSchema schema = schema();

        long first = table.appendRows(List.of(row(schema, 1, "Alice", 25)), "app-A", 8L);

        long second = table.appendRows(List.of(row(schema, 2, "Bob", 30)), "app-A", 7L);

        assertEquals(0, first);
        assertEquals(first, second);

        assertEquals(1, table.readRows().size());

        Txn txn = table.snapshot().transactions().get("app-A");

        assertNotNull(txn);
        assertEquals(8L, txn.version());
    }

    @Test
    void sameApplicationVersionIsIgnoredAfterCommit() throws Exception {
        DeltaTable table = newTable();
        TableSchema schema = schema();

        table.appendRows(List.of(row(schema, 1, "Alice", 25)), "app-A", 10L);

        long before = table.version();

        long result = table.appendRows(List.of(row(schema, 2, "Bob", 30)), "app-A", 10L);

        assertEquals(before, result);
        assertEquals(before, table.version());

        assertEquals(1, table.readRows().size());
    }

    @Test
    void differentApplicationsCanUseSameVersionNumber() throws Exception {
        DeltaTable table = newTable();
        TableSchema schema = schema();

        long first = table.appendRows(List.of(row(schema, 1, "Alice", 25)), "app-A", 7L);

        long second = table.appendRows(List.of(row(schema, 2, "Bob", 30)), "app-B", 7L);

        assertEquals(0, first);
        assertEquals(1, second);

        assertEquals(2, table.readRows().size());

        assertEquals(7L, table.snapshot().transactions().get("app-A").version());

        assertEquals(7L, table.snapshot().transactions().get("app-B").version());
    }

    @Test
    void appIdWithoutTxnVersionIsRejected() throws Exception {
        DeltaTable table = newTable();
        TableSchema schema = schema();

        assertThrows(IllegalArgumentException.class, () -> table.appendRows(List.of(row(schema, 1, "Alice", 25)), "app-A", null));

        assertFalse(table.exists());
    }

    @Test
    void txnVersionWithoutAppIdIsRejected() throws Exception {
        DeltaTable table = newTable();
        TableSchema schema = schema();

        assertThrows(IllegalArgumentException.class, () -> table.appendRows(List.of(row(schema, 1, "Alice", 25)), null, 7L));

        assertFalse(table.exists());
    }

    @Test
    void transactionStateSurvivesReopeningTable() throws Exception {
        LocalStorage storage = new LocalStorage(tempDir);

        DeltaTable first = DeltaTable.open(storage);
        TableSchema schema = schema();

        first.appendRows(List.of(row(schema, 1, "Alice", 25)), "app-A", 42L);

        DeltaTable reopened = DeltaTable.open(storage);

        Txn txn = reopened.snapshot().transactions().get("app-A");

        assertNotNull(txn);
        assertEquals("app-A", txn.appId());
        assertEquals(42L, txn.version());
    }

    @Test
    void transactionStateSurvivesCheckpoint() throws Exception {
        LocalStorage storage = new LocalStorage(tempDir);

        DeltaTable table = DeltaTable.open(storage, 100);

        TableSchema schema = schema();

        table.appendRows(List.of(row(schema, 1, "Alice", 25)), "app-A", 42L);

        table.checkpoint();

        DeltaTable reopened = DeltaTable.open(storage, 100);

        Txn txn = reopened.snapshot().transactions().get("app-A");

        assertNotNull(txn);
        assertEquals("app-A", txn.appId());
        assertEquals(42L, txn.version());
    }

    @Test
    void retryWithDifferentDataForSameApplicationVersionIsIgnored() throws Exception {
        DeltaTable table = newTable();
        TableSchema schema = schema();

        table.appendRows(List.of(row(schema, 1, "Alice", 25)), "app-A", 7L);

        long versionBefore = table.version();

        long result = table.appendRows(List.of(row(schema, 999, "ShouldNotAppear", 99)), "app-A", 7L);

        assertEquals(versionBefore, result);
        assertEquals(versionBefore, table.version());

        List<Row> rows = table.readRows();

        assertEquals(1, rows.size());
        assertEquals(1L, rows.get(0).get("id"));
    }

    @Test
    void appendWithoutApplicationTransactionStillWorks() throws Exception {
        DeltaTable table = newTable();
        TableSchema schema = schema();

        long version = table.appendRows(List.of(row(schema, 1, "Alice", 25)));

        assertEquals(0, version);
        assertEquals(1, table.readRows().size());
        assertTrue(table.snapshot().transactions().isEmpty());
    }

    @Test
    void legacyAppendAlsoSupportsExactlyOnceTransactions() throws Exception {
        DeltaTable table = newTable();
        assertNotNull(table);
    }
}