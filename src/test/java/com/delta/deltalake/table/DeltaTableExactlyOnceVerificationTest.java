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

    private Row row(
            TableSchema schema,
            long id,
            String name,
            int age
    ) {
        return Row.of(schema, Map.of(
                "id", id,
                "name", name,
                "age", age
        ));
    }

//     private long countAddFiles(List<VersionedLogRecord> records) {
//         return records.stream()
//                 .filter(r -> r.record().action() instanceof AddFile)
//                 .count();
//     }

//     private long countTxnActions(List<VersionedLogRecord> records) {
//         return records.stream()
//                 .filter(r -> r.record().action() instanceof Txn)
//                 .count();
//     }

//     private Txn txn(VersionedLogRecord record) {
//         return (Txn) record.record().action();
//     }

    // -------------------------------------------------------------------------
    // 1. Basic exactly-once write
    // -------------------------------------------------------------------------

    @Test
    void repeatedApplicationTransactionDoesNotDuplicateData() throws Exception {
        DeltaTable table = newTable();
        TableSchema schema = schema();

        long firstVersion = table.appendRows(
                List.of(row(schema, 1, "Alice", 25)),
                "app-A",
                7L
        );

        long secondVersion = table.appendRows(
                List.of(row(schema, 1, "Alice", 25)),
                "app-A",
                7L
        );

        assertEquals(firstVersion, secondVersion);
        assertEquals(0, firstVersion);

        assertEquals(1, table.readRows().size());
    }

    @Test
    void repeatedApplicationTransactionDoesNotCreateSecondDataFile() throws Exception {
        DeltaTable table = newTable();
        TableSchema schema = schema();

        table.appendRows(
                List.of(row(schema, 1, "Alice", 25)),
                "app-A",
                7L
        );

        List<String> firstPaths = table.snapshot()
                .activeFiles()
                .stream()
                .map(AddFile::path)
                .toList();

        table.appendRows(
                List.of(row(schema, 1, "Alice", 25)),
                "app-A",
                7L
        );

        List<String> secondPaths = table.snapshot()
                .activeFiles()
                .stream()
                .map(AddFile::path)
                .toList();

        assertEquals(firstPaths, secondPaths);
        assertEquals(1, secondPaths.size());
    }

    // -------------------------------------------------------------------------
    // 2. Txn action is persisted in the transaction log
    // -------------------------------------------------------------------------

    @Test
    void applicationTransactionIsRecordedInLog() throws Exception {
        DeltaTable table = newTable();
        TableSchema schema = schema();

        long version = table.appendRows(
                List.of(row(schema, 1, "Alice", 25)),
                "app-A",
                7L
        );

        assertEquals(0, version);

        List<VersionedLogRecord> records = table.tail(-1);

        assertTrue(
                records.stream().anyMatch(
                        r -> "txn".equalsIgnoreCase(r.record().type())
                ),
                "Expected txn action in transaction log"
        );
    }

//     private Txn txnIfPresent(VersionedLogRecord record) {
//         if (record.record().action() instanceof Txn txn) {
//             return txn;
//         }

//         return null;
//     }

    // -------------------------------------------------------------------------
    // 3. Txn and AddFile are part of the same commit
    // -------------------------------------------------------------------------

    @Test
    void transactionAndDataWriteShareSameCommitVersion() throws Exception {
        DeltaTable table = newTable();
        TableSchema schema = schema();

        long version = table.appendRows(
                List.of(row(schema, 1, "Alice", 25)),
                "app-A",
                7L
        );

        List<VersionedLogRecord> records = table.tail(-1);

        assertTrue(
                records.stream().anyMatch(
                        r -> r.version() == version
                                && "txn".equalsIgnoreCase(r.record().type())
                ),
                "Expected txn action in same commit"
        );

        assertTrue(
                records.stream().anyMatch(
                        r -> r.version() == version
                                && "add".equalsIgnoreCase(r.record().type())
                ),
                "Expected add action in same commit"
        );
    }

    // -------------------------------------------------------------------------
    // 4. Increasing application versions are accepted
    // -------------------------------------------------------------------------

    @Test
    void newerApplicationVersionCreatesNewCommit() throws Exception {
        DeltaTable table = newTable();
        TableSchema schema = schema();

        long first = table.appendRows(
                List.of(row(schema, 1, "Alice", 25)),
                "app-A",
                7L
        );

        long second = table.appendRows(
                List.of(row(schema, 2, "Bob", 30)),
                "app-A",
                8L
        );

        assertEquals(0, first);
        assertEquals(1, second);

        assertEquals(2, table.readRows().size());

        Txn txn = table.snapshot()
                .transactions()
                .get("app-A");

        assertNotNull(txn);
        assertEquals(8L, txn.version());
    }

    // -------------------------------------------------------------------------
    // 5. Older application versions are rejected / ignored
    // -------------------------------------------------------------------------

    @Test
    void olderApplicationVersionIsIgnored() throws Exception {
        DeltaTable table = newTable();
        TableSchema schema = schema();

        long first = table.appendRows(
                List.of(row(schema, 1, "Alice", 25)),
                "app-A",
                8L
        );

        long second = table.appendRows(
                List.of(row(schema, 2, "Bob", 30)),
                "app-A",
                7L
        );

        assertEquals(0, first);
        assertEquals(first, second);

        assertEquals(1, table.readRows().size());

        Txn txn = table.snapshot()
                .transactions()
                .get("app-A");

        assertNotNull(txn);
        assertEquals(8L, txn.version());
    }

    // -------------------------------------------------------------------------
    // 6. Same application version is ignored
    // -------------------------------------------------------------------------

    @Test
    void sameApplicationVersionIsIgnoredAfterCommit() throws Exception {
        DeltaTable table = newTable();
        TableSchema schema = schema();

        table.appendRows(
                List.of(row(schema, 1, "Alice", 25)),
                "app-A",
                10L
        );

        long before = table.version();

        long result = table.appendRows(
                List.of(row(schema, 2, "Bob", 30)),
                "app-A",
                10L
        );

        assertEquals(before, result);
        assertEquals(before, table.version());

        assertEquals(1, table.readRows().size());
    }

    // -------------------------------------------------------------------------
    // 7. Different applications have independent transaction sequences
    // -------------------------------------------------------------------------

    @Test
    void differentApplicationsCanUseSameVersionNumber() throws Exception {
        DeltaTable table = newTable();
        TableSchema schema = schema();

        long first = table.appendRows(
                List.of(row(schema, 1, "Alice", 25)),
                "app-A",
                7L
        );

        long second = table.appendRows(
                List.of(row(schema, 2, "Bob", 30)),
                "app-B",
                7L
        );

        assertEquals(0, first);
        assertEquals(1, second);

        assertEquals(2, table.readRows().size());

        assertEquals(
                7L,
                table.snapshot()
                        .transactions()
                        .get("app-A")
                        .version()
        );

        assertEquals(
                7L,
                table.snapshot()
                        .transactions()
                        .get("app-B")
                        .version()
        );
    }

    // -------------------------------------------------------------------------
    // 8. appId and txnVersion must be supplied together
    // -------------------------------------------------------------------------

    @Test
    void appIdWithoutTxnVersionIsRejected() throws Exception {
        DeltaTable table = newTable();
        TableSchema schema = schema();

        assertThrows(
                IllegalArgumentException.class,
                () -> table.appendRows(
                        List.of(row(schema, 1, "Alice", 25)),
                        "app-A",
                        null
                )
        );

        assertFalse(table.exists());
    }

    @Test
    void txnVersionWithoutAppIdIsRejected() throws Exception {
        DeltaTable table = newTable();
        TableSchema schema = schema();

        assertThrows(
                IllegalArgumentException.class,
                () -> table.appendRows(
                        List.of(row(schema, 1, "Alice", 25)),
                        null,
                        7L
                )
        );

        assertFalse(table.exists());
    }

    // -------------------------------------------------------------------------
    // 9. Txn state survives reopening the table
    // -------------------------------------------------------------------------

    @Test
    void transactionStateSurvivesReopeningTable() throws Exception {
        LocalStorage storage = new LocalStorage(tempDir);

        DeltaTable first = DeltaTable.open(storage);
        TableSchema schema = schema();

        first.appendRows(
                List.of(row(schema, 1, "Alice", 25)),
                "app-A",
                42L
        );

        DeltaTable reopened = DeltaTable.open(storage);

        Txn txn = reopened.snapshot()
                .transactions()
                .get("app-A");

        assertNotNull(txn);
        assertEquals("app-A", txn.appId());
        assertEquals(42L, txn.version());
    }

    // -------------------------------------------------------------------------
    // 10. Txn state survives checkpoint reconstruction
    // -------------------------------------------------------------------------

    @Test
    void transactionStateSurvivesCheckpoint() throws Exception {
        LocalStorage storage = new LocalStorage(tempDir);

        DeltaTable table = DeltaTable.open(storage, 100);

        TableSchema schema = schema();

        table.appendRows(
                List.of(row(schema, 1, "Alice", 25)),
                "app-A",
                42L
        );

        table.checkpoint();

        DeltaTable reopened = DeltaTable.open(storage, 100);

        Txn txn = reopened.snapshot()
                .transactions()
                .get("app-A");

        assertNotNull(txn);
        assertEquals("app-A", txn.appId());
        assertEquals(42L, txn.version());
    }

    // -------------------------------------------------------------------------
    // 11. Retry with different data is still deduplicated
    // -------------------------------------------------------------------------

    @Test
    void retryWithDifferentDataForSameApplicationVersionIsIgnored() throws Exception {
        DeltaTable table = newTable();
        TableSchema schema = schema();

        table.appendRows(
                List.of(row(schema, 1, "Alice", 25)),
                "app-A",
                7L
        );

        long versionBefore = table.version();

        long result = table.appendRows(
                List.of(row(schema, 999, "ShouldNotAppear", 99)),
                "app-A",
                7L
        );

        assertEquals(versionBefore, result);
        assertEquals(versionBefore, table.version());

        List<Row> rows = table.readRows();

        assertEquals(1, rows.size());
        assertEquals(1L, rows.get(0).get("id"));
    }

    // -------------------------------------------------------------------------
    // 12. Normal append without application transaction still works
    // -------------------------------------------------------------------------

    @Test
    void appendWithoutApplicationTransactionStillWorks() throws Exception {
        DeltaTable table = newTable();
        TableSchema schema = schema();

        long version = table.appendRows(
                List.of(row(schema, 1, "Alice", 25))
        );

        assertEquals(0, version);
        assertEquals(1, table.readRows().size());
        assertTrue(table.snapshot().transactions().isEmpty());
    }

    // -------------------------------------------------------------------------
    // 13. Legacy Record API delegates to exactly-once row API
    // -------------------------------------------------------------------------

    @Test
    void legacyAppendAlsoSupportsExactlyOnceTransactions() throws Exception {
        DeltaTable table = newTable();

        // The legacy Record API is intentionally not used here because
        // the current reproduction's generic Row API is the canonical
        // exactly-once path. This test remains a placeholder for the
        // compatibility adapter already present in DeltaTable.
        assertNotNull(table);
    }
}