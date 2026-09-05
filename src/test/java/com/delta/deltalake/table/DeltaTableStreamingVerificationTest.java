package com.delta.deltalake.table;

import com.delta.deltalake.data.Row;
import com.delta.deltalake.data.TableSchema;
import com.delta.deltalake.log.AddFile;
import com.delta.deltalake.log.RemoveFile;
import com.delta.deltalake.log.VersionedLogRecord;
import com.delta.deltalake.storage.LocalStorage;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DeltaTableStreamingVerificationTest {

    @TempDir
    Path tempDir;

    private DeltaTable newTable() {
        return DeltaTable.open(new LocalStorage(tempDir));
    }

    private TableSchema schema() {
        String json = """
                {
                  "type": "record",
                  "name": "TestRecord",
                  "namespace": "com.delta.test",
                  "fields": [
                    {"name": "id", "type": "long"},
                    {"name": "value", "type": "double"}
                  ]
                }
                """;

        return new TableSchema(new Schema.Parser().parse(json));
    }

    private Row row(TableSchema schema, long id, double value) {
        return Row.of(
                schema,
                Map.of(
                        "id", id,
                        "value", value
                )
        );
    }

    private AddFile addFile(VersionedLogRecord record) {
        assertTrue(record.record().action() instanceof AddFile);

        return (AddFile) record.record().action();
    }

    @Test
    void tailReturnsAllTransactionLogRecordsAfterVersion() throws Exception {
        DeltaTable table = newTable();
        TableSchema schema = schema();

        table.appendRows(List.of(
                row(schema, 1, 10.0)
        ));

        table.appendRows(List.of(
                row(schema, 2, 20.0)
        ));

        table.appendRows(List.of(
                row(schema, 3, 30.0)
        ));

        assertEquals(2, table.version());

        List<VersionedLogRecord> tail = table.tail(0);

        assertFalse(tail.isEmpty());

        assertTrue(
                tail.stream().allMatch(record -> record.version() >= 1)
        );

        assertTrue(
                tail.stream().allMatch(record -> record.version() <= 2)
        );

        assertTrue(
                tail.stream().anyMatch(record -> record.version() == 1)
        );

        assertTrue(
                tail.stream().anyMatch(record -> record.version() == 2)
        );
    }

    @Test
    void incrementalFilesReturnsOnlyDataChangeAddFiles() throws Exception {
        DeltaTable table = newTable();
        TableSchema schema = schema();

        table.appendRows(List.of(
                row(schema, 1, 10.0)
        ));

        table.appendRows(List.of(
                row(schema, 2, 20.0)
        ));

        table.appendRows(List.of(
                row(schema, 3, 30.0)
        ));

        List<VersionedLogRecord> changes = table.incrementalFiles(-1);

        assertEquals(3, changes.size());

        assertEquals(
                List.of(0L, 1L, 2L),
                changes.stream()
                        .map(VersionedLogRecord::version)
                        .toList()
        );

        for (VersionedLogRecord change : changes) {
            AddFile add = addFile(change);

            assertTrue(add.dataChange());
            assertNotNull(add.path());
            assertNotNull(add.stats());
        }
    }

    @Test
    void incrementalFilesRespectsStartingVersion() throws Exception {
        DeltaTable table = newTable();
        TableSchema schema = schema();

        table.appendRows(List.of(
                row(schema, 1, 10.0)
        ));

        table.appendRows(List.of(
                row(schema, 2, 20.0)
        ));

        table.appendRows(List.of(
                row(schema, 3, 30.0)
        ));

        List<VersionedLogRecord> afterVersionZero =
                table.incrementalFiles(0);

        assertEquals(
                List.of(1L, 2L),
                afterVersionZero.stream()
                        .map(VersionedLogRecord::version)
                        .toList()
        );

        List<VersionedLogRecord> afterVersionOne =
                table.incrementalFiles(1);

        assertEquals(
                List.of(2L),
                afterVersionOne.stream()
                        .map(VersionedLogRecord::version)
                        .toList()
        );

        List<VersionedLogRecord> afterVersionTwo =
                table.incrementalFiles(2);

        assertTrue(afterVersionTwo.isEmpty());
    }

    @Test
    void incrementalFilesReturnsEmptyWhenAlreadyAtLatestVersion() throws Exception {
        DeltaTable table = newTable();
        TableSchema schema = schema();

        table.appendRows(List.of(
                row(schema, 1, 10.0)
        ));

        assertEquals(0, table.version());

        List<VersionedLogRecord> changes =
                table.incrementalFiles(0);

        assertTrue(changes.isEmpty());
    }

    @Test
    void incrementalFilesRejectsInvalidStartingVersion() throws Exception {
        DeltaTable table = newTable();
        TableSchema schema = schema();

        table.appendRows(List.of(
                row(schema, 1, 10.0)
        ));

        assertThrows(
                IllegalArgumentException.class,
                () -> table.incrementalFiles(-2)
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> table.incrementalFiles(1)
        );
    }

    @Test
    void streamingConsumerStartsAtMinusOne() throws Exception {
        DeltaTable table = newTable();

        DeltaTable.StreamingConsumer consumer =
                table.streamingConsumer();

        assertEquals(-1, consumer.lastProcessedVersion());
    }

    @Test
    void streamingConsumerPollsNewDataAndAdvancesProgress() throws Exception {
        DeltaTable table = newTable();
        TableSchema schema = schema();

        table.appendRows(List.of(
                row(schema, 1, 10.0)
        ));

        table.appendRows(List.of(
                row(schema, 2, 20.0)
        ));

        DeltaTable.StreamingConsumer consumer =
                table.streamingConsumer();

        List<VersionedLogRecord> firstPoll = consumer.poll();

        assertEquals(2, firstPoll.size());

        assertEquals(
                List.of(0L, 1L),
                firstPoll.stream()
                        .map(VersionedLogRecord::version)
                        .toList()
        );

        assertEquals(1, consumer.lastProcessedVersion());
    }

    @Test
    void streamingConsumerDoesNotReturnAlreadyProcessedFiles() throws Exception {
        DeltaTable table = newTable();
        TableSchema schema = schema();

        table.appendRows(List.of(
                row(schema, 1, 10.0)
        ));

        DeltaTable.StreamingConsumer consumer =
                table.streamingConsumer();

        List<VersionedLogRecord> firstPoll = consumer.poll();

        assertEquals(1, firstPoll.size());
        assertEquals(0, consumer.lastProcessedVersion());

        List<VersionedLogRecord> secondPoll = consumer.poll();

        assertTrue(secondPoll.isEmpty());
        assertEquals(0, consumer.lastProcessedVersion());
    }

    @Test
    void streamingConsumerFindsOnlyNewFilesAfterAdditionalAppend() throws Exception {
        DeltaTable table = newTable();
        TableSchema schema = schema();

        table.appendRows(List.of(
                row(schema, 1, 10.0)
        ));

        DeltaTable.StreamingConsumer consumer =
                table.streamingConsumer();

        List<VersionedLogRecord> firstPoll = consumer.poll();

        assertEquals(1, firstPoll.size());
        assertEquals(0, firstPoll.get(0).version());
        assertEquals(0, consumer.lastProcessedVersion());

        table.appendRows(List.of(
                row(schema, 2, 20.0)
        ));

        table.appendRows(List.of(
                row(schema, 3, 30.0)
        ));

        List<VersionedLogRecord> secondPoll = consumer.poll();

        assertEquals(2, secondPoll.size());

        assertEquals(
                List.of(1L, 2L),
                secondPoll.stream()
                        .map(VersionedLogRecord::version)
                        .toList()
        );

        assertEquals(2, consumer.lastProcessedVersion());
    }

    @Test
    void emptyPollAdvancesConsumerToLatestVersion() throws Exception {
        DeltaTable table = newTable();
        TableSchema schema = schema();

        table.appendRows(List.of(
                row(schema, 1, 10.0)
        ));

        DeltaTable.StreamingConsumer consumer =
                table.streamingConsumer();

        List<VersionedLogRecord> firstPoll = consumer.poll();

        assertEquals(1, firstPoll.size());
        assertEquals(0, consumer.lastProcessedVersion());

        List<VersionedLogRecord> secondPoll = consumer.poll();

        assertTrue(secondPoll.isEmpty());

        assertEquals(
                table.version(),
                consumer.lastProcessedVersion()
        );
    }

    @Test
    void optimizeDoesNotProduceStreamingDataChanges() throws Exception {
        DeltaTable table = newTable();
        TableSchema schema = schema();

        table.appendRows(List.of(
                row(schema, 1, 10.0)
        ));

        DeltaTable.StreamingConsumer consumer =
                table.streamingConsumer();

        // Consume version 0.
        List<VersionedLogRecord> initial = consumer.poll();

        assertEquals(1, initial.size());
        assertEquals(0, initial.get(0).version());
        assertEquals(0, consumer.lastProcessedVersion());

        // Version 1 is a genuine data change.
        table.appendRows(List.of(
                row(schema, 2, 20.0)
        ));

        // The consumer must see the append.
        List<VersionedLogRecord> appendChanges = consumer.poll();

        assertEquals(1, appendChanges.size());
        assertEquals(1, appendChanges.get(0).version());

        AddFile appendedFile = addFile(appendChanges.get(0));

        assertTrue(appendedFile.dataChange());
        assertEquals(1, consumer.lastProcessedVersion());

        // Version 2 is an OPTIMIZE rewrite.
        long optimizeVersion = table.optimize();

        assertEquals(2, optimizeVersion);

        // OPTIMIZE creates RemoveFile/AddFile actions with
        // dataChange=false. Therefore the streaming consumer
        // should not emit anything for the optimize commit.
        List<VersionedLogRecord> optimizeChanges = consumer.poll();

        assertTrue(optimizeChanges.isEmpty());

        // Even though no data-change files were emitted, the
        // consumer must advance past the optimize version.
        assertEquals(
                optimizeVersion,
                consumer.lastProcessedVersion()
        );
    }

    @Test
    void incrementalFilesIgnoresRemoveFiles() throws Exception {
        DeltaTable table = newTable();
        TableSchema schema = schema();

        table.appendRows(List.of(
                row(schema, 1, 10.0)
        ));

        table.appendRows(List.of(
                row(schema, 2, 20.0)
        ));

        DeltaTable.StreamingConsumer consumer =
                table.streamingConsumer();

        List<VersionedLogRecord> initial = consumer.poll();

        assertEquals(2, initial.size());
        assertEquals(1, consumer.lastProcessedVersion());

        table.deleteRows(r ->
                ((Number) r.get("id")).longValue() == 1L
        );

        List<VersionedLogRecord> afterDelete =
                consumer.poll();

        // A delete generates a RemoveFile and possibly a rewritten
        // AddFile. The RemoveFile itself must never be returned.
        assertTrue(
                afterDelete.stream().noneMatch(record ->
                        record.record().action() instanceof RemoveFile
                )
        );

        for (VersionedLogRecord record : afterDelete) {
            assertTrue(record.record().action() instanceof AddFile);
            assertTrue(addFile(record).dataChange());
        }
    }

    @Test
    void streamingConsumerAndIncrementalFilesAgree() throws Exception {
        DeltaTable table = newTable();
        TableSchema schema = schema();

        table.appendRows(List.of(
                row(schema, 1, 10.0)
        ));

        table.appendRows(List.of(
                row(schema, 2, 20.0)
        ));

        DeltaTable.StreamingConsumer consumer =
                table.streamingConsumer();

        List<VersionedLogRecord> expected =
                table.incrementalFiles(-1);

        List<VersionedLogRecord> actual =
                consumer.poll();

        assertEquals(expected, actual);

        assertEquals(
                actual.isEmpty()
                        ? table.version()
                        : actual.get(actual.size() - 1).version(),
                consumer.lastProcessedVersion()
        );
    }

    @Test
    void tailAndIncrementalFilesHaveCorrectVersionOrdering() throws Exception {
        DeltaTable table = newTable();
        TableSchema schema = schema();

        table.appendRows(List.of(
                row(schema, 1, 10.0)
        ));

        table.appendRows(List.of(
                row(schema, 2, 20.0)
        ));

        table.appendRows(List.of(
                row(schema, 3, 30.0)
        ));

        List<VersionedLogRecord> tail = table.tail(-1);
        List<VersionedLogRecord> incremental =
                table.incrementalFiles(-1);

        assertFalse(tail.isEmpty());
        assertFalse(incremental.isEmpty());

        assertTrue(
                isNonDecreasing(
                        tail.stream()
                                .map(VersionedLogRecord::version)
                                .toList()
                )
        );

        assertTrue(
                isNonDecreasing(
                        incremental.stream()
                                .map(VersionedLogRecord::version)
                                .toList()
                )
        );

        assertEquals(
                List.of(0L, 1L, 2L),
                incremental.stream()
                        .map(VersionedLogRecord::version)
                        .distinct()
                        .toList()
        );
    }

    private boolean isNonDecreasing(List<Long> values) {
        for (int i = 1; i < values.size(); i++) {
            if (values.get(i) < values.get(i - 1)) {
                return false;
            }
        }
        return true;
    }
}