package com.delta.deltalake.table;

import com.delta.deltalake.data.Row;
import com.delta.deltalake.storage.LocalStorage;
import com.delta.deltalake.storage.Storage;
import com.delta.deltalake.log.LastCheckpoint;
import com.delta.deltalake.log.TransactionLog;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class CheckpointManagerTest {
    @Test
    void olderCheckpointCannotRegressLastCheckpointPointer() throws Exception {
        Path root = Files.createTempDirectory("delta-checkpoint-pointer-");
        try {
            Storage storage = new LocalStorage(root);
            DeltaTable table = DeltaTable.open(storage, 100);
            table.appendRows(List.of(Row.infer(Map.of("id", 1L, "name", "a"))));
            table.appendRows(List.of(Row.infer(Map.of("id", 2L, "name", "b"))));
            CheckpointManager manager = new CheckpointManager(storage, new TransactionLog(storage));
            manager.create(1);
            manager.create(0);
            LastCheckpoint pointer = readPointer(storage);
            assertEquals(1L, pointer.version());
        } finally {
            delete(root);
        }
    }

    @Test
    void checkpointDropsExpiredTombstoneAfterVacuum() throws Exception {
        Path root = Files.createTempDirectory("delta-checkpoint-retention-");
        try {
            Storage storage = new LocalStorage(root);
            DeltaTable table = DeltaTable.open(storage, 100);
            table.appendRows(List.of(Row.infer(Map.of("id", 1L, "name", "a"))));
            long deletedVersion = table.deleteRows(r -> ((Number) r.get("id")).longValue() == 1L);
            table.setRetention(Duration.ZERO);
            table.vacuum(Duration.ZERO);
            table.checkpoint();
            assertNotNull(table.snapshot().metadata());
            assertTrue(table.snapshot(deletedVersion).tombstones().size() >= 1);
            assertEquals(0, table.snapshot().tombstones().size());
        } finally {
            delete(root);
        }
    }

    private static LastCheckpoint readPointer(Storage storage) throws Exception {
        return new TransactionLog(storage).deserialize(storage.read(TransactionLog.LAST_CHECKPOINT), LastCheckpoint.class);
    }

    private static void delete(Path root) throws Exception {
        try (var stream = Files.walk(root)) {
            stream.sorted(java.util.Comparator.reverseOrder()).forEach(path -> {
                try { Files.deleteIfExists(path); } catch (Exception e) { throw new RuntimeException(e); }
            });
        }
    }
}
