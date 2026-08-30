package com.delta.deltalake.table;

import com.delta.deltalake.log.AddFile;
import com.delta.deltalake.log.RemoveFile;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SnapshotBuilderTest {

    @Test
    void shouldApplyAddAndRemoveActions() {
        SnapshotBuilder builder = new SnapshotBuilder();

        AddFile fileA = new AddFile(
                "data/a.parquet",
                100,
                1000,
                true
        );

        AddFile fileB = new AddFile(
                "data/b.parquet",
                200,
                2000,
                true
        );

        RemoveFile removeA = new RemoveFile(
                "data/a.parquet",
                3000,
                true
        );

        builder.apply(fileA);
        builder.apply(fileB);
        builder.apply(removeA);

        Snapshot snapshot = builder.build(2);

        assertEquals(2, snapshot.version());
        assertEquals(1, snapshot.fileCount());
        assertFalse(snapshot.contains("data/a.parquet"));
        assertTrue(snapshot.contains("data/b.parquet"));
    }

    @Test
    void snapshotShouldNotChangeAfterBuilderMutation() {
        SnapshotBuilder builder = new SnapshotBuilder();

        AddFile fileA = new AddFile(
                "data/a.parquet",
                100,
                1000,
                true
        );

        builder.apply(fileA);

        Snapshot snapshot = builder.build(0);

        AddFile fileB = new AddFile(
                "data/b.parquet",
                200,
                2000,
                true
        );

        builder.apply(fileB);

        assertEquals(1, snapshot.fileCount());
        assertTrue(snapshot.contains("data/a.parquet"));
        assertFalse(snapshot.contains("data/b.parquet"));
    }
}