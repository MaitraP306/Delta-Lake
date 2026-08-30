package com.delta.deltalake.table;

import com.delta.deltalake.log.AddFile;
import com.delta.deltalake.log.LogAction;
import com.delta.deltalake.log.RemoveFile;

import java.util.LinkedHashMap;
import java.util.Map;

public final class SnapshotBuilder {

    private final Map<String, AddFile> activeFiles;

    public SnapshotBuilder() {
        this.activeFiles = new LinkedHashMap<>();
    }

    public SnapshotBuilder(Map<String, AddFile> initialFiles) {
        this.activeFiles = new LinkedHashMap<>(initialFiles);
    }

    public void apply(LogAction action) {
        if (action instanceof AddFile addFile) {
            activeFiles.put(addFile.path(), addFile);
        } else if (action instanceof RemoveFile removeFile) {
            activeFiles.remove(removeFile.path());
        } else {
            throw new IllegalArgumentException(
                    "Unsupported action type: " +
                    action.getClass().getName()
            );
        }
    }

    public Snapshot build(long version) {
        return new Snapshot(version, activeFiles);
    }
}