package com.delta.deltalake.table;

import com.delta.deltalake.log.AddFile;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public final class Snapshot {

    private final long version;
    private final Map<String, AddFile> activeFiles;

    public Snapshot(long version, Map<String, AddFile> activeFiles) {
        this.version = version;
        this.activeFiles = new LinkedHashMap<>(activeFiles);
    }

    public long version() {
        return version;
    }

    public Collection<AddFile> activeFiles() {
        return Collections.unmodifiableCollection(activeFiles.values());
    }

    public boolean contains(String path) {
        return activeFiles.containsKey(path);
    }

    public int fileCount() {
        return activeFiles.size();
    }
}