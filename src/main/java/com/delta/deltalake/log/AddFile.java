package com.delta.deltalake.log;

public record AddFile(String path, long size, long modificationTime, boolean dataChange, FileStats stats) implements LogAction {
    public AddFile(String path, long size, long modificationTime, boolean dataChange) {
        this(path, size, modificationTime, dataChange, null);
    }
}