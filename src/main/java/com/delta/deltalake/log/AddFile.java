package com.delta.deltalake.log;

public record AddFile(
        String path,
        long size,
        long modificationTime,
        boolean dataChange
) implements LogAction {
}