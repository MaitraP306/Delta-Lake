package com.delta.deltalake.log;

import java.util.Map;

public record FileStats(long numRecords, Map<String, ColumnStats> columns) {

    public FileStats {
        columns = Map.copyOf(columns == null ? Map.of() : columns);
    }

    public record ColumnStats(Object min, Object max, long nullCount) {}
}