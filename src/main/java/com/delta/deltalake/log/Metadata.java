package com.delta.deltalake.log;
import java.util.List;
import java.util.Map;
public record Metadata(String id, String format, String schemaString, List<String> partitionColumns, Map<String, String> configuration) implements LogAction {
    public Metadata {
        partitionColumns = List.copyOf(partitionColumns == null ? List.of() : partitionColumns);
        configuration = Map.copyOf(configuration == null ? Map.of() : configuration);
    }
}
