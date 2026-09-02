package com.delta.deltalake.table;

import java.util.Map;

public record HistoryEntry(long version, long timestamp, String operation, Map<String, String> operationParameters, String userMetadata) {
}
