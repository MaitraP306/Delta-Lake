package com.delta.deltalake.log;

import java.util.Map;

public record CommitInfo(long timestamp, String operation, Map<String, String> operationParameters, String userMetadata) implements LogAction {
    public CommitInfo {
        operationParameters = Map.copyOf(operationParameters == null ? Map.of() : operationParameters);
    }
}
