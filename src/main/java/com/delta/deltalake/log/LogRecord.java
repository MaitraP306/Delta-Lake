package com.delta.deltalake.log;

public record LogRecord(
        String type,
        Object action
) {
}