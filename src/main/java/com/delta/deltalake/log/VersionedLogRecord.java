package com.delta.deltalake.log;

public record VersionedLogRecord(long version, LogRecord record) {
}
