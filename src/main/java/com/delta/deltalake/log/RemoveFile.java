package com.delta.deltalake.log;
public record RemoveFile(String path, long deletionTimestamp, boolean dataChange) implements LogAction {
}
