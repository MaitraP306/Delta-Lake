package com.delta.deltalake.log;

public record Txn(String appId, long version, long lastUpdated) implements LogAction {
}
