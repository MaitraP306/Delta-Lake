package com.delta.deltalake.table;

import com.delta.deltalake.log.*;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.LinkedHashMap;
import java.util.Map;

public final class SnapshotBuilder {
    private final Map<String, AddFile> activeFiles = new LinkedHashMap<>();
    private final Map<String, RemoveFile> tombstones = new LinkedHashMap<>();
    private final Map<String, Txn> transactions = new LinkedHashMap<>();
    private Metadata metadata;
    private Protocol protocol = new Protocol(1, 1);
    private final ObjectMapper mapper;

    public SnapshotBuilder() { this(new ObjectMapper()); }
    public SnapshotBuilder(ObjectMapper mapper) { this.mapper = mapper; }
    public SnapshotBuilder(Map<String, AddFile> initialFiles) {
        this();
        activeFiles.putAll(initialFiles);
    }

    public void apply(LogRecord record) {
        apply(ActionCodec.decode(record.type(), record.action(), mapper));
    }

    public void apply(LogAction action) {
        switch (action) {
            case AddFile add -> {
                activeFiles.put(add.path(), add);
                tombstones.remove(add.path());
            }
            case RemoveFile remove -> {
                activeFiles.remove(remove.path());
                tombstones.put(remove.path(), remove);
            }
            case Metadata value -> metadata = value;
            case Protocol value -> protocol = value;
            case Txn value -> {
                Txn previous = transactions.get(value.appId());
                if (previous == null || value.version() >= previous.version()) {
                    transactions.put(value.appId(), value);
                }
            }
            case CommitInfo ignored -> {
                // CommitInfo is provenance and is not part of snapshot state.
            }
        }
    }

    public Snapshot build(long version) {
        return new Snapshot(version, activeFiles, tombstones, metadata, protocol, transactions);
    }
}
