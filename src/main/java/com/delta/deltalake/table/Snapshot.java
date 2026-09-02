package com.delta.deltalake.table;

import com.delta.deltalake.log.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class Snapshot {
    private final long version;
    private final Map<String, AddFile> activeFiles;
    private final Map<String, RemoveFile> tombstones;
    private final Metadata metadata;
    private final Protocol protocol;
    private final Map<String, Txn> transactions;

    public Snapshot(long version, Map<String, AddFile> activeFiles) {
        this(version, activeFiles, Map.of(), null, new Protocol(1, 1), Map.of());
    }

    Snapshot(long version, Map<String, AddFile> activeFiles, Map<String, RemoveFile> tombstones, Metadata metadata, Protocol protocol, Map<String, Txn> transactions) {
        this.version = version;
        this.activeFiles = new LinkedHashMap<>(activeFiles);
        this.tombstones = new LinkedHashMap<>(tombstones);
        this.metadata = metadata;
        this.protocol = protocol;
        this.transactions = new LinkedHashMap<>(transactions);
    }

    public long version() { return version; }
    public Collection<AddFile> activeFiles() { return Collections.unmodifiableCollection(activeFiles.values()); }
    public Collection<RemoveFile> tombstones() { return Collections.unmodifiableCollection(tombstones.values()); }
    public boolean contains(String path) { return activeFiles.containsKey(path); }
    public int fileCount() { return activeFiles.size(); }
    public Metadata metadata() { return metadata; }
    public Protocol protocol() { return protocol; }
    public Map<String, Txn> transactions() { return Collections.unmodifiableMap(transactions); }

    List<LogAction> checkpointActions() {
        List<LogAction> actions = new ArrayList<>();
        if (protocol != null) {
            actions.add(protocol);
        }
        if (metadata != null) {
            actions.add(metadata);
        }
        actions.addAll(activeFiles.values());
        actions.addAll(tombstones.values());
        actions.addAll(transactions.values());
        return actions;
    }
}
