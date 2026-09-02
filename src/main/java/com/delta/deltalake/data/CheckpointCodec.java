package com.delta.deltalake.data;

import com.delta.deltalake.log.AddFile;
import com.delta.deltalake.log.CommitInfo;
import com.delta.deltalake.log.FileStats;
import com.delta.deltalake.log.LogAction;
import com.delta.deltalake.log.Metadata;
import com.delta.deltalake.log.Protocol;
import com.delta.deltalake.log.RemoveFile;
import com.delta.deltalake.log.Txn;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class CheckpointCodec {

    private CheckpointCodec() {}

    public static GenericRecord encode(LogAction action) {
        GenericRecord row = new GenericData.Record(CheckpointSchema.schema());
        switch (action) {
            case AddFile add -> {
                GenericRecord value = new GenericData.Record(CheckpointSchema.addSchema());
                value.put("path", add.path());
                value.put("size", add.size());
                value.put("modificationTime", add.modificationTime());
                value.put("dataChange", add.dataChange());
                if (add.stats() != null) {
                    value.put("numRecords", add.stats().numRecords());
                    value.put("stats", encodeStats(add.stats()));
                } else {
                    value.put("numRecords", 0L);
                    value.put("stats", null);
                }

                row.put("add", value);
            }

            case RemoveFile remove -> {
                GenericRecord value = new GenericData.Record(CheckpointSchema.removeSchema());

                value.put("path", remove.path());
                value.put("deletionTimestamp", remove.deletionTimestamp());
                value.put("dataChange", remove.dataChange());

                row.put("remove", value);
            }

            case Metadata metadata -> {
                GenericRecord value = new GenericData.Record(CheckpointSchema.metadataSchema());

                value.put("id", metadata.id());
                value.put("format", metadata.format());
                value.put("schemaString", metadata.schemaString());
                value.put("partitionColumns", new ArrayList<>(metadata.partitionColumns()));
                value.put("configuration", new HashMap<>(metadata.configuration()));
                row.put("metadata", value);
            }

            case Protocol protocol -> {
                GenericRecord value = new GenericData.Record(CheckpointSchema.protocolSchema());

                value.put("minReaderVersion", protocol.minReaderVersion());
                value.put("minWriterVersion", protocol.minWriterVersion());
                row.put("protocol", value);
            }

            case Txn txn -> {
                GenericRecord value = new GenericData.Record(CheckpointSchema.txnSchema());

                value.put("appId", txn.appId());
                value.put("version", txn.version());
                value.put("lastUpdated", txn.lastUpdated());
                row.put("txn", value);
            }

            case CommitInfo ignored -> {
                throw new IllegalArgumentException("CommitInfo is not stored in checkpoints");
            }
        }

        return row;
    }

    public static LogAction decode(GenericRecord row) {

        GenericRecord add = (GenericRecord) row.get("add");

        if (add != null) {
            return decodeAdd(add);
        }

        GenericRecord remove = (GenericRecord) row.get("remove");

        if (remove != null) {
            return decodeRemove(remove);
        }

        GenericRecord metadata = (GenericRecord) row.get("metadata");

        if (metadata != null) {
            return decodeMetadata(metadata);
        }

        GenericRecord protocol = (GenericRecord) row.get("protocol");

        if (protocol != null) {
            return decodeProtocol(protocol);
        }

        GenericRecord txn = (GenericRecord) row.get("txn");

        if (txn != null) {
            return decodeTxn(txn);
        }

        throw new IllegalArgumentException("Checkpoint row contains no action");
    }

    private static List<GenericRecord> encodeStats(FileStats stats) {
        List<GenericRecord> result = new ArrayList<>();
        for (Map.Entry<String, FileStats.ColumnStats> entry : stats.columns().entrySet()) {
            FileStats.ColumnStats columnStats = entry.getValue();
            GenericRecord value = new GenericData.Record(CheckpointSchema.columnStatsSchema());
            value.put("column", entry.getKey());

            value.put("min", encodeStatValue(columnStats.min()));

            value.put("max", encodeStatValue(columnStats.max()));

            value.put("nullCount", columnStats.nullCount());

            result.add(value);
        }

        return result;
    }

    private static Object encodeStatValue(Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof Long || value instanceof Integer || value instanceof Double || value instanceof Float || value instanceof String|| value instanceof Boolean) {
            return value;
        }

        throw new IllegalArgumentException("Unsupported statistic type: " + value.getClass().getName());
    }

    private static FileStats decodeStats(GenericRecord value) {
        if (value == null) {
            return null;
        }

        @SuppressWarnings("unchecked")
        List<GenericRecord> stats = (List<GenericRecord>) value.get("stats");
        Map<String, FileStats.ColumnStats> columns = new HashMap<>();

        if (stats != null) {
            for (GenericRecord columnStat : stats) {
                String column = columnStat.get("column").toString();

                Object min = columnStat.get("min");

                Object max = columnStat.get("max");

                long nullCount = (Long) columnStat.get("nullCount");

                columns.put(column, new FileStats.ColumnStats(min, max, nullCount));
            }
        }

        return new FileStats((Long) value.get("numRecords"), columns);
    }

    private static AddFile decodeAdd(GenericRecord value) {
        return new AddFile(value.get("path").toString(), (Long) value.get("size"), (Long) value.get("modificationTime"), (Boolean) value.get("dataChange"), decodeStats(value));
    }

    private static RemoveFile decodeRemove(GenericRecord value) {
        return new RemoveFile(value.get("path").toString(), (Long) value.get("deletionTimestamp"), (Boolean) value.get("dataChange"));
    }

    @SuppressWarnings("unchecked")
    private static Metadata decodeMetadata(GenericRecord value) {
        List<String> partitionColumns = ((List<CharSequence>) value.get("partitionColumns")).stream().map(Object::toString).toList();

        Map<String, String> configuration = ((Map<CharSequence, CharSequence>) value.get("configuration")).entrySet().stream().collect(java.util.stream.Collectors.toMap(e -> e.getKey().toString(), e -> e.getValue().toString()));

        return new Metadata(value.get("id").toString(), value.get("format").toString(), value.get("schemaString").toString(), partitionColumns, configuration);
    }

    private static Protocol decodeProtocol(GenericRecord value) {
        return new Protocol((Integer) value.get("minReaderVersion"), (Integer) value.get("minWriterVersion"));
    }

    private static Txn decodeTxn(GenericRecord value) {
        return new Txn(value.get("appId").toString(), (Long) value.get("version"), (Long) value.get("lastUpdated"));
    }
}