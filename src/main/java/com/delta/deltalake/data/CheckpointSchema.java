package com.delta.deltalake.data;

import org.apache.avro.Schema;

import java.util.List;

public final class CheckpointSchema {

    private static final Schema COLUMN_STATS_SCHEMA;
    private static final Schema STATS_SCHEMA;

    private static final Schema ADD_SCHEMA;
    private static final Schema REMOVE_SCHEMA;
    private static final Schema METADATA_SCHEMA;
    private static final Schema PROTOCOL_SCHEMA;
    private static final Schema TXN_SCHEMA;

    private static final Schema CHECKPOINT_SCHEMA;

    static {
        COLUMN_STATS_SCHEMA =
                Schema.createRecord(
                        "ColumnStats",
                        null,
                        "com.delta.deltalake.data",
                        false
                );

        Schema nullableStat =
                Schema.createUnion(
                        Schema.create(Schema.Type.NULL),
                        Schema.create(Schema.Type.LONG),
                        Schema.create(Schema.Type.INT),
                        Schema.create(Schema.Type.DOUBLE),
                        Schema.create(Schema.Type.FLOAT),
                        Schema.create(Schema.Type.STRING),
                        Schema.create(Schema.Type.BOOLEAN),
                        Schema.create(Schema.Type.BYTES)
                );

        COLUMN_STATS_SCHEMA.setFields(
                List.of(
                        new Schema.Field(
                                "column",
                                Schema.create(Schema.Type.STRING),
                                null,
                                null
                        ),
                        new Schema.Field(
                                "min",
                                nullableStat,
                                null,
                                null
                        ),
                        new Schema.Field(
                                "max",
                                nullableStat,
                                null,
                                null
                        ),
                        new Schema.Field(
                                "valueType",
                                Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING)),
                                null,
                                null
                        ),
                        new Schema.Field(
                                "nullCount",
                                Schema.create(Schema.Type.LONG),
                                null,
                                null
                        )
                )
        );

        STATS_SCHEMA =
                Schema.createArray(
                        COLUMN_STATS_SCHEMA
                );


        ADD_SCHEMA =
                Schema.createRecord(
                        "CheckpointAdd",
                        null,
                        "com.delta.deltalake.data",
                        false
                );

        Schema nullableStats =
                Schema.createUnion(
                        Schema.create(Schema.Type.NULL),
                        STATS_SCHEMA
                );

        ADD_SCHEMA.setFields(
                List.of(
                        new Schema.Field(
                                "path",
                                Schema.create(Schema.Type.STRING),
                                null,
                                null
                        ),
                        new Schema.Field(
                                "size",
                                Schema.create(Schema.Type.LONG),
                                null,
                                null
                        ),
                        new Schema.Field(
                                "modificationTime",
                                Schema.create(Schema.Type.LONG),
                                null,
                                null
                        ),
                        new Schema.Field(
                                "dataChange",
                                Schema.create(Schema.Type.BOOLEAN),
                                null,
                                null
                        ),
                        new Schema.Field(
                                "numRecords",
                                Schema.create(Schema.Type.LONG),
                                null,
                                null
                        ),
                        new Schema.Field(
                                "stats",
                                nullableStats,
                                null,
                                null
                        )
                )
        );

        REMOVE_SCHEMA =
                Schema.createRecord(
                        "CheckpointRemove",
                        null,
                        "com.delta.deltalake.data",
                        false
                );

        REMOVE_SCHEMA.setFields(
                List.of(
                        new Schema.Field(
                                "path",
                                Schema.create(Schema.Type.STRING),
                                null,
                                null
                        ),
                        new Schema.Field(
                                "deletionTimestamp",
                                Schema.create(Schema.Type.LONG),
                                null,
                                null
                        ),
                        new Schema.Field(
                                "dataChange",
                                Schema.create(Schema.Type.BOOLEAN),
                                null,
                                null
                        )
                )
        );

        METADATA_SCHEMA =
                Schema.createRecord(
                        "CheckpointMetadata",
                        null,
                        "com.delta.deltalake.data",
                        false
                );

        METADATA_SCHEMA.setFields(
                List.of(
                        new Schema.Field(
                                "id",
                                Schema.create(Schema.Type.STRING),
                                null,
                                null
                        ),
                        new Schema.Field(
                                "format",
                                Schema.create(Schema.Type.STRING),
                                null,
                                null
                        ),
                        new Schema.Field(
                                "schemaString",
                                Schema.create(Schema.Type.STRING),
                                null,
                                null
                        ),
                        new Schema.Field(
                                "partitionColumns",
                                Schema.createArray(
                                        Schema.create(Schema.Type.STRING)
                                ),
                                null,
                                null
                        ),
                        new Schema.Field(
                                "configuration",
                                Schema.createMap(
                                        Schema.create(Schema.Type.STRING)
                                ),
                                null,
                                null
                        )
                )
        );

        PROTOCOL_SCHEMA =
                Schema.createRecord(
                        "CheckpointProtocol",
                        null,
                        "com.delta.deltalake.data",
                        false
                );

        PROTOCOL_SCHEMA.setFields(
                List.of(
                        new Schema.Field(
                                "minReaderVersion",
                                Schema.create(Schema.Type.INT),
                                null,
                                null
                        ),
                        new Schema.Field(
                                "minWriterVersion",
                                Schema.create(Schema.Type.INT),
                                null,
                                null
                        )
                )
        );

        TXN_SCHEMA =
                Schema.createRecord(
                        "CheckpointTxn",
                        null,
                        "com.delta.deltalake.data",
                        false
                );

        TXN_SCHEMA.setFields(
                List.of(
                        new Schema.Field(
                                "appId",
                                Schema.create(Schema.Type.STRING),
                                null,
                                null
                        ),
                        new Schema.Field(
                                "version",
                                Schema.create(Schema.Type.LONG),
                                null,
                                null
                        ),
                        new Schema.Field(
                                "lastUpdated",
                                Schema.create(Schema.Type.LONG),
                                null,
                                null
                        )
                )
        );

        CHECKPOINT_SCHEMA =
                Schema.createRecord(
                        "CheckpointRow",
                        null,
                        "com.delta.deltalake.data",
                        false
                );

        CHECKPOINT_SCHEMA.setFields(
                List.of(
                        nullableField("add", ADD_SCHEMA),
                        nullableField("remove", REMOVE_SCHEMA),
                        nullableField("metadata", METADATA_SCHEMA),
                        nullableField("protocol", PROTOCOL_SCHEMA),
                        nullableField("txn", TXN_SCHEMA)
                )
        );
    }

    private CheckpointSchema() {}

    public static Schema schema() {
        return CHECKPOINT_SCHEMA;
    }

    static Schema addSchema() {
        return ADD_SCHEMA;
    }

    static Schema removeSchema() {
        return REMOVE_SCHEMA;
    }

    static Schema metadataSchema() {
        return METADATA_SCHEMA;
    }

    static Schema protocolSchema() {
        return PROTOCOL_SCHEMA;
    }

    static Schema txnSchema() {
        return TXN_SCHEMA;
    }

    static Schema columnStatsSchema() {
        return COLUMN_STATS_SCHEMA;
    }

    static Schema statsSchema() {
        return STATS_SCHEMA;
    }

    private static Schema.Field nullableField(String name, Schema valueSchema) {
        return new Schema.Field(name, Schema.createUnion(Schema.create(Schema.Type.NULL), valueSchema), null, null);
    }
}