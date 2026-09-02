package com.delta.deltalake.data;

import org.apache.avro.Schema;

import java.util.Objects;

public final class TableSchema {
    private final Schema avroSchema;

    public TableSchema(Schema avroSchema) {
        this.avroSchema = Objects.requireNonNull(avroSchema);
    }

    public Schema avroSchema() {
        return avroSchema;
    }

    public String json() {
        return avroSchema.toString();
    }

    public static TableSchema fromJson(String schemaJson) {
        Objects.requireNonNull(schemaJson);
        return new TableSchema(new Schema.Parser().parse(schemaJson));
    }
}