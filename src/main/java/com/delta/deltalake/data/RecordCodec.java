package com.delta.deltalake.data;

import org.apache.avro.generic.GenericRecord;
import java.util.LinkedHashMap;

public final class RecordCodec {
    private RecordCodec() {}

    public static GenericRecord encode(Record record) {
        return RowCodec.encode(toRow(record), new TableSchema(RecordSchema.schema()));
    }

    public static Record decode(GenericRecord value) {
        return new Record((Long) value.get("id"), value.get("name").toString(), ((Number) value.get("age")).intValue());
    }

    public static Row toRow(Record record) {
        LinkedHashMap<String, Object> values = new LinkedHashMap<>();
        values.put("id", record.id());
        values.put("name", record.name());
        values.put("age", record.age());
        return Row.of(new TableSchema(RecordSchema.schema()), values);
    }
}
