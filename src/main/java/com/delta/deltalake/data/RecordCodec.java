package com.delta.deltalake.data;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

public final class RecordCodec {

    private RecordCodec() {}

    public static GenericRecord encode(Record record) {
        GenericRecord value = new GenericData.Record(RecordSchema.schema());
        value.put("id", record.id());
        value.put("name", record.name());
        value.put("age", record.age());
        return value;
    }

    public static Record decode(GenericRecord value) {
        return new Record((Long) value.get("id"), value.get("name").toString(), (Integer) value.get("age"));
    }
}